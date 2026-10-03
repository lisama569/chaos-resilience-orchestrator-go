package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
)

// 多计划并行运行的哨兵错误。
var (
	// ErrPlanAlreadyRunning 表示同一计划已有运行中的实例，被拒绝重复发起。
	// 规则：同一计划 ID 任意时刻至多一个活跃运行；已结束的运行可以再次发起，
	// 此时按引擎既有的续跑规则在持久化状态上继续，绝不出现两份运行同时写同一份状态。
	ErrPlanAlreadyRunning = errors.New("plan is already running")
	// ErrPlanNotStarted 表示查询的计划从未通过该 Runner 发起。
	ErrPlanNotStarted = errors.New("plan has not been started")
)

// planRun 跟踪一个计划的一次（或最近一次）运行。
type planRun struct {
	engine *Engine
	done   chan struct{} // 关闭即代表运行结束，此后 report/err 可读
	report *Report
	err    error
}

// Runner 在同一进程内并行运行多个计划，计划之间完全隔离：
//   - 每个计划有独立的 Engine 与 Store（各自的状态文件/内存实例），
//     状态、事件与结论互不污染；
//   - 每个计划的并发上限只约束自己的目标，不占用其他计划的额度；
//   - 同一计划 ID 重复发起：运行中明确拒绝（ErrPlanAlreadyRunning），
//     已结束则按既有续跑规则继续。
type Runner struct {
	exec     Executor
	newStore func(planID string) Store
	engOpts  []Option

	mu   sync.Mutex
	runs map[string]*planRun
}

// RunnerOption 配置 Runner。
type RunnerOption func(*Runner)

// WithStoreFactory 指定每个计划的状态存储构造方式（按计划 ID 区分）。
func WithStoreFactory(f func(planID string) Store) RunnerOption {
	return func(r *Runner) {
		if f != nil {
			r.newStore = f
		}
	}
}

// WithStateDir 让每个计划使用 dir 下独立的 JSON 状态文件（真实落盘）。
// 文件名由计划 ID 经文件系统安全化得到。
func WithStateDir(dir string) RunnerOption {
	return WithStoreFactory(func(planID string) Store {
		return NewFileStore(filepath.Join(dir, safeFileName(planID)+".state.json"))
	})
}

// WithEngineOptions 为每个计划的引擎追加配置（时钟、日志等）。
func WithEngineOptions(opts ...Option) RunnerOption {
	return func(r *Runner) { r.engOpts = append(r.engOpts, opts...) }
}

// NewRunner 创建多计划运行器。exec 在所有计划间共享，
// 执行器实现必须是并发安全的（SimExecutor 只读脚本，天然安全）。
// 默认每个计划使用独立的内存存储；需要落盘请用 WithStateDir/WithStoreFactory。
func NewRunner(exec Executor, opts ...RunnerOption) *Runner {
	r := &Runner{
		exec: exec,
		newStore: func(string) Store {
			return NewMemoryStore()
		},
		runs: map[string]*planRun{},
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Start 异步发起一个计划的运行。
//
// 重复发起规则（显式且确定）：
//   - 该计划当前有活跃运行：返回 ErrPlanAlreadyRunning，明确拒绝；
//   - 该计划此前的运行已结束：允许再次发起，新引擎在同一存储上加载持久化
//     状态，按引擎续跑规则继续（终态目标不重跑，中断目标只清理）。
func (r *Runner) Start(ctx context.Context, plan *Plan) error {
	if err := ValidatePlan(plan); err != nil {
		return fmt.Errorf("%w: %v", ErrPlanInvalid, err)
	}

	r.mu.Lock()
	if pr, ok := r.runs[plan.ID]; ok {
		select {
		case <-pr.done: // 上一次运行已结束：允许按续跑规则再次发起
		default:
			r.mu.Unlock()
			return fmt.Errorf("%w: %s", ErrPlanAlreadyRunning, plan.ID)
		}
	}
	eng := NewEngine(r.exec, r.newStore(plan.ID), r.engOpts...)
	pr := &planRun{engine: eng, done: make(chan struct{})}
	r.runs[plan.ID] = pr
	r.mu.Unlock()

	go func() {
		defer close(pr.done) // 关闭先于任何 Wait 读，保证 report/err 可见
		pr.report, pr.err = eng.Run(ctx, plan)
	}()
	return nil
}

// Wait 阻塞等待指定计划的运行结束并返回报告。
// 计划从未发起时返回 ErrPlanNotStarted。
func (r *Runner) Wait(planID string) (*Report, error) {
	pr, err := r.handle(planID)
	if err != nil {
		return nil, err
	}
	<-pr.done
	return pr.report, pr.err
}

// Progress 查询指定计划的运行期进度，返回某一时刻的一致视图。
// 查询只读，不影响正在执行的动作与最终结果。
func (r *Runner) Progress(planID string) (PlanProgress, error) {
	pr, err := r.handle(planID)
	if err != nil {
		return PlanProgress{}, err
	}
	p := pr.engine.Snapshot()
	// 以 done 通道为准修正 Running，消除引擎启动/收尾的短暂窗口。
	select {
	case <-pr.done:
		p.Running = false
	default:
		p.Running = true
	}
	return p, nil
}

// Run 同步运行一个计划（Start + Wait 的便捷组合）。
func (r *Runner) Run(ctx context.Context, plan *Plan) (*Report, error) {
	if err := r.Start(ctx, plan); err != nil {
		return nil, err
	}
	return r.Wait(plan.ID)
}

// handle 取计划的运行句柄。
func (r *Runner) handle(planID string) (*planRun, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	pr, ok := r.runs[planID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrPlanNotStarted, planID)
	}
	return pr, nil
}

// safeFileName 把计划 ID 映射为文件系统安全的文件名，
// 仅保留字母数字与 . _ -，其余字符替换为 _。
func safeFileName(id string) string {
	return strings.Map(func(c rune) rune {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '.', c == '_', c == '-':
			return c
		default:
			return '_'
		}
	}, id)
}
