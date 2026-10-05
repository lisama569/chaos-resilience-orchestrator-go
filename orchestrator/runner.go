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
	// ErrPlanIDUnsafe 表示计划 ID 无法安全落盘（编码后超出文件名长度上限），
	// 在发起时明确拒绝，绝不在背后改写 ID 或让它与别的计划共用落点。
	ErrPlanIDUnsafe = errors.New("plan id is not safe for state persistence")
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
	stateDir string // 非空时启用落盘，Start 会先校验计划 ID 的落点安全性

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
// 文件名由计划 ID 经单射编码得到（见 stateFileName）：任意两个不同的
// 计划 ID 一定落到不同文件，且任何 ID 都不可能逃出 dir。
// 编码后仍无法安全落盘的 ID（超长）会在 Start 时被明确拒绝（ErrPlanIDUnsafe）。
func WithStateDir(dir string) RunnerOption {
	return func(r *Runner) {
		r.stateDir = dir
		r.newStore = func(planID string) Store {
			name, err := stateFileName(planID)
			if err != nil {
				// Start 已先行校验，此处不可达；兜底返回错误路径让 Save 显式失败。
				name = "__invalid_plan_id__"
			}
			return NewFileStore(filepath.Join(dir, name))
		}
	}
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
//
// 落盘安全规则（仅 WithStateDir 模式，发起时先行校验）：
//   - 计划 ID 经单射编码后落在 dir 内、且不与任何其他 ID 撞名，才允许发起；
//   - 无法安全落盘的 ID（如编码后超出文件名长度上限）返回 ErrPlanIDUnsafe，
//     明确拒绝并说明原因，绝不静默改写或与其他计划共用状态文件。
func (r *Runner) Start(ctx context.Context, plan *Plan) error {
	if err := ValidatePlan(plan); err != nil {
		return fmt.Errorf("%w: %v", ErrPlanInvalid, err)
	}
	if r.stateDir != "" {
		if _, err := stateFileName(plan.ID); err != nil {
			return fmt.Errorf("%w: %v", ErrPlanIDUnsafe, err)
		}
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

// stateFileSuffix 是计划状态文件的后缀。
const stateFileSuffix = ".state.json"

// maxStateFileName 是常见文件系统对单个文件名的长度上限（字节）。
const maxStateFileName = 255

// stateFileName 把计划 ID 单射编码为 dir 内的状态文件名。
//
// 编码规则：仅 [A-Za-z0-9._-] 原样保留，其余每个字节编码为 %XX
// （% 自身编码为 %25）。该编码是单射：不同的计划 ID 必得不同文件名，
// 因此任何两个计划都不可能共用同一份状态文件；编码不含路径分隔符，
// 任何 ID 都不可能逃出状态目录。
//
// 唯一无法安全落盘的情形是编码后文件名超出文件系统长度上限，
// 此时返回错误，由调用方在发起时明确拒绝。
func stateFileName(planID string) (string, error) {
	var b strings.Builder
	b.Grow(len(planID) + len(stateFileSuffix))
	for i := 0; i < len(planID); i++ {
		c := planID[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '.', c == '_', c == '-':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	b.WriteString(stateFileSuffix)
	name := b.String()
	if len(name) > maxStateFileName {
		return "", fmt.Errorf("plan id %q encodes to a %d-byte file name, limit is %d",
			planID, len(name), maxStateFileName)
	}
	return name, nil
}
