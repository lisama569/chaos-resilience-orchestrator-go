package orchestrator

import (
	"context"
	"errors"
	"fmt"
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

// StoreFactory 为指定计划构造独立的状态存储。
//
// 返回的 location 是该存储的规范落点标识（例如状态文件的绝对路径），
// Runner 用它保证：只要两个计划 ID 不完全相同，就绝不能共用同一落点。
// 工厂必须保证 location 与存储一一对应；若计划 ID 不适合落盘，
// 应返回包装了 ErrPlanIDUnsafe 的错误，运行在发起阶段即被拒绝。
//
// 同一 planID 的重复调用（续跑场景）必须返回同一个 location。
type StoreFactory func(planID string) (store Store, location string, err error)

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
//     已结束则按既有续跑规则继续；
//   - 不同计划 ID 的状态落点在发起时做归属校验，任何共用落点的尝试
//     都以 ErrPlanLocationConflict 明确拒绝，而不是悄悄共享状态。
type Runner struct {
	exec     Executor
	newStore StoreFactory
	engOpts  []Option

	mu        sync.Mutex
	runs      map[string]*planRun // planID -> 最近一次运行
	locations map[string]string   // 落点标识 -> 归属 planID
}

// RunnerOption 配置 Runner。
type RunnerOption func(*Runner)

// WithStoreFactory 指定每个计划的状态存储构造方式（按计划 ID 区分）。
// 工厂必须返回该存储的规范落点标识；两个不同计划 ID 不得返回相同落点，
// 否则发起时返回 ErrPlanLocationConflict。
func WithStoreFactory(f StoreFactory) RunnerOption {
	return func(r *Runner) {
		if f != nil {
			r.newStore = f
		}
	}
}

// WithStateDir 让每个计划使用 dir 下独立的 JSON 状态文件（真实落盘）。
//
// 文件名由计划 ID 经可逆、无碰撞的百分号编码得到：字母数字与 ._- 原样保留，
// 其余字节（含路径分隔符 / \、空格、中文等）一律转义为 %XX。因此：
//   - 两个不同的计划 ID 绝不可能落到同一个文件；
//   - 任何 ID 都不可能编码出路径分隔符，状态文件绝不可能逃逸 dir；
//   - 计划 ID 含 NUL 或编码后超出文件名长度上限时，Start 直接返回
//     ErrPlanIDUnsafe，运行不会被发起。
func WithStateDir(dir string) RunnerOption {
	return WithStoreFactory(func(planID string) (Store, string, error) {
		abs, _, err := planStateLocation(dir, planID)
		if err != nil {
			return nil, "", err
		}
		return NewFileStore(abs), abs, nil
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
		newStore: func(planID string) (Store, string, error) {
			return NewMemoryStore(), "memory:" + planID, nil
		},
		runs:      map[string]*planRun{},
		locations: map[string]string{},
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// isActive 报告该计划当前是否有运行中的实例。
// 依据 done 通道判断，无需持有 Runner 锁。
func (pr *planRun) isActive() bool {
	select {
	case <-pr.done:
		return false
	default:
		return true
	}
}

// Start 异步发起一个计划的运行。
//
// 发起校验全部是显式且确定的，按顺序执行：
//  1. 计划静态校验失败：ErrPlanInvalid；
//  2. 计划 ID 不适合落盘：ErrPlanIDUnsafe（运行绝不被发起）；
//  3. 该计划当前有活跃运行：ErrPlanAlreadyRunning（同一计划任意时刻至多一份写者）；
//  4. 工厂给出的落点已属于另一个不同的计划 ID：ErrPlanLocationConflict
//     （不同计划绝不共用同一份状态）；
//  5. 该计划此前的运行已结束：允许再次发起，新引擎在同一存储上加载持久化
//     状态，按引擎续跑规则继续（终态目标不重跑，中断目标只清理）。
func (r *Runner) Start(ctx context.Context, plan *Plan) error {
	if err := ValidatePlan(plan); err != nil {
		return fmt.Errorf("%w: %v", ErrPlanInvalid, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if pr, ok := r.runs[plan.ID]; ok && pr.isActive() {
		return fmt.Errorf("%w: %s", ErrPlanAlreadyRunning, plan.ID)
	}

	// 工厂在锁内调用：落点归属登记必须与“取出运行槽位”原子完成，
	// 否则两个并发 Start 可能同时通过冲突检查后共用同一落点。
	store, location, err := r.newStore(plan.ID)
	if err != nil {
		return err
	}
	if owner, ok := r.locations[location]; ok && owner != plan.ID {
		return fmt.Errorf("%w: location %s already owned by plan %q (requested by %q)",
			ErrPlanLocationConflict, location, owner, plan.ID)
	}

	eng := NewEngine(r.exec, store, r.engOpts...)
	pr := &planRun{engine: eng, done: make(chan struct{})}
	r.runs[plan.ID] = pr
	r.locations[location] = plan.ID

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
//
// Runner 自身的互斥锁只在取出运行句柄时短暂持有，快照在锁外完成，
// 因此多个计划的高频查询彼此不排队，也不阻塞 Start/Wait。
func (r *Runner) Progress(planID string) (PlanProgress, error) {
	r.mu.Lock()
	pr, ok := r.runs[planID]
	r.mu.Unlock()
	if !ok {
		return PlanProgress{}, fmt.Errorf("%w: %s", ErrPlanNotStarted, planID)
	}
	p := pr.engine.Snapshot()
	// 以 done 通道为准修正 Running，消除引擎启动/收尾的短暂窗口。
	p.Running = pr.isActive()
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
