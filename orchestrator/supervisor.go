package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// 多计划并行运行的哨兵错误：调用方必须显式处理。
var (
	// ErrPlanAlreadyRunning 表示同一计划已有进行中的运行。
	// 绝不允许多份运行同时写同一份状态文件。
	ErrPlanAlreadyRunning = errors.New("plan run already in flight")
	// ErrUnknownPlan 表示查询/等待了一个从未启动过的计划。
	ErrUnknownPlan = errors.New("unknown plan run")
	// errUnsafePlanID 表示计划 ID 不能安全地映射为落盘路径。
	errUnsafePlanID = errors.New("plan id is not filesystem-safe")
)

// ExecutorFactory 为一次计划运行构造执行器。
// 不同计划的运行应使用相互独立的执行器实例，避免执行层串扰。
type ExecutorFactory func(plan *Plan) Executor

// Supervisor 在同一进程内并行运行多个计划，并保证它们互不串扰：
//   - 每个计划有独立的 Engine、状态与落盘位置（dir/<plan-id>/state.json）；
//   - 每个计划的并发上限只约束自己的目标，不影响其他计划；
//   - 同一计划重复 Start：进行中 -> 明确拒绝（ErrPlanAlreadyRunning）；
//     已结束 -> 按既有续跑规则从持久化状态继续（见 Engine.Run）。
type Supervisor struct {
	dir     string
	factory ExecutorFactory
	opts    []Option

	mu   sync.Mutex
	runs map[string]*planRun
}

// planRun 跟踪一次计划运行的句柄。
type planRun struct {
	engine *Engine
	done   chan struct{}
	report *Report
	err    error
}

// NewSupervisor 创建多计划运行管理器。dir 为各计划状态文件的根目录。
func NewSupervisor(dir string, factory ExecutorFactory, opts ...Option) *Supervisor {
	return &Supervisor{dir: dir, factory: factory, opts: opts, runs: map[string]*planRun{}}
}

// statePath 返回一个计划的专属状态文件位置。
func (s *Supervisor) statePath(planID string) string {
	return filepath.Join(s.dir, planID, "state.json")
}

// checkPlanID 拒绝无法安全映射为独立落盘位置的计划 ID，
// 防止两个计划因路径解析而共享同一份状态文件。
func checkPlanID(id string) error {
	if id == "" || id == "." || id == ".." ||
		strings.ContainsRune(id, '/') || strings.ContainsRune(id, '\\') ||
		strings.ContainsRune(id, 0) {
		return fmt.Errorf("%w: %q", errUnsafePlanID, id)
	}
	return nil
}

// Start 异步启动一个计划的运行。
//
// 重复发起规则（显式）：
//   - 同一计划已有进行中的运行：返回 ErrPlanAlreadyRunning，绝不并发双写；
//   - 同一计划上一轮已结束：允许再次 Start，按 Engine 的续跑规则
//     从上次持久化状态继续（终态目标不重放，中断目标只清理）。
func (s *Supervisor) Start(ctx context.Context, plan *Plan) error {
	if err := ValidatePlan(plan); err != nil {
		return fmt.Errorf("%w: %v", ErrPlanInvalid, err)
	}
	if err := checkPlanID(plan.ID); err != nil {
		return fmt.Errorf("%w: %v", ErrPlanInvalid, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.runs[plan.ID]; ok {
		select {
		case <-r.done: // 上一轮已结束：按续跑规则开新一轮
		default:
			return fmt.Errorf("%w: plan=%q", ErrPlanAlreadyRunning, plan.ID)
		}
	}
	eng := NewEngine(s.factory(plan), NewFileStore(s.statePath(plan.ID)), s.opts...)
	r := &planRun{engine: eng, done: make(chan struct{})}
	s.runs[plan.ID] = r
	go func() {
		r.report, r.err = eng.Run(ctx, plan)
		close(r.done)
	}()
	return nil
}

// run 返回指定计划的运行句柄。
func (s *Supervisor) run(planID string) (*planRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[planID]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownPlan, planID)
	}
	return r, nil
}

// Progress 查询指定计划当前进度的一致快照；计划从未启动时返回 ErrUnknownPlan。
// 查询不阻塞任何正在执行的计划。
func (s *Supervisor) Progress(planID string) (PlanProgress, error) {
	r, err := s.run(planID)
	if err != nil {
		return PlanProgress{}, err
	}
	return r.engine.Progress(), nil
}

// Wait 阻塞等待指定计划本轮运行结束并返回报告；计划从未启动时返回 ErrUnknownPlan。
func (s *Supervisor) Wait(planID string) (*Report, error) {
	r, err := s.run(planID)
	if err != nil {
		return nil, err
	}
	<-r.done
	return r.report, r.err
}

// Running 报告指定计划当前是否有进行中的运行。
func (s *Supervisor) Running(planID string) bool {
	r, err := s.run(planID)
	if err != nil {
		return false
	}
	select {
	case <-r.done:
		return false
	default:
		return true
	}
}

// Plans 返回已知（启动过）的计划 ID 列表，按字典序排列，输出稳定。
func (s *Supervisor) Plans() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.runs))
	for id := range s.runs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
