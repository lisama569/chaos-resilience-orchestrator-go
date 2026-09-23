package engine

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/action"
	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/recovery"
	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/store"
)

// Engine 编排实验：并发上限、超时、取消、清理、稳态判定与自愈。
type Engine struct {
	store store.Store
	rt    action.Runtime
	log   Logger

	experimentID model.ExperimentID

	defaultCleanupTimeout time.Duration

	mu      sync.Mutex
	running map[model.ExperimentID]context.CancelFunc
}

// New 创建编排引擎。
func New(st store.Store, rt action.Runtime, log Logger) *Engine {
	if log == nil {
		log = NopLogger()
	}
	return &Engine{
		store:                 st,
		rt:                    rt,
		log:                   log,
		running:               map[model.ExperimentID]context.CancelFunc{},
		defaultCleanupTimeout: 5 * time.Second,
	}
}

// Run 从头运行一个实验（阻塞至终态）。
func (e *Engine) Run(ctx context.Context, spec model.ExperimentSpec) (*model.ExperimentState, error) {
	now := e.rt.Now()
	st := &model.ExperimentState{
		ID:        spec.ID,
		Spec:      spec,
		Status:    model.StatusPending,
		Steps:     initSteps(spec),
		Results:   map[model.TargetID]*model.TargetResult{},
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := e.store.Create(ctx, st); err != nil {
		return nil, err
	}
	e.log.Printf("experiment created: id=%s targets=%d concurrency=%d observe_rounds=%d resume=%s",
		spec.ID, len(spec.Targets), spec.Concurrency, spec.ObserveRounds, spec.ResumePolicy)
	return e.execute(ctx, st)
}

// Resume 重启后续跑或终止一个未完成实验。
func (e *Engine) Resume(ctx context.Context, id model.ExperimentID) (*model.ExperimentState, error) {
	st, err := e.store.Load(ctx, id)
	if err != nil {
		return nil, err
	}
	plan := recovery.Decide(st)
	e.log.Printf("recovery decision: id=%s decision=%s reason=%q reset_steps=%v",
		id, plan.Decision, plan.Reason, plan.ResetSteps)

	switch plan.Decision {
	case recovery.DecisionNone:
		return st, nil
	case recovery.DecisionAbort:
		return e.abortAndCleanup(ctx, st)
	case recovery.DecisionContinue:
		return e.continueRun(ctx, st)
	default:
		return nil, fmt.Errorf("unknown recovery decision %q", plan.Decision)
	}
}

// Interrupt 主动中断正在运行的实验：取消执行上下文，运行协程将完成清理并落盘 paused。
func (e *Engine) Interrupt(id model.ExperimentID) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	cancel, ok := e.running[id]
	if !ok {
		return fmt.Errorf("experiment %s is not running in this process", id)
	}
	cancel()
	return nil
}

// execute 是运行/续跑共享的执行入口。
func (e *Engine) execute(ctx context.Context, st *model.ExperimentState) (*model.ExperimentState, error) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	e.experimentID = st.ID
	e.mu.Lock()
	e.running[st.ID] = cancel
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.running, st.ID)
		e.mu.Unlock()
	}()

	// pending / paused -> running
	if st.Status != model.StatusRunning {
		var err error
		st, err = e.setStatus(runCtx, model.StatusRunning)
		if err != nil {
			return nil, err
		}
	}

	results := e.scheduleTargets(runCtx, st)

	// 回读最新状态（恢复路径会重置 running 步骤）。取消后用独立上下文读盘/落盘。
	dbCtx := runCtx
	if runCtx.Err() != nil {
		dbCtx = context.Background()
	}
	latest, err := e.store.Load(dbCtx, st.ID)
	if err != nil {
		return nil, err
	}
	final := e.finalize(latest, results, runCtx.Err())
	return e.commitFinal(dbCtx, final)
}

func (e *Engine) continueRun(ctx context.Context, st *model.ExperimentState) (*model.ExperimentState, error) {
	e.experimentID = st.ID
	// 崩溃时不可能确认完成的 running 步骤重置为 pending，其余终态步骤原样保留。
	if _, err := e.resetRunningSteps(ctx); err != nil {
		return nil, err
	}
	return e.execute(ctx, st)
}

// setStatus 迁移实验级状态，状态回退/越级由 model 状态机拒绝。
func (e *Engine) setStatus(ctx context.Context, to model.ExperimentStatus) (*model.ExperimentState, error) {
	var out *model.ExperimentState
	err := func() error {
		stepCommitMu.Lock()
		defer stepCommitMu.Unlock()
		cur, err := e.store.Load(ctx, e.experimentID)
		if err != nil {
			return err
		}
		if err := model.CheckExperimentTransition(cur.Status, to); err != nil {
			return err
		}
		cur.Status = to
		cur.Version++
		cur.UpdatedAt = e.rt.Now()
		out, err = e.store.Commit(ctx, cur, cur.Version-1)
		return err
	}()
	return out, err
}

// finalize 汇总各目标结果，得出可区分的实验结论与失败原因。
func (e *Engine) finalize(st *model.ExperimentState, results []*model.TargetResult, runErr error) *model.ExperimentState {
	order := make([]model.TargetID, 0, len(st.Spec.Targets))
	for _, t := range st.Spec.Targets {
		order = append(order, t.ID)
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })

	st.Results = map[model.TargetID]*model.TargetResult{}
	st.Conclusion = ""
	st.Reason = model.ReasonNone // 清除上次运行遗留的陈旧原因，按本次结果重新计算
	allCleaned := true
	canceled := runErr == context.Canceled

	// 失败原因优先级：取消 > 清理失败 > 注入失败 > 判定异常 > 自愈中止。
	var primary model.FailureReason
	priority := func(r model.FailureReason) {
		if reasonRank(r) > reasonRank(primary) {
			primary = r
		}
	}

	for _, id := range order {
		r := findResult(results, id)
		if r == nil {
			r = &model.TargetResult{ID: id, Verdict: model.ConclusionInconclusive,
				Reason: model.ReasonCanceled}
		}
		st.Results[id] = r
		if !r.CleanedUp {
			allCleaned = false
		}
		if r.Reason != model.ReasonNone {
			priority(r.Reason)
		}
	}

	if canceled {
		primary = model.ReasonCanceled
	}

	allSteady := true
	for _, r := range st.Results {
		if r.Verdict != model.ConclusionSteady {
			allSteady = false
		}
	}

	switch {
	case canceled:
		st.Status = model.StatusPaused
		st.Conclusion = model.ConclusionInconclusive
		st.Reason = model.ReasonCanceled
	case primary == model.ReasonCleanupFailed || primary == model.ReasonCleanupTimeout:
		// 清理未完成 = 可能残留半生效故障，实验失败且结论不可信。
		st.Status = model.StatusFailed
		st.Conclusion = model.ConclusionInconclusive
		st.Reason = primary
	case !allCleaned:
		st.Status = model.StatusFailed
		st.Conclusion = model.ConclusionInconclusive
		st.Reason = model.ReasonCleanupFailed
	case primary != model.ReasonNone:
		st.Status = model.StatusFailed
		st.Conclusion = model.ConclusionInconclusive
		st.Reason = primary
	case allSteady:
		st.Status = model.StatusSucceeded
		st.Conclusion = model.ConclusionSteady
	default:
		// 全部清理成功、没有执行异常，但存在 not_steady：实验本身成功，结论为未达稳态。
		st.Status = model.StatusSucceeded
		st.Conclusion = model.ConclusionNotSteady
	}
	return st
}

func (e *Engine) commitFinal(ctx context.Context, st *model.ExperimentState) (*model.ExperimentState, error) {
	stepCommitMu.Lock()
	defer stepCommitMu.Unlock()
	cur, err := e.store.Load(ctx, st.ID)
	if err != nil {
		return nil, err
	}
	if cur.Status.IsTerminal() {
		return nil, fmt.Errorf("%w: experiment %s already terminal", model.ErrStateRollback, st.ID)
	}
	st.Version = cur.Version + 1
	st.UpdatedAt = e.rt.Now()
	out, err := e.store.Commit(ctx, st, cur.Version)
	if err != nil {
		return nil, err
	}
	e.log.Printf("experiment finalized: id=%s status=%s conclusion=%s reason=%s",
		st.ID, st.Status, st.Conclusion, st.Reason)
	return out, nil
}

func findResult(rs []*model.TargetResult, id model.TargetID) *model.TargetResult {
	for _, r := range rs {
		if r.ID == id {
			return r
		}
	}
	return nil
}

// reasonRank 定义失败原因在聚合时的优先级（越大越优先）。
func reasonRank(r model.FailureReason) int {
	switch r {
	case model.ReasonCleanupFailed, model.ReasonCleanupTimeout:
		return 50
	case model.ReasonTargetUnreachable:
		return 40
	case model.ReasonInjectTimeout, model.ReasonInjectFailed:
		return 30
	case model.ReasonProbeError, model.ReasonProbeTimeout,
		model.ReasonProbeInsufficient, model.ReasonProbeContradictory:
		return 20
	case model.ReasonHealTimeout, model.ReasonHealFailed, model.ReasonHealExhausted:
		return 10
	default:
		return 0
	}
}
