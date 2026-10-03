package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultActionTimeout 是动作未单独配置超时时使用的默认值。
const DefaultActionTimeout = 5 * time.Second

// Logger 是引擎的结构化日志回调；单测可注入以捕获关键输入与判定依据。
type Logger func(format string, args ...any)

// discardLogger 是默认日志器。
func discardLogger(string, ...any) {}

// Engine 按计划编排多个目标的故障注入、稳态判定、自愈与清理。
type Engine struct {
	exec      Executor
	evaluator *Evaluator
	store     Store
	now       func() time.Time
	logf      Logger

	stateMu sync.Mutex
	state   *PlanState

	// snapshot 是最近一次成功提交后发布的不可变状态深拷贝，
	// 供运行期进度查询原子读取，查询路径不加锁、不阻塞执行。
	snapshot  atomic.Pointer[PlanState]
	runStatus atomic.Value // RunStatus
}

// Option 配置引擎。
type Option func(*Engine)

// WithClock 注入固定时钟（测试确定性）。
func WithClock(clock func() time.Time) Option {
	return func(e *Engine) { e.now = clock }
}

// WithLogger 注入日志回调。
func WithLogger(logf Logger) Option {
	return func(e *Engine) {
		if logf != nil {
			e.logf = logf
		}
	}
}

// NewEngine 创建编排引擎。
func NewEngine(exec Executor, store Store, opts ...Option) *Engine {
	e := &Engine{
		exec:  exec,
		store: store,
		now:   time.Now,
		logf:  discardLogger,
	}
	for _, o := range opts {
		o(e)
	}
	e.evaluator = NewEvaluator(e.now)
	e.runStatus.Store(RunNotStarted)
	return e
}

// newAttemptID 生成确定性的运行尝试标识。
func (e *Engine) newAttemptID(planID string) string {
	return fmt.Sprintf("%s#%d", planID, e.now().UnixNano())
}

// mutate 在持锁状态下修改状态并立即持久化，保证崩溃恢复边界清晰。
// 持久化成功后发布新的不可变进度快照，供运行期查询一致地读取。
func (e *Engine) mutate(fn func()) error {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	fn()
	if err := e.store.Save(e.state); err != nil {
		return fmt.Errorf("%w: %v", ErrPersistFailed, err)
	}
	e.publishSnapshot()
	return nil
}

// event 追加审计事件并持久化。
func (e *Engine) event(targetID, kind string, detail map[string]any) {
	_ = e.mutate(func() {
		e.state.AppendEvent(e.now(), targetID, kind, detail)
	})
}

// Run 执行（或中断后续跑）一个计划，遵守并发上限、整体超时与取消信号。
//
// 续跑规则（显式且确定）：
//  1. 已处于终态（completed、aborted、cleanup_failed 三者之一）的目标：
//     已确认完成的步骤绝不重复执行，直接以持久化结果产出报告；
//  2. 非终态且故障已确认注入（注入后、探测中、自愈中、清理中崩溃）：
//     崩溃点无法安全复现，不重放实验，只执行有界幂等清理，
//     目标结论为 aborted 且失败原因为 interrupted_by_restart，清理结果单独给出；
//  3. 非终态且故障未确认注入：实验从头执行，已 MarkDone 的步骤不重跑；
//  4. 状态回退或重复完成上报由状态守卫明确拒绝
//     （ErrPhaseRollback、ErrStaleReport、ErrAlreadyTerminal）。
func (e *Engine) Run(ctx context.Context, plan *Plan) (*Report, error) {
	if err := ValidatePlan(plan); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPlanInvalid, err)
	}

	loaded, err := e.store.Load()
	switch {
	case errors.Is(err, ErrNotExist):
		loaded = &PlanState{
			PlanID:    plan.ID,
			AttemptID: e.newAttemptID(plan.ID),
			Targets:   map[string]*TargetState{},
		}
		for _, t := range plan.Targets {
			loaded.Targets[t.ID] = &TargetState{ID: t.ID, AttemptID: loaded.AttemptID, Cleanup: CleanupNotNeeded}
		}
	case err != nil:
		return nil, err
	default:
		if loaded.PlanID != plan.ID {
			return nil, fmt.Errorf("state belongs to plan %q, refusing to run plan %q", loaded.PlanID, plan.ID)
		}
	}
	if loaded.Targets == nil {
		loaded.Targets = map[string]*TargetState{}
	}
	for _, t := range plan.Targets {
		if _, ok := loaded.Targets[t.ID]; !ok {
			loaded.Targets[t.ID] = &TargetState{ID: t.ID, AttemptID: loaded.AttemptID, Cleanup: CleanupNotNeeded}
		}
	}
	e.state = loaded
	e.runStatus.Store(RunRunning)
	defer e.runStatus.Store(RunFinished)
	e.event("", "run_started", map[string]any{"plan_id": plan.ID, "attempt_id": loaded.AttemptID, "targets": len(plan.Targets)})

	runCtx := ctx
	if plan.RunTimeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, plan.RunTimeout)
		defer cancel()
	}

	sem := make(chan struct{}, plan.Concurrency)
	results := make([]TargetResult, len(plan.Targets))
	var wg sync.WaitGroup
	for i := range plan.Targets {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i] = e.runTarget(runCtx, plan.Targets[i])
		}(i)
	}
	wg.Wait()
	e.event("", "run_finished", nil)

	report := &Report{PlanID: plan.ID, At: e.now(), Results: results}
	if runCtx.Err() != nil {
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			report.TimedOut = true
		} else {
			report.Canceled = true
		}
	}
	return report, nil
}

// runTarget 按续跑规则处理单个目标。
func (e *Engine) runTarget(ctx context.Context, spec TargetSpec) TargetResult {
	st := e.state.Targets[spec.ID]

	if st.Phase.IsTerminal() {
		e.logf("[target=%s] already terminal phase=%s on load, skipping (no step is re-executed)", spec.ID, st.Phase)
		e.event(spec.ID, "resume_skip_terminal", map[string]any{"phase": string(st.Phase)})
		return e.resultOf(st)
	}
	if st.FaultInjected {
		// 规则 2：故障已注入但进程中断过。崩溃点不可安全复现，只清理并中止。
		e.logf("[target=%s] resume detected: fault was injected before interruption; cleanup-only then abort", spec.ID)
		e.event(spec.ID, "resume_interrupted", map[string]any{"phase": string(st.Phase)})
		cleanup := e.enforceCleanup(ctx, spec, st)
		_ = e.mutate(func() {
			st.Failure = ReasonInterrupted
			st.FailureDetail = "process restarted after fault injection; experiment aborted, fault cleanup enforced"
			st.Cleanup = cleanup
			st.UpdatedAt = e.now()
			if cleanup == CleanupFailed {
				st.Failure = ReasonRecoverFailed
				st.FailureDetail = "interrupted by restart and fault cleanup failed"
				_ = st.Advance(PhaseCleanupFailed, e.now())
			} else {
				_ = st.Advance(PhaseAborted, e.now())
			}
		})
		return e.resultOf(st)
	}

	return e.runFresh(ctx, spec, st)
}

// runFresh 执行一个目标的完整实验流水线。
func (e *Engine) runFresh(ctx context.Context, spec TargetSpec, st *TargetState) TargetResult {
	// 1) 注入前可达性确认
	reachCtx, reachCancel := context.WithTimeout(ctx, DefaultActionTimeout)
	err := e.exec.Reachable(reachCtx, spec.Target, spec.ReachableBy)
	reachCancel()
	e.logf("[target=%s] reachability via %q: err=%v", spec.ID, spec.ReachableBy, err)
	e.event(spec.ID, "reachability_checked", map[string]any{"probe": spec.ReachableBy, "ok": err == nil})
	if err != nil {
		reason := classifyErr(err, ReasonTargetUnreachable)
		e.abortNoFault(st, reason, fmt.Sprintf("before injection: %v", err))
		return e.resultOf(st)
	}

	// 2) 注入故障（已完成则不重放；正常情况下首次运行均未完成）
	if !st.IsStepDone("inject") {
		_ = e.mutate(func() { _ = st.Advance(PhaseInjecting, e.now()) })
		actCtx, cancel := context.WithTimeout(ctx, actionTimeout(spec.Inject.Timeout))
		err = e.exec.Do(actCtx, spec.Inject)
		cancel()
		e.logf("[target=%s] inject action=%s timeout=%s: err=%v", spec.ID, spec.Inject.Name, actionTimeout(spec.Inject.Timeout), err)
		e.event(spec.ID, "inject_attempted", map[string]any{"action": spec.Inject.Name, "ok": err == nil})
		if err != nil {
			reason := classifyErr(err, ReasonInjectFailed)
			// 超时/取消时注入是否生效未知：按“可能已生效”处理，必须执行清理。
			uncertain := reason == ReasonActionTimeout || reason == ReasonExperimentCanceled
			if uncertain {
				_ = e.mutate(func() { st.FaultInjected = true })
				cleanup := e.enforceCleanup(ctx, spec, st)
				e.finishAfterCleanup(st, PhaseAborted, reason,
					fmt.Sprintf("inject did not finish cleanly (%v); fault state unknown, cleanup attempted", err), cleanup)
			} else {
				e.abortNoFault(st, reason, fmt.Sprintf("inject failed: %v", err))
			}
			return e.resultOf(st)
		}
		_ = e.mutate(func() {
			st.FaultInjected = true
			_ = st.MarkDone("inject", st.AttemptID, e.now())
			_ = st.Advance(PhaseInjected, e.now())
		})
	}

	// 3) 稳态判定（结论必须来自确定依据；不足/矛盾不得默认通过）
	_ = e.mutate(func() { _ = st.Advance(PhaseProbing, e.now()) })
	ev, err := e.evaluator.Evaluate(0, spec.Probes, spec.Rules, e.sampleFunc(ctx, spec, spec.Probes))
	if err != nil {
		cleanup := e.enforceCleanup(ctx, spec, st)
		e.finishAfterCleanup(st, PhaseAborted, ReasonPlanInvalid, err.Error(), cleanup)
		return e.resultOf(st)
	}
	e.logf("[target=%s] steady verdict=%s basis=%s", spec.ID, ev.Verdict, ev.BasisSummary)
	e.event(spec.ID, "steady_evaluated", map[string]any{
		"verdict": string(ev.Verdict), "round": ev.Round, "basis": ev.BasisSummary,
	})
	_ = e.mutate(func() { st.Verdict = ev.Verdict })

	switch ev.Verdict {
	case VerdictSteady:
		cleanup := e.enforceCleanup(ctx, spec, st)
		e.finishAfterCleanup(st, PhaseCompleted, ReasonNone, "", cleanup)
		return e.resultOf(st)
	case VerdictViolated:
		if spec.Heal == nil {
			cleanup := e.enforceCleanup(ctx, spec, st)
			e.finishAfterCleanup(st, PhaseAborted, ReasonSteadyViolated,
				"steady state violated by deterministic evidence; no heal policy configured", cleanup)
			return e.resultOf(st)
		}
		// 落入自愈联动
	case VerdictInconclusive:
		cleanup := e.enforceCleanup(ctx, spec, st)
		e.finishAfterCleanup(st, PhaseAborted, ReasonEvidenceInsufficient, ev.BasisSummary, cleanup)
		return e.resultOf(st)
	case VerdictContradictory:
		cleanup := e.enforceCleanup(ctx, spec, st)
		e.finishAfterCleanup(st, PhaseAborted, ReasonEvidenceContradictory, ev.BasisSummary, cleanup)
		return e.resultOf(st)
	}

	// 4) 自愈联动：有限轮次，收敛或明确中止
	_ = e.mutate(func() { _ = st.Advance(PhaseHealing, e.now()) })
	sink := newHealSink(e, ctx, spec, st)
	outcome, rounds, herr := NewHealer().Run(spec.Heal, st.HealRounds, sink)
	_ = e.mutate(func() { st.HealRounds = rounds })
	e.logf("[target=%s] heal outcome=%s rounds=%d err=%v", spec.ID, outcome, rounds, herr)
	e.event(spec.ID, "heal_finished", map[string]any{"outcome": string(outcome), "rounds": rounds})

	switch outcome {
	case HealConverged:
		cleanup := e.enforceCleanup(ctx, spec, st)
		e.finishAfterCleanup(st, PhaseCompleted, ReasonNone, "", cleanup)
	case HealNotConverged:
		cleanup := e.enforceCleanup(ctx, spec, st)
		e.finishAfterCleanup(st, PhaseAborted, ReasonHealNotConverged,
			fmt.Sprintf("heal did not converge within %d rounds", spec.Heal.MaxRounds), cleanup)
	case HealInconclusive:
		reason := ReasonEvidenceInsufficient
		if sink.last != nil && sink.last.Verdict == VerdictContradictory {
			reason = ReasonEvidenceContradictory
		}
		basis := ""
		if sink.last != nil {
			basis = sink.last.BasisSummary
		}
		cleanup := e.enforceCleanup(ctx, spec, st)
		e.finishAfterCleanup(st, PhaseAborted, reason, basis, cleanup)
	case HealActionFailed:
		reason := classifyErr(herr, ReasonHealActionFailed)
		cleanup := e.enforceCleanup(ctx, spec, st)
		e.finishAfterCleanup(st, PhaseAborted, reason, fmt.Sprintf("heal action failed: %v", herr), cleanup)
	case HealCanceled:
		cleanup := e.enforceCleanup(ctx, spec, st)
		e.finishAfterCleanup(st, PhaseAborted, ReasonExperimentCanceled, "healing canceled", cleanup)
	}
	return e.resultOf(st)
}

// abortNoFault 记录一个发生在故障确认注入之前的中止（无需清理）。
func (e *Engine) abortNoFault(st *TargetState, reason FailureReason, detail string) {
	_ = e.mutate(func() {
		st.Failure = reason
		st.FailureDetail = detail
		st.Cleanup = CleanupNotNeeded
		_ = st.Advance(PhaseAborted, e.now())
	})
	e.logf("[target=%s] aborted reason=%s detail=%s", st.ID, reason, detail)
	e.event(st.ID, "target_aborted", map[string]any{"reason": string(reason), "detail": detail})
}

// finishAfterCleanup 根据清理结果落到正常终态或 cleanup_failed 终态。
func (e *Engine) finishAfterCleanup(st *TargetState, onSuccess Phase, reason FailureReason, detail string, cleanup CleanupResult) {
	_ = e.mutate(func() {
		st.Failure = reason
		st.FailureDetail = detail
		st.Cleanup = cleanup
		if cleanup == CleanupFailed {
			_ = st.Advance(PhaseCleanupFailed, e.now())
			st.Failure = ReasonRecoverFailed
			st.FailureDetail = "fault cleanup failed after " + fmt.Sprint(MaxCleanupAttempts) + " attempts"
		} else {
			_ = st.Advance(onSuccess, e.now())
		}
	})
	e.event(st.ID, "target_finished", map[string]any{
		"phase": string(st.Phase), "reason": string(st.Failure), "cleanup": string(cleanup),
	})
	e.logf("[target=%s] finished phase=%s reason=%s cleanup=%s", st.ID, st.Phase, st.Failure, cleanup)
}

// enforceCleanup 以“脱离父级取消、单次有超时、总次数有上限”的方式保证
// 已注入故障最终被清理：即使实验被取消或超时，清理仍会尝试执行。
func (e *Engine) enforceCleanup(parent context.Context, spec TargetSpec, st *TargetState) CleanupResult {
	if !st.FaultInjected {
		return CleanupNotNeeded
	}
	_ = e.mutate(func() { _ = st.Advance(PhaseCleaning, e.now()) })
	act := spec.Recover
	for attempt := 1; attempt <= MaxCleanupAttempts; attempt++ {
		// WithoutCancel：实验取消/超时不影响清理；清理仍受自身超时约束。
		ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), actionTimeout(act.Timeout))
		err := e.exec.Do(ctx, act)
		cancel()
		e.logf("[target=%s] cleanup attempt=%d/%d err=%v", spec.ID, attempt, MaxCleanupAttempts, err)
		e.event(spec.ID, "cleanup_attempted", map[string]any{"attempt": attempt, "ok": err == nil})
		if err == nil {
			_ = e.mutate(func() { st.FaultInjected = false })
			return CleanupSucceeded
		}
	}
	return CleanupFailed
}

// sampleFunc 产出评估器使用的确定性采样回调，并记录关键输入。
func (e *Engine) sampleFunc(ctx context.Context, spec TargetSpec, probes []ProbeSpec) SampleFunc {
	index := map[string]ProbeSpec{}
	for _, p := range probes {
		index[p.Name] = p
	}
	return func(name string) (Sample, error) {
		ps := index[name]
		pctx, cancel := context.WithTimeout(ctx, actionTimeout(ps.Timeout))
		s, err := e.exec.Probe(pctx, spec.Target, ps)
		cancel()
		e.logf("[target=%s] probe=%s sample=%v err=%v", spec.ID, name, s.Values, err)
		if err != nil {
			// 探测超时/取消属于“拿不到证据”，统一归入探测异常 -> 依据不足。
			return Sample{}, &ActionError{Reason: ReasonProbeError, Detail: fmt.Sprintf("probe %s: %v", name, err)}
		}
		e.event(spec.ID, "probe_sample", map[string]any{"probe": name, "values": s.Values})
		return s, nil
	}
}

func (e *Engine) resultOf(st *TargetState) TargetResult {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	evs := make([]Event, len(st.Events))
	copy(evs, st.Events)
	return TargetResult{
		TargetID:      st.ID,
		Phase:         st.Phase,
		Verdict:       st.Verdict,
		Failure:       st.Failure,
		FailureDetail: st.FailureDetail,
		Cleanup:       st.Cleanup,
		HealRounds:    st.HealRounds,
		Events:        evs,
	}
}

// actionTimeout 返回动作生效超时。
func actionTimeout(d time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return DefaultActionTimeout
}

// classifyErr 把执行器错误映射为可区分的失败原因；
// 非 ActionError 或未分类时使用 fallback。
func classifyErr(err error, fallback FailureReason) FailureReason {
	if err == nil {
		return ReasonNone
	}
	if ae := AsActionError(err); ae != nil {
		switch ae.Reason {
		case ReasonTargetUnreachable:
			return ReasonTargetUnreachable
		case ReasonActionTimeout:
			return ReasonActionTimeout
		case ReasonExperimentCanceled:
			return ReasonExperimentCanceled
		case ReasonProbeError:
			return ReasonProbeError
		}
	}
	return fallback
}
