package engine

import (
	"context"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/action"
	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
)

// maxCleanupAttempts 限制清理重试次数，保证不会无限重试。
const maxCleanupAttempts = 2

// scheduleTargets 按并发上限执行所有目标，返回每个目标的结果。
// 目标之间互不阻塞：单个目标失败/取消不影响其他目标的注入清理，
// 但实验是否被取消由共享 ctx 传播（Interrupt）。
func (e *Engine) scheduleTargets(ctx context.Context, st *model.ExperimentState) []*model.TargetResult {
	spec := st.Spec
	n := spec.Concurrency
	if n <= 0 {
		n = len(spec.Targets)
	}
	if n < 1 {
		n = 1
	}
	sem := make(chan struct{}, n)

	type indexed struct {
		idx int
		res *model.TargetResult
	}
	out := make(chan indexed, len(spec.Targets))

	for i, t := range spec.Targets {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		}
		go func(i int, t model.TargetSpec) {
			defer func() { <-sem }()
			res := e.runTarget(ctx, st, t)
			out <- indexed{i, res}
		}(i, t)
	}

	results := make([]*model.TargetResult, len(spec.Targets))
	for range spec.Targets {
		r := <-out
		results[r.idx] = r.res
	}
	return results
}

// runTarget 执行单个目标的完整流水线：可达性 -> 注入 -> 观测+自愈 -> 清理。
func (e *Engine) runTarget(ctx context.Context, st *model.ExperimentState, t model.TargetSpec) *model.TargetResult {
	res := &model.TargetResult{ID: t.ID, Verdict: model.ConclusionInconclusive}

	// 1. 可达性检查
	if t.Reach != nil {
		reason, detail, skip := e.runReachability(ctx, t)
		if skip {
			res.Reason = reason
			res.Verdict = model.ConclusionInconclusive
			res.CleanedUp = true // 未注入任何故障，无需清理
			e.log.Printf("target=%s phase=reachability verdict=skip reason=%s detail=%s", t.ID, reason, detail)
			// 注入及其后所有步骤（含 cleanup）均无意义：全部跳过。
			_, _ = e.skipAllBut(ctx, t.ID, model.PhaseReachability, reason)
			return res
		}
	}

	// 2. 故障注入
	injected, reason, detail := e.runInject(ctx, t)
	if !injected {
		res.Reason = reason
		res.Verdict = model.ConclusionInconclusive
		if reason == model.ReasonCanceled {
			// 实验被中断：注入步骤保持 running，下游步骤保持 pending，
			// 交给恢复规则重置注入步骤后续跑；cleanup 同样保留 pending。
			e.log.Printf("target=%s phase=inject verdict=interrupted", t.ID)
			_, _ = e.markResult(ctx, res)
			return res
		}
		res.CleanedUp = true // 注入未生效，无半生效状态需要清理
		e.log.Printf("target=%s phase=inject verdict=skip reason=%s detail=%s", t.ID, reason, detail)
		// 观测/自愈无意义；cleanup 也无需执行（没有生效的故障），显式跳过。
		_, _ = e.skipFrom(ctx, t.ID, model.PhaseInject, reason)
		_, _ = e.markResult(ctx, res)
		return res
	}
	e.log.Printf("target=%s phase=inject verdict=injected", t.ID)

	// 3. 稳态观测 + 有限次自愈
	verdict := e.observeAndHeal(ctx, st.Spec, t, res)
	res.Verdict = verdict.Outcome
	res.Healthy = verdict.Healthy
	res.Evidence = verdict.Evidence
	if verdict.Reason != model.ReasonNone && res.Reason == model.ReasonNone {
		res.Reason = verdict.Reason
	}
	for _, b := range verdict.Basis {
		e.log.Printf("target=%s basis: %s", t.ID, b)
	}

	// 4. 清理：无论判定结论、失败或取消，只要注入生效就必须最终清理。
	cleaned, creason, cdetail := e.runCleanup(ctx, t)
	res.CleanedUp = cleaned
	if !cleaned {
		res.Reason = creason // 清理失败具有最高优先级
		e.log.Printf("target=%s phase=cleanup verdict=failed reason=%s detail=%s", t.ID, creason, cdetail)
	} else {
		e.log.Printf("target=%s phase=cleanup verdict=cleaned", t.ID)
	}

	_, _ = e.markResult(ctx, res)
	return res
}

// runReachability 执行可选的可达性检查动作。
// skip=true 表示目标不可达（或检查动作失败/超时），调用方必须跳过后续全部步骤。
func (e *Engine) runReachability(ctx context.Context, t model.TargetSpec) (model.FailureReason, string, bool) {
	key := model.StepKey{Target: t.ID, Phase: model.PhaseReachability}
	// 可达性阶段：超时、不可达、普通失败一律视为目标不可达（实验取消由上层单独识别）。
	_, reason, detail := e.executeClassified(ctx, key, *t.Reach,
		func(err error) model.FailureReason { return model.ReasonTargetUnreachable })
	if reason == model.ReasonCanceled {
		return model.ReasonCanceled, detail, true
	}
	if reason != model.ReasonNone {
		return model.ReasonTargetUnreachable, detail, true
	}
	return model.ReasonNone, "", false
}

func (e *Engine) runInject(ctx context.Context, t model.TargetSpec) (bool, model.FailureReason, string) {
	key := model.StepKey{Target: t.ID, Phase: model.PhaseInject}
	_, reason, detail := e.executeClassified(ctx, key, t.Inject, action.ClassifyInject)
	if reason != model.ReasonNone {
		return false, reason, detail
	}
	return true, model.ReasonNone, ""
}

func (e *Engine) runCleanup(ctx context.Context, t model.TargetSpec) (bool, model.FailureReason, string) {
	// 取消后仍尝试清理：使用独立的截止时间上下文，保证清理能执行且必然有界。
	cctx := ctx
	cancel := func() {}
	if ctx.Err() != nil {
		d := t.Cleanup.Timeout
		if d <= 0 {
			d = e.defaultCleanupTimeout
		}
		cctx, cancel = context.WithTimeout(context.Background(), d)
	}
	defer cancel()
	for attempt := 0; attempt < maxCleanupAttempts; attempt++ {
		key := model.StepKey{Target: t.ID, Phase: model.PhaseCleanup, Attempt: attempt}
		ran, reason, detail := e.executeClassified(cctx, key, t.Cleanup, action.ClassifyCleanup)
		if reason == model.ReasonNone {
			// 成功：未使用的重试步骤标记 skipped，保证步骤终态完整。
			for a := attempt + 1; a < maxCleanupAttempts; a++ {
				_, _ = e.skipStep(cctx,
					model.StepKey{Target: t.ID, Phase: model.PhaseCleanup, Attempt: a},
					model.ReasonNone)
			}
			return true, model.ReasonNone, ""
		}
		if reason == model.ReasonCanceled {
			return false, model.ReasonCleanupFailed, "cleanup canceled"
		}
		e.log.Printf("target=%s cleanup attempt=%d ran=%v failed reason=%s detail=%s",
			t.ID, attempt+1, !ran, reason, detail)
	}
	return false, model.ReasonCleanupFailed, "cleanup retries exhausted"
}

// executeClassified 执行动作并把错误映射成可区分的失败原因后落终态。
func (e *Engine) executeClassified(ctx context.Context, key model.StepKey,
	spec model.ActionSpec, classify func(error) model.FailureReason) (bool, model.FailureReason, string) {
	act, err := action.Build(spec)
	if err != nil {
		_, _ = e.finishStep(ctx, key, model.StepFailed, model.ReasonInjectFailed, err.Error())
		return false, classify(err), err.Error()
	}
	st, terminal, err := e.startStep(ctx, key)
	if err != nil {
		return false, model.ReasonCanceled, err.Error()
	}
	if terminal {
		step := model.FindStep(st.Steps, key)
		if step.Status == model.StepSucceeded {
			return true, model.ReasonNone, "already succeeded"
		}
		return false, step.Reason, "previously " + string(step.Status)
	}

	cctx := ctx
	var cancel context.CancelFunc
	if spec.Timeout > 0 {
		cctx, cancel = context.WithTimeout(ctx, spec.Timeout)
	}
	if cancel != nil {
		defer cancel()
	}
	execErr := act.Execute(cctx, e.rt, spec.Params)
	if execErr == nil {
		if _, err := e.finishStep(ctx, key, model.StepSucceeded, model.ReasonNone, ""); err != nil {
			return false, model.ReasonCanceled, err.Error()
		}
		return true, model.ReasonNone, ""
	}
	// 实验级取消优先于动作分类：结果未知，步骤保持 running（不落 failed）。
	// 重启后由恢复规则重置为 pending 重试，绝不把未知结果当成已确认失败。
	if ctx.Err() == context.Canceled && cctx.Err() != context.DeadlineExceeded {
		return false, model.ReasonCanceled, execErr.Error()
	}
	reason := classify(execErr)
	if ctx.Err() == context.Canceled && reason != model.ReasonInjectTimeout &&
		reason != model.ReasonCleanupTimeout && reason != model.ReasonHealTimeout {
		return false, model.ReasonCanceled, execErr.Error()
	}
	_, _ = e.finishStep(ctx, key, model.StepFailed, reason, execErr.Error())
	return false, reason, execErr.Error()
}
