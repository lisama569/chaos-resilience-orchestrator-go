package engine

import (
	"context"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
)

// abortAndCleanup 处理 resume_policy=abort 或状态为 paused 的未完成实验：
// 不再继续实验流程，但对"注入已确认成功"的目标必须执行清理，随后标记实验 aborted。
// 已终态步骤不会重复执行；崩溃时处于 running 的注入步骤视为未确认，不执行清理。
func (e *Engine) abortAndCleanup(ctx context.Context, st *model.ExperimentState) (*model.ExperimentState, error) {
	e.experimentID = st.ID

	// 崩溃时 running 的步骤重置为 pending（随后按规则跳过或清理）。
	if _, err := e.resetRunningSteps(ctx); err != nil {
		return nil, err
	}

	// pending/paused -> running；崩溃遗留的 running 状态保持 running（running->running 非法）。
	cur, err := e.store.Load(ctx, st.ID)
	if err != nil {
		return nil, err
	}
	if cur.Status != model.StatusRunning {
		if cur, err = e.setStatus(ctx, model.StatusRunning); err != nil {
			return nil, err
		}
	}
	st = cur

	var firstReason model.FailureReason
	for _, t := range st.Spec.Targets {
		if !e.wasInjected(ctx, t.ID) {
			// 注入未确认成功：不存在半生效故障，全部步骤跳过。
			_, _ = e.skipRemaining(ctx, t.ID, model.ReasonCanceled)
			continue
		}
		d := t.Cleanup.Timeout
		if d <= 0 {
			d = e.defaultCleanupTimeout
		}
		cctx, cancel := context.WithTimeout(context.Background(), d)
		cleaned, reason, detail := e.runCleanup(cctx, t)
		cancel()
		res := &model.TargetResult{
			ID: t.ID, Verdict: model.ConclusionInconclusive,
			Reason: model.ReasonCanceled, CleanedUp: cleaned,
		}
		if !cleaned {
			e.log.Printf("abort-cleanup failed: target=%s reason=%s detail=%s", t.ID, reason, detail)
			res.Reason = reason
			if firstReason == model.ReasonNone {
				firstReason = reason
			}
		}
		if _, err := e.markResult(ctx, res); err != nil {
			return nil, err
		}
	}

	// 终态落盘：清理失败 => failed/inconclusive（可能残留故障）；否则 aborted。
	stepCommitMu.Lock()
	cur, err = e.store.Load(ctx, st.ID)
	stepCommitMu.Unlock()
	if err != nil {
		return nil, err
	}
	final := *cur
	if firstReason != model.ReasonNone {
		final.Status = model.StatusFailed
		final.Conclusion = model.ConclusionInconclusive
		final.Reason = firstReason
	} else {
		final.Status = model.StatusAborted
		final.Conclusion = model.ConclusionInconclusive
		final.Reason = model.ReasonCanceled
	}
	final.UpdatedAt = e.rt.Now()

	stepCommitMu.Lock()
	defer stepCommitMu.Unlock()
	cur2, err := e.store.Load(ctx, st.ID)
	if err != nil {
		return nil, err
	}
	return e.store.Commit(ctx, &final, cur2.Version)
}

// wasInjected 判断崩溃前注入是否已确认成功（以持久化的 succeeded 步骤为准）。
func (e *Engine) wasInjected(ctx context.Context, id model.TargetID) bool {
	cur, err := e.store.Load(ctx, e.experimentID)
	if err != nil {
		return false
	}
	step := model.FindStep(cur.Steps, model.StepKey{Target: id, Phase: model.PhaseInject})
	return step != nil && step.Status == model.StepSucceeded
}
