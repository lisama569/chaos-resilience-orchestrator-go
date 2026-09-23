package engine

import (
	"context"
	"errors"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/action"
	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/policy"
	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/probe"
)

// observeAndHeal 执行"观测 -> 判定 -> 自愈 -> 重新观测"的有界闭环。
//
// 自愈调用总次数受 HealPolicySpec.MaxAttempts 严格限制（不无限重试）；
// 每次自愈使用唯一 attempt 编号，已终态的 heal 步骤不会被重复触发；
// 普通失败（heal_failed）可在剩余额度内继续尝试，超时/不可达立即中止（语义不可假设安全）；
// inconclusive（依据不足/相互矛盾/探针异常）不触发自愈，直接作为异常结论返回；
// 达到额度仍不稳态则明确中止（heal_exhausted）。
func (e *Engine) observeAndHeal(ctx context.Context, exp model.ExperimentSpec,
	t model.TargetSpec, res *model.TargetResult) policy.Verdict {

	maxCalls := 0
	if t.Heal != nil {
		maxCalls = t.Heal.MaxAttempts
	}
	healCalls := 0

	// 先做初始观测（attempt=0 表示尚未自愈）。
	evidence, probeReason := e.collectObservation(ctx, exp, t, 0)
	if probeReason == model.ReasonCanceled {
		return policy.Verdict{Target: t.ID, Outcome: model.ConclusionInconclusive, Reason: model.ReasonCanceled}
	}

	for {
		// 证据随采集立即持久化：中断重启后已确认观测不重复执行、不丢失判定依据。
		res.Evidence = dedupEvidence(append(e.loadEvidence(ctx, t.ID), evidence...))
		_, _ = e.mergeResult(ctx, &model.TargetResult{ID: t.ID, Evidence: res.Evidence})

		verdict := policy.New(t).Evaluate(evidence)
		if probeReason != model.ReasonNone {
			// 探针异常优先于规则判定：采集层出错时依据不可信，结论必须是 inconclusive。
			verdict.Outcome = model.ConclusionInconclusive
			verdict.Reason = probeReason
			verdict.Healthy = false
			verdict.Basis = append(verdict.Basis, "probe collection error => inconclusive: "+string(probeReason))
			_, _ = e.skipRemaining(ctx, t.ID, probeReason)
			return verdict
		}

		switch verdict.Outcome {
		case model.ConclusionSteady:
			e.skipUnusedHealSteps(ctx, exp, t, healCalls)
			return verdict
		case model.ConclusionInconclusive:
			// 依据不足 / 相互矛盾：不得默认通过，也不得盲目自愈。
			_, _ = e.skipRemaining(ctx, t.ID, verdict.Reason)
			return verdict
		case model.ConclusionNotSteady:
			if t.Heal == nil || healCalls >= maxCalls {
				if t.Heal != nil {
					verdict.Reason = model.ReasonHealExhausted
					verdict.Basis = append(verdict.Basis, "heal attempts exhausted without steady state")
					_, _ = e.skipRemaining(ctx, t.ID, model.ReasonHealExhausted)
				}
				return verdict
			}
		}

		// 触发下一次自愈（编号全局唯一，防重复触发）。
		healCalls++
		res.HealRuns = healCalls
		key := model.StepKey{Target: t.ID, Phase: model.PhaseHeal, Attempt: healCalls}
		ran, reason, detail := e.executeClassified(ctx, key, t.Heal.Action, action.ClassifyHeal)
		e.log.Printf("target=%s phase=heal call=%d/%d verdict=%s reason=%s detail=%s",
			t.ID, healCalls, maxCalls, map[bool]string{true: "already-completed", false: "executed"}[!ran], reason, detail)

		if reason == model.ReasonCanceled {
			return policy.Verdict{Target: t.ID, Outcome: model.ConclusionInconclusive, Reason: model.ReasonCanceled}
		}
		if reason == model.ReasonHealTimeout || reason == model.ReasonTargetUnreachable {
			// 不可假设动作安全生效：立即明确中止。
			e.skipUnusedHealSteps(ctx, exp, t, healCalls)
			_, _ = e.skipRemaining(ctx, t.ID, reason)
			return policy.Verdict{
				Target: t.ID, Outcome: model.ConclusionInconclusive, Reason: reason,
				Basis: []string{"heal aborted: " + detail},
			}
		}
		if reason != model.ReasonNone {
			// 普通失败：额度内继续下一次尝试（日志中保留每次失败的明确原因）。
			continue
		}

		// 自愈成功：重新观测。
		evidence, probeReason = e.collectObservation(ctx, exp, t, healCalls)
		if probeReason == model.ReasonCanceled {
			return policy.Verdict{Target: t.ID, Outcome: model.ConclusionInconclusive, Reason: model.ReasonCanceled}
		}
	}
}

// skipUnusedHealSteps 把收敛后未使用的自愈/观测步骤标记 skipped。
func (e *Engine) skipUnusedHealSteps(ctx context.Context, exp model.ExperimentSpec, t model.TargetSpec, used int) {
	max := 0
	if t.Heal != nil {
		max = t.Heal.MaxAttempts
	}
	for a := used + 1; a <= max; a++ {
		_, _ = e.skipStep(ctx, model.StepKey{Target: t.ID, Phase: model.PhaseHeal, Attempt: a}, model.ReasonNone)
		_, _ = e.skipStep(ctx, model.StepKey{Target: t.ID, Phase: model.PhaseObserve, Attempt: a}, model.ReasonNone)
	}
}

// collectObservation 执行一次稳态观测（含多轮间隔），返回证据与探针异常原因。
// 观测步骤以 (target, attempt, round=0) 落盘；探针错误/超时只影响该探针，其余照常采集。
func (e *Engine) collectObservation(ctx context.Context, exp model.ExperimentSpec,
	t model.TargetSpec, attempt int) ([]model.Evidence, model.FailureReason) {

	stepKey := model.StepKey{Target: t.ID, Phase: model.PhaseObserve, Attempt: attempt}
	status := e.stepStatus(ctx, stepKey)
	switch status {
	case model.StepSucceeded:
		// 已确认完成的观测不得重复执行：复用已持久化证据。
		return evidenceForAttempt(e.loadEvidence(ctx, t.ID), attempt), model.ReasonNone
	case model.StepSkipped:
		return nil, model.ReasonNone
	}
	if _, _, serr := e.startStep(ctx, stepKey); serr != nil {
		return nil, model.ReasonCanceled
	}

	var evidence []model.Evidence
	probeErrReason := model.ReasonNone

	for round := 1; round <= exp.ObserveRounds; round++ {
		if round > 1 && exp.ObserveEvery > 0 {
			if err := e.rt.Sleep(ctx, exp.ObserveEvery); err != nil {
				return evidence, model.ReasonCanceled
			}
		}
		for _, ps := range t.Probes {
			p, err := probe.Build(ps, e.rt)
			if err != nil {
				probeErrReason = model.ReasonProbeError
				e.log.Printf("target=%s probe=%s round=%d build-error=%s", t.ID, ps.Name, round, err)
				continue
			}

			cctx := ctx
			var cancel context.CancelFunc
			if ps.Timeout > 0 {
				cctx, cancel = context.WithTimeout(ctx, ps.Timeout)
			}
			start := e.rt.Now()
			val, perr := p.Collect(cctx, ps.Key)
			latency := e.rt.Now().Sub(start)
			if cancel != nil {
				cancel()
			}
			e.log.Printf("target=%s probe=%s round=%d key=%s value=%v err=%v latency=%s",
				t.ID, ps.Name, round, ps.Key, val, perr, latency)

			if perr != nil {
				switch {
				case errors.Is(perr, context.DeadlineExceeded):
					probeErrReason = model.ReasonProbeTimeout
				case ctx.Err() == context.Canceled:
					return evidence, model.ReasonCanceled
				default:
					probeErrReason = model.ReasonProbeError
				}
				continue
			}
			evidence = append(evidence, model.Evidence{
				Probe: ps.Name, Attempt: attempt, Round: round, Value: val,
				At: start, Latency: latency,
			})
		}
	}

	if probeErrReason == model.ReasonNone {
		if _, ferr := e.finishStep(ctx, stepKey, model.StepSucceeded, model.ReasonNone, ""); ferr != nil {
			return evidence, model.ReasonCanceled
		}
	} else {
		_, _ = e.finishStep(ctx, stepKey, model.StepFailed, probeErrReason, "probe error")
	}
	return evidence, probeErrReason
}

// loadEvidence 从持久化状态读回某目标已采集的证据。
func (e *Engine) loadEvidence(ctx context.Context, id model.TargetID) []model.Evidence {
	cur, err := e.store.Load(ctx, e.experimentID)
	if err != nil || cur.Results[id] == nil {
		return nil
	}
	return append([]model.Evidence(nil), cur.Results[id].Evidence...)
}

// dedupEvidence 按 (probe, attempt, round) 去重，保证重启不会产生重复证据。
func dedupEvidence(in []model.Evidence) []model.Evidence {
	type ek struct {
		probe          string
		attempt, round int
	}
	seen := map[ek]bool{}
	var out []model.Evidence
	for _, ev := range in {
		k := ek{ev.Probe, ev.Attempt, ev.Round}
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, ev)
	}
	return out
}

// evidenceForAttempt 过滤出指定自愈轮次的证据。
func evidenceForAttempt(in []model.Evidence, attempt int) []model.Evidence {
	var out []model.Evidence
	for _, ev := range in {
		if ev.Attempt == attempt {
			out = append(out, ev)
		}
	}
	return out
}
