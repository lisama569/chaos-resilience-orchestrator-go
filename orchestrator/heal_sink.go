package orchestrator

import (
	"context"
	"fmt"
)

// healSink 是 ProgressSink 的引擎实现：
//   - FireHealAction 以步骤标记做去重，同一轮动作绝不重复触发；
//   - Recheck 复用确定性评估器，依据完整写入审计事件。
type healSink struct {
	engine *Engine
	ctx    context.Context
	spec   TargetSpec
	st     *TargetState
	last   *Evaluation
}

func newHealSink(engine *Engine, ctx context.Context, spec TargetSpec, st *TargetState) *healSink {
	return &healSink{engine: engine, ctx: ctx, spec: spec, st: st}
}

// stepName 返回某一轮自愈动作的唯一步骤键。
func healActionStep(round int) string { return fmt.Sprintf("heal:%d:action", round) }

// FireHealAction 执行（或跳过）一轮自愈动作。
//
// 步骤已确认完成时返回 (false, nil)：调用方据此只补做复核，绝不重复触发。
// 同一轮内的直接重复调用会被 MarkDone 以 ErrStaleReport 明确拒绝。
func (s *healSink) FireHealAction(round int, act ActionSpec) (bool, error) {
	name := healActionStep(round)
	if s.st.IsStepDone(name) {
		s.engine.logf("[target=%s] heal round=%d action already fired, skip duplicate trigger", s.spec.ID, round)
		s.engine.event(s.spec.ID, "heal_action_skipped_duplicate", map[string]any{"round": round, "step": name})
		return false, nil
	}

	actCtx, cancel := context.WithTimeout(s.ctx, actionTimeout(act.Timeout))
	err := s.engine.exec.Do(actCtx, act)
	cancel()
	s.engine.logf("[target=%s] heal round=%d action=%s err=%v", s.spec.ID, round, act.Name, err)
	s.engine.event(s.spec.ID, "heal_action_fired", map[string]any{"round": round, "action": act.Name, "ok": err == nil})
	if err != nil {
		return false, err
	}
	if merr := s.engine.mutate(func() {
		_ = s.st.MarkDone(name, s.st.AttemptID, s.engine.now())
	}); merr != nil {
		return false, merr
	}
	return true, nil
}

// Recheck 在一轮自愈动作后重新采集证据并做确定性判定。
func (s *healSink) Recheck(round int) (*Evaluation, error) {
	policy := s.spec.Heal
	ev, err := s.engine.evaluator.Evaluate(
		round+1,
		policy.RecheckProbes,
		policy.RecheckRules,
		s.engine.sampleFunc(s.ctx, s.spec, policy.RecheckProbes),
	)
	if err != nil {
		return nil, err
	}
	s.last = ev
	s.engine.logf("[target=%s] heal recheck round=%d verdict=%s basis=%s", s.spec.ID, round+1, ev.Verdict, ev.BasisSummary)
	s.engine.event(s.spec.ID, "heal_rechecked", map[string]any{
		"round": round + 1, "verdict": string(ev.Verdict), "basis": ev.BasisSummary,
	})
	if merr := s.engine.mutate(func() {
		s.st.Verdict = ev.Verdict
		_ = s.st.MarkDone(fmt.Sprintf("heal:%d:recheck", round), s.st.AttemptID, s.engine.now())
	}); merr != nil {
		return nil, merr
	}
	return ev, nil
}
