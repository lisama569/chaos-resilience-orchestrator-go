package engine

import (
	"testing"
	"time"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
)

// 正常链路：注入后探针全部满足 -> steady，清理执行。
func TestRunSteady(t *testing.T) {
	h := newHarness(t)
	spec := model.ExperimentSpec{
		ID: "steady", Concurrency: 1, ObserveRounds: 2, ResumePolicy: model.ResumeResume,
		Targets: []model.TargetSpec{{
			ID:      "a",
			Inject:  act("set-flag", time.Second, map[string]string{"key": "a/inj", "value": "true"}),
			Cleanup: act("set-flag", time.Second, map[string]string{"key": "a/inj", "value": "false"}),
			Probes:  []model.ProbeSpec{pr("p", "flag", "a/inj", true, 2)},
		}},
	}
	st := h.run(t, spec)
	if st.Status != model.StatusSucceeded || st.Conclusion != model.ConclusionSteady {
		t.Fatalf("status=%s conclusion=%s reason=%s", st.Status, st.Conclusion, st.Reason)
	}
	r := st.Results["a"]
	if !r.CleanedUp || r.HealRuns != 0 || !r.Healthy {
		t.Fatalf("unexpected target result: %+v", r)
	}
	if h.rt.Flag("a/inj") {
		t.Fatal("fault flag must be cleared after cleanup")
	}
}

// 自愈联动：注入使服务不健康，一次自愈后探针满足 -> 有限次数内收敛。
func TestHealConverges(t *testing.T) {
	h := newHarness(t)
	spec := model.ExperimentSpec{
		ID: "heal-ok", Concurrency: 1, ObserveRounds: 1, ResumePolicy: model.ResumeResume,
		Targets: []model.TargetSpec{{
			ID:      "b",
			Inject:  act("set-flag", time.Second, map[string]string{"key": "b/svc", "value": "false"}),
			Cleanup: act("set-flag", time.Second, map[string]string{"key": "b/svc", "value": "true"}),
			Heal: &model.HealPolicySpec{
				MaxAttempts: 3,
				Action:      act("set-flag", time.Second, map[string]string{"key": "b/svc", "value": "true"}),
			},
			Probes: []model.ProbeSpec{pr("p", "flag", "b/svc", true, 1)},
		}},
	}
	st := h.run(t, spec)
	r := st.Results["b"]
	if r.HealRuns != 1 {
		t.Fatalf("heal runs=%d want 1 (no repeated triggering)", r.HealRuns)
	}
	if !r.Healthy || r.Verdict != model.ConclusionSteady {
		t.Fatalf("verdict=%s healthy=%v reason=%s", r.Verdict, r.Healthy, r.Reason)
	}
}

// 自愈耗尽：自愈动作不改变探针结果，达到上限后明确中止（heal_exhausted），不无限重试。
func TestHealExhausted(t *testing.T) {
	h := newHarness(t)
	spec := model.ExperimentSpec{
		ID: "heal-exhausted", Concurrency: 1, ObserveRounds: 1, ResumePolicy: model.ResumeResume,
		Targets: []model.TargetSpec{{
			ID: "c",
			// 注入制造两个效果：故障标记 + 服务不健康；清理只复位故障标记，
			// 探针只看服务健康，从而能独立验证"故障最终被清理"。
			Inject:  act("inc-metric", time.Second, map[string]string{"key": "c/injected", "delta": "1"}),
			Cleanup: act("inc-metric", time.Second, map[string]string{"key": "c/injected", "delta": "-1"}),
			Heal: &model.HealPolicySpec{
				MaxAttempts: 2,
				// 自愈动作修复了另一个 key，对探针无效 => 永不收敛
				Action: act("set-flag", time.Second, map[string]string{"key": "c/other", "value": "true"}),
			},
			Probes: []model.ProbeSpec{pr("p", "flag", "c/svc", true, 1)},
		}},
	}
	st := h.run(t, spec)
	r := st.Results["c"]
	if r.HealRuns != 2 {
		t.Fatalf("heal runs=%d want exactly 2", r.HealRuns)
	}
	if r.Verdict != model.ConclusionNotSteady || r.Reason != model.ReasonHealExhausted {
		t.Fatalf("verdict=%s reason=%s", r.Verdict, r.Reason)
	}
	// 清理仍必须执行，故障不留存
	if !r.CleanedUp || h.rt.Metric("c/injected") != 0 {
		t.Fatalf("cleanup must run; cleaned=%v injected-metric=%d", r.CleanedUp, h.rt.Metric("c/injected"))
	}
}

// 目标不可达：给出 target_unreachable，步骤全部跳过，实验 failed/inconclusive。
func TestTargetUnreachable(t *testing.T) {
	h := newHarness(t)
	spec := model.ExperimentSpec{
		ID: "unreachable", Concurrency: 1, ObserveRounds: 1, ResumePolicy: model.ResumeResume,
		Targets: []model.TargetSpec{{
			ID:      "u",
			Reach:   ptr(act("unreachable", time.Second, nil)),
			Inject:  act("set-flag", time.Second, map[string]string{"key": "u/x", "value": "true"}),
			Cleanup: act("set-flag", time.Second, map[string]string{"key": "u/x", "value": "false"}),
			Probes:  []model.ProbeSpec{pr("p", "flag", "u/x", true, 1)},
		}},
	}
	st := h.run(t, spec)
	if st.Status != model.StatusFailed || st.Reason != model.ReasonTargetUnreachable {
		t.Fatalf("status=%s reason=%s", st.Status, st.Reason)
	}
	r := st.Results["u"]
	if r.Verdict != model.ConclusionInconclusive || r.CleanedUp != true {
		t.Fatalf("result=%+v", r)
	}
}

// 注入超时：动作超过注入截止时间 -> inject_timeout，与不可达/普通失败可区分。
func TestInjectTimeout(t *testing.T) {
	h := newHarness(t)
	spec := model.ExperimentSpec{
		ID: "inject-timeout", Concurrency: 1, ObserveRounds: 1, ResumePolicy: model.ResumeResume,
		Targets: []model.TargetSpec{{
			ID:      "to",
			Inject:  act("sleep", 20*time.Millisecond, map[string]string{"duration": "5s", "flag": "to/x"}),
			Cleanup: act("set-flag", time.Second, map[string]string{"key": "to/x", "value": "false"}),
			Probes:  []model.ProbeSpec{pr("p", "flag", "to/x", true, 1)},
		}},
	}
	st := h.run(t, spec)
	if st.Status != model.StatusFailed || st.Reason != model.ReasonInjectTimeout {
		t.Fatalf("status=%s reason=%s", st.Status, st.Reason)
	}
	if h.rt.Flag("to/x") {
		t.Fatal("timed-out injection must not leave half-applied flag")
	}
}

// 稳态判定异常：探针恒报错 -> probe_error，结论 inconclusive，且不触发自愈。
func TestProbeErrorNoHeal(t *testing.T) {
	h := newHarness(t)
	healed := false
	_ = healed
	spec := model.ExperimentSpec{
		ID: "probe-error", Concurrency: 1, ObserveRounds: 1, ResumePolicy: model.ResumeResume,
		Targets: []model.TargetSpec{{
			ID:      "pe",
			Inject:  act("set-flag", time.Second, map[string]string{"key": "pe/x", "value": "true"}),
			Cleanup: act("set-flag", time.Second, map[string]string{"key": "pe/x", "value": "false"}),
			Heal: &model.HealPolicySpec{
				MaxAttempts: 1,
				Action:      act("inc-metric", time.Second, map[string]string{"key": "pe/healed", "delta": "1"}),
			},
			Probes: []model.ProbeSpec{{Name: "bad", Kind: "error", Key: "pe/x", Want: true, MinHits: 1, Timeout: time.Second}},
		}},
	}
	st := h.run(t, spec)
	r := st.Results["pe"]
	if r.Verdict != model.ConclusionInconclusive || r.Reason != model.ReasonProbeError {
		t.Fatalf("verdict=%s reason=%s", r.Verdict, r.Reason)
	}
	if r.HealRuns != 0 || h.rt.Metric("pe/healed") != 0 {
		t.Fatal("inconclusive due to probe error must not trigger healing")
	}
}

// 清理失败：清理动作始终失败 -> cleanup_failed，实验 failed/inconclusive，
// 与"业务未达稳态但实验成功"明确区分。
func TestCleanupFailure(t *testing.T) {
	h := newHarness(t)
	spec := model.ExperimentSpec{
		ID: "cleanup-fail", Concurrency: 1, ObserveRounds: 1, ResumePolicy: model.ResumeResume,
		Targets: []model.TargetSpec{{
			ID:     "cf",
			Inject: act("set-flag", time.Second, map[string]string{"key": "cf/x", "value": "true"}),
			// set-flag 缺少 value 参数 => 恒失败
			Cleanup: act("set-flag", time.Second, map[string]string{"key": "cf/x"}),
			Probes:  []model.ProbeSpec{pr("p", "flag", "cf/x", true, 1)},
		}},
	}
	st := h.run(t, spec)
	if st.Status != model.StatusFailed || st.Reason != model.ReasonCleanupFailed {
		t.Fatalf("status=%s reason=%s", st.Status, st.Reason)
	}
	if st.Conclusion != model.ConclusionInconclusive {
		t.Fatalf("cleanup failure => inconclusive, got %s", st.Conclusion)
	}
}

// 并发上限：多个目标同时执行仍得到完整、确定的结果。
func TestConcurrencyMixed(t *testing.T) {
	h := newHarness(t)
	spec := model.ExperimentSpec{
		ID: "mixed", Concurrency: 2, ObserveRounds: 1, ResumePolicy: model.ResumeResume,
		Targets: []model.TargetSpec{
			{
				ID: "ok", Inject: act("set-flag", time.Second, map[string]string{"key": "ok/x", "value": "true"}),
				Cleanup: act("set-flag", time.Second, map[string]string{"key": "ok/x", "value": "false"}),
				Probes:  []model.ProbeSpec{pr("p", "flag", "ok/x", true, 1)},
			},
			{
				ID: "bad", Reach: ptr(act("unreachable", time.Second, nil)),
				Inject:  act("set-flag", time.Second, map[string]string{"key": "bad/x", "value": "true"}),
				Cleanup: act("set-flag", time.Second, map[string]string{"key": "bad/x", "value": "false"}),
				Probes:  []model.ProbeSpec{pr("p", "flag", "bad/x", true, 1)},
			},
		},
	}
	st := h.run(t, spec)
	if st.Status != model.StatusFailed || st.Reason != model.ReasonTargetUnreachable {
		t.Fatalf("status=%s reason=%s", st.Status, st.Reason)
	}
	if st.Results["ok"].Verdict != model.ConclusionSteady {
		t.Fatalf("ok target should be steady: %+v", st.Results["ok"])
	}
	if !h.rt.Flag("ok/x") == false && h.rt.Flag("ok/x") {
		t.Fatal("ok target fault must be cleaned")
	}
}

func ptr(a model.ActionSpec) *model.ActionSpec { return &a }

// 自愈动作 flaky：第 1 次自愈调用失败，随后策略进入下一次自愈尝试并成功。
// 验证自愈失败被明确记录、有限重试内收敛、收敛后不再重复触发后续自愈动作。
func TestFlakyHealRetriesAndConverges(t *testing.T) {
	h := newHarness(t)
	spec := model.ExperimentSpec{
		ID: "flaky-heal", Concurrency: 1, ObserveRounds: 1, ResumePolicy: model.ResumeResume,
		Targets: []model.TargetSpec{{
			ID: "f",
			// 注入：服务标记置 false
			Inject:  act("set-flag", time.Second, map[string]string{"key": "f/svc", "value": "false"}),
			Cleanup: act("set-flag", time.Second, map[string]string{"key": "f/svc", "value": "true"}),
			Heal: &model.HealPolicySpec{
				MaxAttempts: 3,
				// 第 1 次调用失败，之后成功把服务标记置 true。
				Action: act("flaky", time.Second, map[string]string{
					"key": "f/heal-calls", "fail_until": "1",
					"flag": "f/svc", "value": "true",
				}),
			},
			Probes: []model.ProbeSpec{pr("p", "flag", "f/svc", true, 1)},
		}},
	}
	st := h.run(t, spec)
	r := st.Results["f"]
	if r.Verdict != model.ConclusionSteady || !r.Healthy {
		t.Fatalf("verdict=%s healthy=%v reason=%s basis=%v", r.Verdict, r.Healthy, r.Reason, st.Results["f"].Evidence)
	}
	if r.HealRuns != 2 {
		t.Fatalf("heal runs=%d want 2 (1 flaky fail + 1 success)", r.HealRuns)
	}
	// 第一个自愈动作失败必须有明确的失败原因落盘
	failed := model.FindStep(h.load(t, "flaky-heal").Steps,
		model.StepKey{Target: "f", Phase: model.PhaseHeal, Attempt: 1})
	if failed.Status != model.StepFailed || failed.Reason != model.ReasonHealFailed {
		t.Fatalf("heal attempt 1 = %s/%s, want failed/heal_failed", failed.Status, failed.Reason)
	}
	// 第三个自愈动作从未触发（收敛后不重复触发）
	third := model.FindStep(h.load(t, "flaky-heal").Steps,
		model.StepKey{Target: "f", Phase: model.PhaseHeal, Attempt: 3})
	if third.Status != model.StepSkipped {
		t.Fatalf("heal attempt 3 = %s, want skipped (no duplicate trigger after convergence)", third.Status)
	}
}

// 清理首次失败、第二次成功：有界重试必须真正执行第二次清理，最终实验成功、故障清除。
func TestCleanupFlakyRetriesAndSucceeds(t *testing.T) {
	h := newHarness(t)
	spec := model.ExperimentSpec{
		ID: "cleanup-flaky", Concurrency: 1, ObserveRounds: 1, ResumePolicy: model.ResumeResume,
		Targets: []model.TargetSpec{{
			ID:     "x",
			Inject: act("set-flag", time.Second, map[string]string{"key": "x/fault", "value": "true"}),
			// 清理动作 flaky：前 1 次失败，之后成功复位（setFlag 由 flaky 成功分支执行）。
			Cleanup: act("flaky", time.Second, map[string]string{
				"key": "x/cleanup-calls", "fail_until": "1",
				"flag": "x/fault", "value": "false",
			}),
			Probes: []model.ProbeSpec{pr("p", "flag", "x/fault", true, 1)},
		}},
	}
	st := h.run(t, spec)
	if st.Status != model.StatusSucceeded || st.Conclusion != model.ConclusionSteady {
		t.Fatalf("status=%s conclusion=%s reason=%s", st.Status, st.Conclusion, st.Reason)
	}
	if h.rt.Flag("x/fault") {
		t.Fatal("fault must be cleared after flaky cleanup retries")
	}
	saved := h.load(t, "cleanup-flaky")
	c0 := model.FindStep(saved.Steps, model.StepKey{Target: "x", Phase: model.PhaseCleanup, Attempt: 0})
	c1 := model.FindStep(saved.Steps, model.StepKey{Target: "x", Phase: model.PhaseCleanup, Attempt: 1})
	if c0.Status != model.StepFailed || c0.Reason != model.ReasonCleanupFailed {
		t.Fatalf("cleanup attempt 0 = %s/%s", c0.Status, c0.Reason)
	}
	if c1.Status != model.StepSucceeded {
		t.Fatalf("cleanup attempt 1 = %s want succeeded", c1.Status)
	}
}
