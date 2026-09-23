package orchestrator

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func runOnce(t *testing.T, plan *Plan, exec Executor, store Store) *Report {
	t.Helper()
	eng := NewEngine(exec, store, WithLogger(t.Logf))
	rep, err := eng.Run(context.Background(), plan)
	if err != nil {
		t.Fatalf("engine run: %v", err)
	}
	return rep
}

// 目标不可达：不注入、不清理，原因可区分。
func TestEngineTargetUnreachable(t *testing.T) {
	exec := NewSimExecutor(simScripts(map[string]*Script{"reach": {Unreachable: true}}))
	rep := runOnce(t, testPlan(testTarget("dead", false)), exec, NewMemoryStore())
	r := rep.Results[0]
	t.Logf("result phase=%s reason=%s cleanup=%s detail=%q", r.Phase, r.Failure, r.Cleanup, r.FailureDetail)
	if r.Phase != PhaseAborted || r.Failure != ReasonTargetUnreachable || r.Cleanup != CleanupNotNeeded {
		t.Fatalf("unexpected result: %+v", r)
	}
}

// 故障注入动作普通失败：实验中止，无需清理。
func TestEngineInjectFailed(t *testing.T) {
	exec := NewSimExecutor(simScripts(map[string]*Script{"inject": {DoFail: true}}))
	rep := runOnce(t, testPlan(testTarget("x", false)), exec, NewMemoryStore())
	r := rep.Results[0]
	t.Logf("result phase=%s reason=%s cleanup=%s", r.Phase, r.Failure, r.Cleanup)
	if r.Phase != PhaseAborted || r.Failure != ReasonInjectFailed || r.Cleanup != CleanupNotNeeded {
		t.Fatalf("unexpected result: %+v", r)
	}
}

// 故障注入动作超时：注入状态未知，仍必须执行清理，原因标记为超时。
func TestEngineInjectTimeoutStillCleansUp(t *testing.T) {
	exec := NewSimExecutor(simScripts(map[string]*Script{"inject": {DoDelay: 200 * time.Millisecond}}))
	spec := testTarget("x", false)
	spec.Inject.Timeout = 20 * time.Millisecond
	rep := runOnce(t, testPlan(spec), exec, NewMemoryStore())
	r := rep.Results[0]
	t.Logf("result phase=%s reason=%s cleanup=%s", r.Phase, r.Failure, r.Cleanup)
	if r.Failure != ReasonActionTimeout || r.Cleanup != CleanupSucceeded {
		t.Fatalf("timeout must be distinguished and cleanup enforced, got %+v", r)
	}
}

// 稳态探测异常：依据不足，绝不默认通过，仍清理故障。
func TestEngineProbeErrorInconclusive(t *testing.T) {
	exec := NewSimExecutor(simScripts(map[string]*Script{"probe": {ProbeFail: true}}))
	rep := runOnce(t, testPlan(testTarget("x", false)), exec, NewMemoryStore())
	r := rep.Results[0]
	t.Logf("result phase=%s verdict=%s reason=%s cleanup=%s", r.Phase, r.Verdict, r.Failure, r.Cleanup)
	if r.Phase != PhaseAborted || r.Verdict != VerdictInconclusive ||
		r.Failure != ReasonEvidenceInsufficient || r.Cleanup != CleanupSucceeded {
		t.Fatalf("unexpected result: %+v", r)
	}
}

// 稳态违例 + 自愈：首轮违例触发 heal，复核恢复稳态，收敛完成。
func TestEngineViolatedThenHealConverges(t *testing.T) {
	exec := NewSimExecutor(simScripts(map[string]*Script{
		"probe":   {Values: map[string]float64{"err": 0.9}},
		"reprobe": {Values: map[string]float64{"err": 0.0}},
	}))
	rep := runOnce(t, testPlan(testTarget("x", true)), exec, NewMemoryStore())
	r := rep.Results[0]
	t.Logf("result phase=%s verdict=%s rounds=%d cleanup=%s", r.Phase, r.Verdict, r.HealRounds, r.Cleanup)
	if r.Phase != PhaseCompleted || r.Verdict != VerdictSteady || r.HealRounds != 1 || r.Cleanup != CleanupSucceeded {
		t.Fatalf("unexpected result: %+v", r)
	}
}

// 自愈多轮仍不收敛：达到硬上限后明确中止，轮次有界。
func TestEngineHealNotConverged(t *testing.T) {
	exec := NewSimExecutor(simScripts(map[string]*Script{
		"probe":   {Values: map[string]float64{"err": 0.9}},
		"reprobe": {Values: map[string]float64{"err": 0.8}},
	}))
	rep := runOnce(t, testPlan(testTarget("x", true)), exec, NewMemoryStore())
	r := rep.Results[0]
	t.Logf("result phase=%s reason=%s rounds=%d cleanup=%s", r.Phase, r.Failure, r.HealRounds, r.Cleanup)
	if r.Phase != PhaseAborted || r.Failure != ReasonHealNotConverged || r.HealRounds != 2 || r.Cleanup != CleanupSucceeded {
		t.Fatalf("unexpected result: %+v", r)
	}
}

// 自愈动作本身失败：明确中止且原因可区分。
func TestEngineHealActionFails(t *testing.T) {
	exec := NewSimExecutor(simScripts(map[string]*Script{
		"probe": {Values: map[string]float64{"err": 0.9}},
		"heal":  {DoFail: true},
	}))
	rep := runOnce(t, testPlan(testTarget("x", true)), exec, NewMemoryStore())
	r := rep.Results[0]
	t.Logf("result phase=%s reason=%s rounds=%d cleanup=%s", r.Phase, r.Failure, r.HealRounds, r.Cleanup)
	if r.Failure != ReasonHealActionFailed || r.Phase != PhaseAborted || r.Cleanup != CleanupSucceeded {
		t.Fatalf("unexpected result: %+v", r)
	}
}

// 清理动作始终失败：cleanup 结果必须为 failed 且与实验结论可区分（cleanup_failed 终态）。
func TestEngineCleanupFailureDistinguishable(t *testing.T) {
	exec := NewSimExecutor(simScripts(map[string]*Script{
		"recover": {DoFail: true},
	}))
	rep := runOnce(t, testPlan(testTarget("x", false)), exec, NewMemoryStore())
	r := rep.Results[0]
	t.Logf("result phase=%s reason=%s cleanup=%s", r.Phase, r.Failure, r.Cleanup)
	if r.Phase != PhaseCleanupFailed || r.Cleanup != CleanupFailed || r.Failure != ReasonRecoverFailed {
		t.Fatalf("unexpected result: %+v", r)
	}
	// 审计日志必须留下每次清理尝试的痕迹（共 MaxCleanupAttempts 次，无静默跳过）。
	attempts := 0
	for _, ev := range r.Events {
		if ev.Kind == "cleanup_attempted" {
			attempts++
		}
	}
	if attempts != MaxCleanupAttempts {
		t.Fatalf("want %d cleanup attempts, got %d", MaxCleanupAttempts, attempts)
	}
}

// 实验在故障注入后被取消：清理脱离取消上下文仍然执行，原因标记为取消。
func TestEngineCanceledStillCleansUp(t *testing.T) {
	// 注入动作阻塞直到 ctx 取消，模拟实验进行中被取消。
	exec := NewSimExecutor(simScripts(map[string]*Script{
		"inject": {DoDelay: 300 * time.Millisecond},
	}))
	spec := testTarget("x", false)
	spec.Inject.Timeout = 5 * time.Second
	plan := testPlan(spec)
	plan.RunTimeout = 30 * time.Millisecond // 整体超时等价于取消驱动
	eng := NewEngine(exec, NewMemoryStore(), WithLogger(t.Logf))
	rep, err := eng.Run(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	r := rep.Results[0]
	t.Logf("result phase=%s reason=%s cleanup=%s; report timed_out=%v",
		r.Phase, r.Failure, r.Cleanup, rep.TimedOut)
	if r.Cleanup != CleanupSucceeded {
		t.Fatalf("fault must be cleaned after cancellation, cleanup=%s", r.Cleanup)
	}
	if r.Failure != ReasonActionTimeout && r.Failure != ReasonExperimentCanceled {
		t.Fatalf("want timeout/cancel reason, got %s", r.Failure)
	}
}

// 计划级取消（父 ctx 取消）必须映射为 experiment_canceled。
func TestEngineParentCancelReason(t *testing.T) {
	exec := NewSimExecutor(simScripts(map[string]*Script{
		"inject": {DoDelay: 300 * time.Millisecond},
	}))
	spec := testTarget("x", false)
	spec.Inject.Timeout = 5 * time.Second
	plan := testPlan(spec)
	plan.RunTimeout = 0
	eng := NewEngine(exec, NewMemoryStore(), WithLogger(t.Logf))
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	rep, err := eng.Run(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	r := rep.Results[0]
	t.Logf("result reason=%s cleanup=%s canceled=%v", r.Failure, r.Cleanup, rep.Canceled)
	if !rep.Canceled || (r.Failure != ReasonExperimentCanceled && r.Failure != ReasonActionTimeout) {
		t.Fatalf("unexpected: reason=%s canceled=%v", r.Failure, rep.Canceled)
	}
	if r.Cleanup != CleanupSucceeded {
		t.Fatalf("cleanup must run even when canceled, got %s", r.Cleanup)
	}
}

// 非法计划（判定规则引用不存在的探测）必须在运行前拒绝。
func TestEngineRejectsInvalidPlan(t *testing.T) {
	spec := testTarget("x", false)
	spec.Rules = []RuleSpec{{Probe: "ghost", Metric: "m", Operator: "<=", Threshold: 1}}
	eng := NewEngine(NewSimExecutor(nil), NewMemoryStore())
	_, err := eng.Run(context.Background(), testPlan(spec))
	if !errors.Is(err, ErrPlanInvalid) {
		t.Fatalf("want ErrPlanInvalid, got %v", err)
	}
}

// 中断恢复场景 1：所有目标已终态后再次运行，已确认步骤不得重复执行。
func TestResumeSkipsCompletedTargets(t *testing.T) {
	plan := testPlan(testTarget("a", false), testTarget("b", false))
	store := NewMemoryStore()
	exec := NewSimExecutor(simScripts(nil))

	rep1 := runOnce(t, plan, exec, store)
	for _, r := range rep1.Results {
		if r.Phase != PhaseCompleted {
			t.Fatalf("first run should complete, got %+v", r)
		}
	}

	// 第二次运行：复用同一存储。统计第二次运行中实际发生的动作，
	// 期望注入/探测/清理全都不再次执行。
	counting := &countingExecutor{inner: exec}
	eng := NewEngine(counting, store, WithLogger(t.Logf))
	rep2, err := eng.Run(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rep2.Results {
		if r.Phase != PhaseCompleted || r.Cleanup != CleanupSucceeded {
			t.Fatalf("resume must preserve terminal result, got %+v", r)
		}
	}
	t.Logf("second run doCalls=%v probeCalls=%d", counting.doCommands, counting.probes)
	if len(counting.doCommands) != 0 || counting.probes != 0 {
		t.Fatalf("no action may re-execute after completion, do=%v probes=%d", counting.doCommands, counting.probes)
	}
}

// 中断恢复场景 2：进程在“故障已注入、尚未清理”时崩溃。
// 重启后续跑：不得重放实验，只执行清理，实验明确中止并标记 interrupted_by_restart。
func TestResumeAfterCrashWithInjectedFault(t *testing.T) {
	spec := testTarget("a", false)
	plan := testPlan(spec)
	store := NewMemoryStore()
	// 手工写入崩溃现场：注入已确认，阶段停在 injected。
	store.state = &PlanState{
		PlanID:    plan.ID,
		AttemptID: "old-attempt",
		Targets: map[string]*TargetState{
			"a": {
				ID: "a", AttemptID: "old-attempt", Phase: PhaseInjected,
				FaultInjected: true, Cleanup: CleanupNotNeeded,
				Done: []StepDone{{Name: "inject"}},
			},
		},
	}
	counting := &countingExecutor{inner: NewSimExecutor(simScripts(nil))}
	eng := NewEngine(counting, store, WithLogger(t.Logf))
	rep, err := eng.Run(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	r := rep.Results[0]
	t.Logf("resume result phase=%s reason=%s cleanup=%s doCalls=%v",
		r.Phase, r.Failure, r.Cleanup, counting.doCommands)
	if r.Phase != PhaseAborted || r.Failure != ReasonInterrupted {
		t.Fatalf("want aborted/interrupted_by_restart, got phase=%s reason=%s", r.Phase, r.Failure)
	}
	if r.Cleanup != CleanupSucceeded {
		t.Fatalf("injected fault must be cleaned on resume, got %s", r.Cleanup)
	}
	// 只允许发生清理动作：注入不得重放。
	if len(counting.doCommands) != 1 || counting.doCommands[0] != "recover" {
		t.Fatalf("resume must execute only recover, got %v (probes=%d)", counting.doCommands, counting.probes)
	}
	if counting.probes != 0 {
		t.Fatalf("steady probing must not be replayed after crash, probes=%d", counting.probes)
	}

	// 再跑一次：已经终态，仍不得有任何动作。
	counting2 := &countingExecutor{inner: NewSimExecutor(simScripts(nil))}
	eng2 := NewEngine(counting2, store, WithLogger(t.Logf))
	if _, err := eng2.Run(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if len(counting2.doCommands) != 0 || counting2.probes != 0 {
		t.Fatalf("terminal target must stay untouched, do=%v probes=%d", counting2.doCommands, counting2.probes)
	}
}

// 跨“进程重启”的恢复：用 FileStore 持久化崩溃现场，再新建引擎实例续跑。
func TestResumeAcrossRestartWithFileStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.json")
	spec := testTarget("a", false)
	plan := testPlan(spec)

	// 第一次运行：模拟在注入后崩溃 —— 直接写持久化状态后丢弃引擎实例。
	crashState := &PlanState{
		PlanID:    plan.ID,
		AttemptID: "attempt-before-crash",
		Targets: map[string]*TargetState{
			"a": {
				ID: "a", AttemptID: "attempt-before-crash", Phase: PhaseHealing,
				FaultInjected: true, Cleanup: CleanupNotNeeded,
				Done:   []StepDone{{Name: "inject"}, {Name: "heal:0:action"}},
				Events: nil,
			},
		},
	}
	if err := NewFileStore(path).Save(crashState); err != nil {
		t.Fatal(err)
	}

	// “重启”：新建执行器与引擎，从文件加载状态。
	counting := &countingExecutor{inner: NewSimExecutor(simScripts(nil))}
	eng := NewEngine(counting, NewFileStore(path), WithLogger(t.Logf))
	rep, err := eng.Run(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	r := rep.Results[0]
	t.Logf("after restart: phase=%s reason=%s cleanup=%s doCalls=%v probes=%d",
		r.Phase, r.Failure, r.Cleanup, counting.doCommands, counting.probes)
	if r.Phase != PhaseAborted || r.Failure != ReasonInterrupted {
		t.Fatalf("want aborted/interrupted_by_restart, got phase=%s reason=%s", r.Phase, r.Failure)
	}
	if r.Cleanup != CleanupSucceeded || len(counting.doCommands) != 1 || counting.doCommands[0] != "recover" {
		t.Fatalf("only cleanup should run, calls=%v cleanup=%s", counting.doCommands, r.Cleanup)
	}
}

// 依据相互矛盾：两条判据一条满足、一条违反，必须得出 contradictory 并中止，
// 而不是任意挑一条默认通过。
func TestEngineContradictoryEvidence(t *testing.T) {
	spec := testTarget("x", false)
	spec.Probes = []ProbeSpec{{Name: "good", MinSamples: 1}, {Name: "bad", MinSamples: 1}}
	spec.Rules = []RuleSpec{
		{Probe: "good", Metric: "err", Operator: "<=", Threshold: 0.05},
		{Probe: "bad", Metric: "err", Operator: "<=", Threshold: 0.05},
	}
	exec := NewSimExecutor(simScripts(map[string]*Script{
		"good": {Values: map[string]float64{"err": 0.0}},
		"bad":  {Values: map[string]float64{"err": 0.9}},
	}))
	rep := runOnce(t, testPlan(spec), exec, NewMemoryStore())
	r := rep.Results[0]
	t.Logf("contradictory inputs: verdict=%s reason=%s cleanup=%s", r.Verdict, r.Failure, r.Cleanup)
	if r.Verdict != VerdictContradictory || r.Failure != ReasonEvidenceContradictory || r.Cleanup != CleanupSucceeded {
		t.Fatalf("unexpected: %+v", r)
	}
}

// 自愈在恢复时不得重复触发同一动作：构造自愈动作已执行、复核未完成的现场。
func TestResumeInsideHealingDoesNotRefireSameAction(t *testing.T) {
	spec := testTarget("a", true)
	plan := testPlan(spec)
	store := NewMemoryStore()
	store.state = &PlanState{
		PlanID:    plan.ID,
		AttemptID: "old",
		Targets: map[string]*TargetState{
			"a": {
				ID: "a", AttemptID: "old", Phase: PhaseInjected,
				FaultInjected: true, Cleanup: CleanupNotNeeded,
				Done: []StepDone{{Name: "inject"}, {Name: "heal:0:action"}},
			},
		},
	}
	// 注入后中断规则优先：只清理并中止，不会重放自愈动作。
	counting := &countingExecutor{inner: NewSimExecutor(simScripts(nil))}
	eng := NewEngine(counting, store, WithLogger(t.Logf))
	rep, err := eng.Run(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	r := rep.Results[0]
	for _, cmd := range counting.doCommands {
		if cmd == "heal" {
			t.Fatalf("heal action must not be re-fired on resume, calls=%v", counting.doCommands)
		}
	}
	t.Logf("resume in healing: calls=%v phase=%s reason=%s", counting.doCommands, r.Phase, r.Failure)
}

// 持久化状态属于另一个计划时必须拒绝运行，避免状态串用。
func TestEngineRejectsForeignPlanState(t *testing.T) {
	store := NewMemoryStore()
	store.state = &PlanState{
		PlanID:  "other-plan",
		Targets: map[string]*TargetState{},
	}
	eng := NewEngine(NewSimExecutor(nil), store)
	_, err := eng.Run(context.Background(), testPlan(testTarget("a", false)))
	if err == nil {
		t.Fatal("foreign plan state must be rejected")
	}
	t.Logf("foreign state rejected: %v", err)
}
