package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
)

// 用新的引擎实例模拟"进程重启"（共享同一份文件状态，但内存中的运行态全部丢失）。
func (h *testHarness) restartEngine() *Engine {
	return New(h.st, h.rt, &stdLoggerAdapter{l: h.logf})
}

// 优雅中断：注入动作执行期间取消，状态落 paused、注入步骤保持 running；
// 重启后续跑：running 步骤被重置为 pending 重新执行，已完成步骤不重复，最终 steady 且故障被清理。
func TestInterruptThenResume(t *testing.T) {
	h := newHarness(t)
	spec := model.ExperimentSpec{
		ID: "interrupted", Concurrency: 1, ObserveRounds: 1,
		ResumePolicy: model.ResumeResume,
		Targets: []model.TargetSpec{{
			ID: "i",
			// 注入睡眠 300ms，给中断留出窗口；成功后置故障标记。
			Inject:  act("sleep", time.Second, map[string]string{"duration": "300ms", "flag": "i/x"}),
			Cleanup: act("set-flag", time.Second, map[string]string{"key": "i/x", "value": "false"}),
			Probes:  []model.ProbeSpec{pr("p", "flag", "i/x", true, 1)},
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() {
		_, runErr = h.eng.Run(ctx, spec)
		close(done)
	}()

	time.Sleep(80 * time.Millisecond)
	cancel() // 发送 SIGINT 等价的取消信号
	<-done
	t.Logf("first run returned: %v", runErr)

	st := h.load(t, "interrupted")
	if st.Status != model.StatusPaused || st.Reason != model.ReasonCanceled {
		t.Fatalf("after cancel: status=%s reason=%s", st.Status, st.Reason)
	}
	// 注入步骤必须保持 running（结果未知），不得被当作已确认失败
	inject := model.FindStep(st.Steps, model.StepKey{Target: "i", Phase: model.PhaseInject})
	if inject.Status != model.StepRunning {
		t.Fatalf("inject step=%s, want running (unknown outcome)", inject.Status)
	}
	if h.rt.Flag("i/x") {
		t.Fatal("fault flag must not be half-applied after interrupted inject")
	}

	// 模拟进程重启：新引擎实例执行恢复
	eng2 := h.restartEngine()
	restarted, err := eng2.Resume(context.Background(), "interrupted")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if restarted.Status != model.StatusSucceeded || restarted.Conclusion != model.ConclusionSteady {
		t.Fatalf("after resume: status=%s conclusion=%s reason=%s",
			restarted.Status, restarted.Conclusion, restarted.Reason)
	}
	// 注入只执行了一次（中断那次未确认），重跑次数通过最终 flag 已被清理来佐证无残留
	if h.rt.Flag("i/x") {
		t.Fatal("resumed experiment must clean up fault")
	}
}

// 已终态实验 resume 必须返回 none：已确认完成的步骤不得重复执行。
func TestResumeTerminalIsNoop(t *testing.T) {
	h := newHarness(t)
	spec := model.ExperimentSpec{
		ID: "terminal", Concurrency: 1, ObserveRounds: 1, ResumePolicy: model.ResumeResume,
		Targets: []model.TargetSpec{{
			ID: "a", Inject: act("set-flag", time.Second, map[string]string{"key": "a/x", "value": "true"}),
			Cleanup: act("set-flag", time.Second, map[string]string{"key": "a/x", "value": "false"}),
			Probes:  []model.ProbeSpec{pr("p", "flag", "a/x", true, 1)},
		}},
	}
	st := h.run(t, spec)
	v1 := st.Version

	eng2 := h.restartEngine()
	out, err := eng2.Resume(context.Background(), "terminal")
	if err != nil {
		t.Fatal(err)
	}
	if out.Version != v1 {
		t.Fatalf("terminal resume must not write; version %d -> %d", v1, out.Version)
	}
}

// resume_policy=abort：paused 实验重启后必须清理已注入故障并终止为 aborted。
func TestResumeAbortPolicy(t *testing.T) {
	h := newHarness(t)
	spec := model.ExperimentSpec{
		ID: "abort-me", Concurrency: 1, ObserveRounds: 1,
		ResumePolicy: model.ResumeAbort,
		Targets: []model.TargetSpec{{
			ID: "a", Inject: act("set-flag", time.Second, map[string]string{"key": "a/x", "value": "true"}),
			Cleanup: act("set-flag", time.Second, map[string]string{"key": "a/x", "value": "false"}),
			Probes:  []model.ProbeSpec{pr("p", "flag", "a/x", true, 1)},
		}},
	}

	// 构造精确的中断现场：文档已创建，注入步骤 succeeded（故障确实已生效），
	// 实验整体 paused；环境中的故障标记保持生效。
	now := h.rt.Now()
	st := &model.ExperimentState{
		ID: "abort-me", Spec: spec, Status: model.StatusPaused,
		Conclusion: model.ConclusionInconclusive, Reason: model.ReasonCanceled,
		Steps:   initSteps(spec),
		Results: map[model.TargetID]*model.TargetResult{},
		Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := h.st.Create(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	h.rt.SetFlag("a/x", true) // 注入已生效的故障
	inject := model.FindStep(st.Steps, model.StepKey{Target: "a", Phase: model.PhaseInject})
	inject.Status = model.StepSucceeded
	st.Version = 2
	st.UpdatedAt = now
	if _, err := h.st.Commit(context.Background(), st, 1); err != nil {
		t.Fatal(err)
	}

	eng2 := h.restartEngine()
	out, rerr := eng2.Resume(context.Background(), "abort-me")
	if rerr != nil {
		t.Fatal(rerr)
	}
	if out.Status != model.StatusAborted {
		t.Fatalf("status=%s want aborted", out.Status)
	}
	if h.rt.Flag("a/x") {
		t.Fatal("abort path must still clean up injected fault")
	}
	if out.Results["a"] == nil || !out.Results["a"].CleanedUp {
		t.Fatal("abort result must record successful cleanup")
	}
}

// 重复上报防护：对已终态步骤再次 finishStep 必须收到 ErrDuplicateReport。
func TestDuplicateStepReportRejected(t *testing.T) {
	h := newHarness(t)
	spec := model.ExperimentSpec{
		ID: "dup", Concurrency: 1, ObserveRounds: 1, ResumePolicy: model.ResumeResume,
		Targets: []model.TargetSpec{{
			ID: "a", Inject: act("set-flag", time.Second, map[string]string{"key": "a/x", "value": "true"}),
			Cleanup: act("set-flag", time.Second, map[string]string{"key": "a/x", "value": "false"}),
			Probes:  []model.ProbeSpec{pr("p", "flag", "a/x", true, 1)},
		}},
	}
	h.eng.experimentID = "dup"
	_ = h.run(t, spec)

	key := model.StepKey{Target: "a", Phase: model.PhaseInject}
	_, err := h.eng.finishStep(context.Background(), key, model.StepSucceeded, model.ReasonNone, "duplicate")
	if !errors.Is(err, model.ErrDuplicateReport) {
		t.Fatalf("duplicate report must be rejected with ErrDuplicateReport, got %v", err)
	}
}

// 崩溃恢复：进程被杀时文档停留在 status=running，注入 succeeded、观测 running、
// cleanup pending。重启后 decision=continue：running 步骤重置为 pending，
// 已完成的注入不重复执行，实验最终 steady 且故障清理完成。
func TestCrashRecoveryResetsRunningSteps(t *testing.T) {
	h := newHarness(t)
	spec := model.ExperimentSpec{
		ID: "crashed", Concurrency: 1, ObserveRounds: 2,
		ResumePolicy: model.ResumeResume,
		Targets: []model.TargetSpec{{
			ID:      "z",
			Inject:  act("set-flag", time.Second, map[string]string{"key": "z/x", "value": "true"}),
			Cleanup: act("set-flag", time.Second, map[string]string{"key": "z/x", "value": "false"}),
			Probes:  []model.ProbeSpec{pr("p", "flag", "z/x", true, 2)},
		}},
	}

	now := h.rt.Now()
	st := &model.ExperimentState{
		ID: "crashed", Spec: spec, Status: model.StatusRunning,
		Steps:   initSteps(spec),
		Results: map[model.TargetID]*model.TargetResult{},
		Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := h.st.Create(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	// 注入已确认成功（故障确实生效），观测进行到一半崩溃（步骤停在 running），
	// 清理尚未开始。部分证据已落盘，但观测步骤未确认 => 重置后整次观测重采。
	inject := model.FindStep(st.Steps, model.StepKey{Target: "z", Phase: model.PhaseInject})
	inject.Status = model.StepSucceeded
	obs := model.FindStep(st.Steps, model.StepKey{Target: "z", Phase: model.PhaseObserve, Attempt: 0})
	obs.Status = model.StepRunning // 崩溃在观测采集中：证据不可信，重置后重采
	st.Results["z"] = &model.TargetResult{
		ID: "z",
		// 部分证据已落盘；重采后按 (probe,attempt,round) 去重，判定仍需两轮都满足。
		Evidence: []model.Evidence{{Probe: "p", Attempt: 0, Round: 1, Value: true, At: now}},
	}
	h.rt.SetFlag("z/x", true) // 已注入的故障
	st.Version = 2
	if _, err := h.st.Commit(context.Background(), st, 1); err != nil {
		t.Fatal(err)
	}

	eng2 := h.restartEngine()
	out, err := eng2.Resume(context.Background(), "crashed")
	if err != nil {
		t.Fatalf("resume after crash: %v", err)
	}
	if out.Status != model.StatusSucceeded || out.Conclusion != model.ConclusionSteady {
		t.Fatalf("after crash resume: status=%s conclusion=%s reason=%s",
			out.Status, out.Conclusion, out.Reason)
	}
	// running 的观测轮次被重置重采；succeeded 的注入未被重复执行
	if model.FindStep(out.Steps, model.StepKey{Target: "z", Phase: model.PhaseInject}).Status != model.StepSucceeded {
		t.Fatal("confirmed inject must remain succeeded, not re-run")
	}
	if !out.Results["z"].CleanedUp || h.rt.Flag("z/x") {
		t.Fatal("injected fault must be cleaned after recovery")
	}
}

// 崩溃恢复 + abort 策略：注入已确认、状态 running 遗留，重启后只清理不续跑。
func TestCrashRecoveryAbortCleansInjected(t *testing.T) {
	h := newHarness(t)
	spec := model.ExperimentSpec{
		ID: "crash-abort", Concurrency: 1, ObserveRounds: 1,
		ResumePolicy: model.ResumeAbort,
		Targets: []model.TargetSpec{{
			ID:      "z",
			Inject:  act("set-flag", time.Second, map[string]string{"key": "z/y", "value": "true"}),
			Cleanup: act("set-flag", time.Second, map[string]string{"key": "z/y", "value": "false"}),
			Probes:  []model.ProbeSpec{pr("p", "flag", "z/y", true, 1)},
		}},
	}
	now := h.rt.Now()
	st := &model.ExperimentState{
		ID: "crash-abort", Spec: spec, Status: model.StatusRunning,
		Steps: initSteps(spec), Results: map[model.TargetID]*model.TargetResult{},
		Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := h.st.Create(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	inject := model.FindStep(st.Steps, model.StepKey{Target: "z", Phase: model.PhaseInject})
	inject.Status = model.StepRunning // 崩溃在注入执行中：结果未知
	h.rt.SetFlag("z/y", false)        // 标记动作未生效
	st.Version = 2
	if _, err := h.st.Commit(context.Background(), st, 1); err != nil {
		t.Fatal(err)
	}

	eng2 := h.restartEngine()
	out, err := eng2.Resume(context.Background(), "crash-abort")
	if err != nil {
		t.Fatal(err)
	}
	// 注入结果未知 => 不执行清理（没有确认生效的故障），实验 aborted
	if out.Status != model.StatusAborted {
		t.Fatalf("status=%s want aborted", out.Status)
	}
}
