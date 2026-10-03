package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// namedPlan 构造一个带指定 ID 与目标的计划。
func namedPlan(id string, concurrency int, targets ...TargetSpec) *Plan {
	p := testPlan(targets...)
	p.ID = id
	p.Concurrency = concurrency
	return p
}

// 多计划并行：三个不同行为特征的计划同时跑，状态各自落盘，
// 结论与单独运行时完全一致，互不污染。
func TestSupervisorParallelPlansIsolated(t *testing.T) {
	dir := t.TempDir()
	factory := func(plan *Plan) Executor {
		switch plan.ID {
		case "plan-steady":
			return NewSimExecutor(simScripts(nil))
		case "plan-unreachable":
			return NewSimExecutor(simScripts(map[string]*Script{"reach": {Unreachable: true}}))
		case "plan-heal":
			return NewSimExecutor(simScripts(map[string]*Script{
				"probe":   {Values: map[string]float64{"err": 0.9}},
				"reprobe": {Values: map[string]float64{"err": 0.0}},
			}))
		}
		return NewSimExecutor(simScripts(nil))
	}
	sup := NewSupervisor(dir, factory, WithLogger(t.Logf))

	plans := []*Plan{
		namedPlan("plan-steady", 2, testTarget("s1", false), testTarget("s2", false)),
		namedPlan("plan-unreachable", 1, testTarget("u1", false)),
		namedPlan("plan-heal", 1, testTarget("h1", true)),
	}
	for _, p := range plans {
		if err := sup.Start(context.Background(), p); err != nil {
			t.Fatalf("start %s: %v", p.ID, err)
		}
	}

	// 稳态计划：全部完成。
	rep, err := sup.Wait("plan-steady")
	if err != nil {
		t.Fatalf("wait steady: %v", err)
	}
	for _, r := range rep.Results {
		t.Logf("plan-steady target=%s phase=%s verdict=%s cleanup=%s", r.TargetID, r.Phase, r.Verdict, r.Cleanup)
		if r.Phase != PhaseCompleted || r.Verdict != VerdictSteady || r.Cleanup != CleanupSucceeded {
			t.Fatalf("plan-steady polluted: %+v", r)
		}
	}
	// 不可达计划：明确中止、无需清理，不能变成别的结论。
	rep, err = sup.Wait("plan-unreachable")
	if err != nil {
		t.Fatalf("wait unreachable: %v", err)
	}
	r := rep.Results[0]
	t.Logf("plan-unreachable target=%s phase=%s reason=%s cleanup=%s", r.TargetID, r.Phase, r.Failure, r.Cleanup)
	if r.Phase != PhaseAborted || r.Failure != ReasonTargetUnreachable || r.Cleanup != CleanupNotNeeded {
		t.Fatalf("plan-unreachable polluted: %+v", r)
	}
	// 自愈计划：一轮收敛。
	rep, err = sup.Wait("plan-heal")
	if err != nil {
		t.Fatalf("wait heal: %v", err)
	}
	r = rep.Results[0]
	t.Logf("plan-heal target=%s phase=%s verdict=%s rounds=%d", r.TargetID, r.Phase, r.Verdict, r.HealRounds)
	if r.Phase != PhaseCompleted || r.Verdict != VerdictSteady || r.HealRounds != 1 {
		t.Fatalf("plan-heal polluted: %+v", r)
	}

	// 每个计划都有自己独立的落盘位置，且确实写到了真实文件。
	for _, id := range []string{"plan-steady", "plan-unreachable", "plan-heal"} {
		path := filepath.Join(dir, id, "state.json")
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("plan %s state file missing: %v", id, err)
		}
		p, err := sup.Progress(id)
		if err != nil || p.Status != RunFinished || p.PlanID != id {
			t.Fatalf("plan %s progress: %+v err=%v", id, p, err)
		}
		checkProgressConsistent(t, p)
	}
	if got := sup.Plans(); len(got) != 3 {
		t.Fatalf("plans=%v", got)
	}
	if sup.Running("plan-steady") {
		t.Fatal("plan-steady should be finished")
	}
}

// 同一计划重复发起：进行中必须明确拒绝；结束后允许按续跑规则继续，
// 已终态目标绝不重复执行。
func TestSupervisorDuplicateStartRules(t *testing.T) {
	dir := t.TempDir()
	barrier := newBarrierExecutor(8)
	defer barrier.close()
	counting := &countingExecutor{inner: barrier}
	sup := NewSupervisor(dir, func(*Plan) Executor { return counting })
	plan := namedPlan("dup", 1, testTarget("x", false))

	if err := sup.Start(context.Background(), plan); err != nil {
		t.Fatalf("first start: %v", err)
	}
	<-barrier.starts // 第一个目标进入注入动作，运行进行中

	err := sup.Start(context.Background(), plan)
	if !errors.Is(err, ErrPlanAlreadyRunning) {
		t.Fatalf("in-flight duplicate start must be rejected, got %v", err)
	}
	t.Logf("in-flight duplicate rejected: %v", err)

	barrier.close()
	rep, err := sup.Wait("dup")
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if rep.Results[0].Phase != PhaseCompleted {
		t.Fatalf("first run: %+v", rep.Results[0])
	}
	injectsAfterFirst := len(counting.doCommands)

	// 已结束 -> 允许再次发起，按续跑规则：终态目标不重放。
	if err := sup.Start(context.Background(), plan); err != nil {
		t.Fatalf("restart after finish must be allowed: %v", err)
	}
	rep, err = sup.Wait("dup")
	if err != nil {
		t.Fatalf("wait second: %v", err)
	}
	if rep.Results[0].Phase != PhaseCompleted {
		t.Fatalf("resumed run: %+v", rep.Results[0])
	}
	counting.mu.Lock()
	got := len(counting.doCommands)
	counting.mu.Unlock()
	if got != injectsAfterFirst {
		t.Fatalf("terminal target re-executed: do commands %d -> %d", injectsAfterFirst, got)
	}
	t.Logf("resume reused persisted result, do commands unchanged at %d", got)
}

// 未启动过的计划：查询与等待都返回明确错误，而不是编造空结果。
func TestSupervisorUnknownPlan(t *testing.T) {
	sup := NewSupervisor(t.TempDir(), func(*Plan) Executor { return NewSimExecutor(nil) })
	if _, err := sup.Progress("nope"); !errors.Is(err, ErrUnknownPlan) {
		t.Fatalf("progress: %v", err)
	}
	if _, err := sup.Wait("nope"); !errors.Is(err, ErrUnknownPlan) {
		t.Fatalf("wait: %v", err)
	}
	if sup.Running("nope") {
		t.Fatal("unknown plan must not be running")
	}
}

// 计划 ID 必须能安全映射为独立落盘位置，防止路径串扰。
func TestSupervisorRejectsUnsafePlanID(t *testing.T) {
	sup := NewSupervisor(t.TempDir(), func(*Plan) Executor { return NewSimExecutor(nil) })
	for _, id := range []string{"a/b", "..", `a\b`, "a\x00b"} {
		err := sup.Start(context.Background(), namedPlan(id, 1, testTarget("x", false)))
		if !errors.Is(err, ErrPlanInvalid) {
			t.Fatalf("plan id %q must be rejected, got %v", id, err)
		}
		t.Logf("plan id %q rejected: %v", id, err)
	}
}

// 计划运行中即可通过 Supervisor 查询进度；运行中的计划状态为 running。
func TestSupervisorProgressDuringRun(t *testing.T) {
	dir := t.TempDir()
	barrier := newBarrierExecutor(8)
	defer barrier.close()
	sup := NewSupervisor(dir, func(*Plan) Executor { return barrier })
	plan := namedPlan("live", 1, testTarget("x", false))

	if err := sup.Start(context.Background(), plan); err != nil {
		t.Fatalf("start: %v", err)
	}
	<-barrier.starts
	if !sup.Running("live") {
		t.Fatal("plan should be running")
	}
	p, err := sup.Progress("live")
	if err != nil {
		t.Fatalf("progress: %v", err)
	}
	if p.Status != RunRunning || len(p.Targets) != 1 || p.Targets[0].Phase != PhaseInjecting {
		t.Fatalf("mid-run progress: %+v", p)
	}
	t.Logf("mid-run via supervisor: status=%s phase=%s", p.Status, p.Targets[0].Phase)
	barrier.close()
	if _, err := sup.Wait("live"); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if sup.Running("live") {
		t.Fatal("plan should be finished")
	}
}

// 整体超时只影响对应计划自身，不波及其他并行计划。
func TestSupervisorTimeoutIsolatedPerPlan(t *testing.T) {
	dir := t.TempDir()
	sup := NewSupervisor(dir, func(plan *Plan) Executor {
		if plan.ID == "slow" {
			return NewSimExecutor(simScripts(map[string]*Script{
				"inject": {DoDelay: 300 * time.Millisecond},
			}))
		}
		return NewSimExecutor(simScripts(nil))
	})
	slow := namedPlan("slow", 1, testTarget("x", false))
	slow.RunTimeout = 50 * time.Millisecond
	fast := namedPlan("fast", 1, testTarget("y", false))

	if err := sup.Start(context.Background(), slow); err != nil {
		t.Fatalf("start slow: %v", err)
	}
	if err := sup.Start(context.Background(), fast); err != nil {
		t.Fatalf("start fast: %v", err)
	}
	slowRep, err := sup.Wait("slow")
	if err != nil {
		t.Fatalf("wait slow: %v", err)
	}
	if !slowRep.TimedOut {
		t.Fatalf("slow plan should time out: %+v", slowRep.Results[0])
	}
	t.Logf("slow plan timed out as configured: phase=%s reason=%s cleanup=%s",
		slowRep.Results[0].Phase, slowRep.Results[0].Failure, slowRep.Results[0].Cleanup)
	fastRep, err := sup.Wait("fast")
	if err != nil {
		t.Fatalf("wait fast: %v", err)
	}
	if fastRep.Results[0].Phase != PhaseCompleted || fastRep.TimedOut {
		t.Fatalf("fast plan affected by slow plan timeout: %+v", fastRep.Results[0])
	}
}
