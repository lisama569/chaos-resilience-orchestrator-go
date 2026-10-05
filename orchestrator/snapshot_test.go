package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestSnapshotBeforeRun 验证引擎从未运行时返回零值视图。
func TestSnapshotBeforeRun(t *testing.T) {
	eng := NewEngine(NewSimExecutor(nil), NewMemoryStore())
	p := eng.Snapshot()
	if p.Started || p.Running || len(p.Targets) != 0 {
		t.Fatalf("want empty snapshot, got %+v", p)
	}
}

// TestSnapshotDuringRun 在注入被屏障卡住时查询进度：
// 快照必须立刻返回（不被执行中的动作卡住），且内容自洽。
func TestSnapshotDuringRun(t *testing.T) {
	exec := newBarrierExecutor(64)
	targets := make([]TargetSpec, 0, 3)
	for i := 0; i < 3; i++ {
		spec := testTarget(fmt.Sprintf("t%d", i), false)
		targets = append(targets, spec)
	}
	plan := testPlan(targets...)
	plan.Concurrency = 3
	eng := NewEngine(exec, NewMemoryStore())

	done := make(chan *Report, 1)
	go func() {
		rep, _ := eng.Run(context.Background(), plan)
		done <- rep
	}()

	// 等 3 个注入都进入屏障，此时所有目标应处于 injecting。
	for i := 0; i < 3; i++ {
		<-exec.starts
	}

	// 快照必须及时返回，不能被正在执行的动作卡住。
	type res struct {
		p PlanProgress
	}
	snapCh := make(chan res, 1)
	go func() { snapCh <- res{eng.Snapshot()} }()
	var snap PlanProgress
	select {
	case r := <-snapCh:
		snap = r.p
	case <-time.After(2 * time.Second):
		exec.close()
		t.Fatal("snapshot blocked behind running actions")
	}

	if !snap.Started || !snap.Running {
		t.Fatalf("want started+running, got %+v", snap)
	}
	if len(snap.Targets) != 3 {
		t.Fatalf("want 3 targets, got %d", len(snap.Targets))
	}
	for _, tp := range snap.Targets {
		if tp.Phase != PhaseInjecting && tp.Phase != PhaseInjected {
			t.Fatalf("target %s: unexpected phase %s during barrier", tp.TargetID, tp.Phase)
		}
		if tp.Verdict != "" {
			t.Fatalf("target %s: verdict set before probing: %s", tp.TargetID, tp.Verdict)
		}
		if tp.Cleanup != CleanupNotNeeded {
			t.Fatalf("target %s: cleanup=%s before any cleanup", tp.TargetID, tp.Cleanup)
		}
	}

	exec.close()
	rep := <-done

	// 运行结束后：快照与最终报告必须一致。
	final := eng.Snapshot()
	if final.Running {
		t.Fatal("snapshot still running after run finished")
	}
	for i, tp := range final.Targets {
		r := rep.Results[i]
		if tp.TargetID != r.TargetID || tp.Phase != r.Phase || tp.Verdict != r.Verdict ||
			tp.Cleanup != r.Cleanup || tp.Failure != r.Failure {
			t.Fatalf("snapshot/report mismatch for %s: snap=%+v report=%+v", tp.TargetID, tp, r)
		}
		if tp.Phase != PhaseCompleted || tp.Verdict != VerdictSteady || tp.Cleanup != CleanupSucceeded {
			t.Fatalf("target %s: unexpected final %+v", tp.TargetID, tp)
		}
	}
}

// TestSnapshotConsistentUnderChurn 在高频状态变更下持续查询，
// 每次快照都必须满足一致性不变量，且跨快照阶段单调不回退。
func TestSnapshotConsistentUnderChurn(t *testing.T) {
	const n = 8
	exec := NewSimExecutor(simScripts(map[string]*Script{
		// 探测先违例后恢复，走完整自愈链路，制造密集状态迁移。
		"probe":   {Values: map[string]float64{"err": 0.5}},
		"reprobe": {Values: map[string]float64{"err": 0.0}},
	}))
	var targets []TargetSpec
	for i := 0; i < n; i++ {
		targets = append(targets, testTarget(fmt.Sprintf("t%d", i), true))
	}
	plan := testPlan(targets...)
	plan.Concurrency = n
	eng := NewEngine(exec, NewMemoryStore())

	done := make(chan struct{})
	go func() {
		_, _ = eng.Run(context.Background(), plan)
		close(done)
	}()

	// 运行期间持续查询，校验每个快照内部自洽、跨快照不回退。
	lastPhase := map[string]int{}
	var checks int
	for {
		select {
		case <-done:
			goto finished
		default:
		}
		p := eng.Snapshot()
		checkSnapshotInvariants(t, p)
		for _, tp := range p.Targets {
			if prev, ok := lastPhase[tp.TargetID]; ok && phaseOrder[tp.Phase] < prev {
				t.Fatalf("target %s: phase regressed %d -> %d", tp.TargetID, prev, phaseOrder[tp.Phase])
			}
			lastPhase[tp.TargetID] = phaseOrder[tp.Phase]
		}
		checks++
	}
finished:
	if checks == 0 {
		t.Fatal("no snapshot was taken during run")
	}
	t.Logf("took %d consistent snapshots during run", checks)
}

// checkSnapshotInvariants 校验单个快照的内部一致性。
func checkSnapshotInvariants(t *testing.T, p PlanProgress) {
	t.Helper()
	for _, tp := range p.Targets {
		// 事件序号在目标内严格递增（无缺失、无乱序）。
		for i := 1; i < len(tp.Events); i++ {
			if tp.Events[i].Seq <= tp.Events[i-1].Seq {
				t.Fatalf("target %s: event seq not increasing: %d then %d",
					tp.TargetID, tp.Events[i-1].Seq, tp.Events[i].Seq)
			}
		}
		// 稳态结论只可能在进入探测阶段之后出现。
		if tp.Verdict != "" && phaseOrder[tp.Phase] < phaseOrder[PhaseProbing] {
			t.Fatalf("target %s: verdict=%s but phase=%s", tp.TargetID, tp.Verdict, tp.Phase)
		}
		// 终态目标必须给出明确的清理结果与（失败时）失败原因。
		if tp.Phase.IsTerminal() {
			switch tp.Cleanup {
			case CleanupSucceeded, CleanupFailed, CleanupNotNeeded:
			default:
				t.Fatalf("target %s: terminal phase %s with cleanup=%q", tp.TargetID, tp.Phase, tp.Cleanup)
			}
			if tp.Phase == PhaseAborted && tp.Failure == ReasonNone {
				t.Fatalf("target %s: aborted without failure reason", tp.TargetID)
			}
		}
		// 自愈轮次只可能在进入自愈阶段后非零。
		if tp.HealRounds > 0 && phaseOrder[tp.Phase] < phaseOrder[PhaseHealing] {
			t.Fatalf("target %s: heal rounds=%d but phase=%s", tp.TargetID, tp.HealRounds, tp.Phase)
		}
	}
}

// TestSnapshotIsolation 验证调用方修改快照不会影响引擎状态。
func TestSnapshotIsolation(t *testing.T) {
	exec := NewSimExecutor(simScripts(nil))
	eng := NewEngine(exec, NewMemoryStore())
	plan := testPlan(testTarget("t1", false))
	if _, err := eng.Run(context.Background(), plan); err != nil {
		t.Fatalf("run: %v", err)
	}

	p1 := eng.Snapshot()
	if len(p1.Targets) != 1 || len(p1.Targets[0].Events) == 0 {
		t.Fatalf("unexpected snapshot: %+v", p1)
	}
	// 恶意修改快照：改字段、改事件、追加事件、改事件详情。
	evCount := len(p1.Targets[0].Events)
	p1.Targets[0].Phase = PhaseAborted
	p1.Targets[0].Events[0].Kind = "tampered"
	p1.Targets[0].Events[0].Detail["ok"] = "tampered"
	p1.Targets[0].Events = append(p1.Targets[0].Events, Event{Seq: 9999, Kind: "fake"})

	p2 := eng.Snapshot()
	tp := p2.Targets[0]
	if tp.Phase != PhaseCompleted {
		t.Fatalf("engine state polluted: phase=%s", tp.Phase)
	}
	if len(tp.Events) != evCount {
		t.Fatalf("engine events polluted: want %d, got %d", evCount, len(tp.Events))
	}
	if tp.Events[0].Kind == "tampered" {
		t.Fatal("engine event kind polluted by snapshot mutation")
	}
	if v, ok := tp.Events[0].Detail["ok"]; ok && v == "tampered" {
		t.Fatal("engine event detail polluted by snapshot mutation")
	}
}

// TestSnapshotNotBlockedBySlowDisk 在每次状态落盘都显著变慢（真实文件
// 存储 + 人为延迟，非内存桩）的条件下验证：
//   - 写盘进行中 Snapshot 仍必须即时返回一致视图（不等写盘）；
//   - 同一引擎的落盘严格串行（不允许两个 Save 重叠写出交错内容）；
//   - 高频查询不改变最终结果。
func TestSnapshotNotBlockedBySlowDisk(t *testing.T) {
	store := newSlowFileStore(filepath.Join(t.TempDir(), "slow.state.json"), 40*time.Millisecond)

	var targets []TargetSpec
	for i := 0; i < 4; i++ {
		targets = append(targets, testTarget(fmt.Sprintf("t%d", i), true)) // 走自愈链路，事件密集
	}
	plan := testPlan(targets...)
	plan.Concurrency = 4
	eng := NewEngine(NewSimExecutor(simScripts(nil)), store, WithLogger(t.Logf))

	done := make(chan struct{})
	go func() {
		_, _ = eng.Run(context.Background(), plan)
		close(done)
	}()

	var queries int
	var maxSnapDuration time.Duration
waitLoop:
	for {
		select {
		case <-done:
			break waitLoop
		default:
		}
		start := time.Now()
		p := eng.Snapshot()
		d := time.Since(start)
		if d > maxSnapDuration {
			maxSnapDuration = d
		}
		// 落盘耗时 40ms；快照是锁内纯内存拷贝，给宽限也必须远小于一次写盘。
		if d > 20*time.Millisecond {
			t.Fatalf("snapshot blocked behind disk write: took %v", d)
		}
		checkSnapshotInvariants(t, p)
		queries++
	}

	if queries == 0 {
		t.Fatal("no snapshot executed during slow-disk run")
	}
	saves, maxActive := store.stats()
	if maxActive > 1 {
		t.Fatalf("persistence not serialized: observed %d concurrent saves", maxActive)
	}
	t.Logf("slow-disk run: saves=%d (max concurrent=%d), snapshots=%d, worst snapshot latency=%v",
		saves, maxActive, queries, maxSnapDuration)
	if maxSnapDuration >= 20*time.Millisecond {
		t.Fatalf("query latency should stay well below one disk write, worst=%v", maxSnapDuration)
	}

	// 磁盘上的最终状态文件必须可解析且与最终快照一致（没有写一半的内容）。
	data, err := os.ReadFile(store.inner.(*FileStore).path)
	if err != nil {
		t.Fatalf("read final state file: %v", err)
	}
	var ps PlanState
	if err := json.Unmarshal(data, &ps); err != nil {
		t.Fatalf("final state file corrupt: %v", err)
	}
	final := eng.Snapshot()
	if len(ps.Targets) != len(final.Targets) {
		t.Fatalf("disk targets %d != snapshot targets %d", len(ps.Targets), len(final.Targets))
	}
	for _, tp := range final.Targets {
		st := ps.Targets[tp.TargetID]
		if st == nil || st.Phase != tp.Phase || st.Verdict != tp.Verdict ||
			st.Cleanup != tp.Cleanup || st.Failure != tp.Failure ||
			len(st.Events) != len(tp.Events) {
			t.Fatalf("disk/snapshot mismatch for %s: disk=%+v snap=%+v", tp.TargetID, st, tp)
		}
	}
}
