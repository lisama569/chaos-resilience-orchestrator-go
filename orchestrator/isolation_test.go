package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestStateFileNameEncoding 验证计划 ID -> 状态文件名的编码是单射：
// 任何两个不同的 ID（含斜杠、空格、中文、%、路径片段等易撞名组合）
// 都得到不同文件名，且文件名不含路径分隔符、不超出文件系统长度上限。
func TestStateFileNameEncoding(t *testing.T) {
	ids := []string{
		"a/b", "a b", "a_b", "a%2Fb", "a%252Fb",
		"env/生产", "env 生产", "env_生产",
		"计划-甲", "计划-乙", "中文/空格 混合%25",
		".", "..", "a\\b", "a:b", "normal-id_1.2",
	}
	seen := map[string]string{}
	for _, id := range ids {
		name, err := stateFileName(id)
		if err != nil {
			t.Fatalf("plan id %q unexpectedly rejected: %v", id, err)
		}
		if strings.ContainsRune(name, '/') || strings.ContainsRune(name, os.PathSeparator) {
			t.Fatalf("plan id %q escapes directory: %q", id, name)
		}
		if len(name) > maxStateFileName {
			t.Fatalf("plan id %q encodes beyond limit: %d bytes", id, len(name))
		}
		if prev, dup := seen[name]; dup {
			t.Fatalf("plan ids %q and %q collide on state file %q", prev, id, name)
		}
		seen[name] = id
		t.Logf("plan id %q -> state file %q", id, name)
	}

	// 编码后超出文件名长度上限的 ID 必须被明确拒绝，而不是截断后撞名。
	for _, id := range []string{strings.Repeat("x", 300), strings.Repeat("长", 100)} {
		if _, err := stateFileName(id); err == nil {
			t.Fatalf("overlong plan id (%d bytes) must be rejected", len(id))
		} else {
			t.Logf("overlong plan id (%d bytes) rejected as expected: %v", len(id), err)
		}
	}
}

// trickyPlan 为第 i 个计划构造两个目标：ok 健康直达完成；
// bad 按计划序号交替呈现“不可达 / 注入失败”两种异常链路，
// 使各计划结论互不相同，任何状态串扰都会表现为结果错位。
func trickyPlan(i int, id string) (*Plan, map[string]*Script) {
	key := func(s string) string { return fmt.Sprintf("p%d-%s", i, s) }
	ok := testTarget("ok", false)
	ok.ReachableBy, ok.Inject.Command, ok.Recover.Command = key("reach-ok"), key("inject-ok"), key("recover-ok")
	ok.Probes[0].Name, ok.Rules[0].Probe = key("probe-ok"), key("probe-ok")
	bad := testTarget("bad", false)
	bad.ReachableBy, bad.Inject.Command, bad.Recover.Command = key("reach-bad"), key("inject-bad"), key("recover-bad")
	bad.Probes[0].Name, bad.Rules[0].Probe = key("probe-bad"), key("probe-bad")

	scripts := map[string]*Script{
		key("reach-ok"):   {},
		key("inject-ok"):  {},
		key("recover-ok"): {},
		key("probe-ok"):   {Values: map[string]float64{"err": 0.01}},
		key("reach-bad"):   {},
		key("inject-bad"):  {},
		key("recover-bad"): {},
		key("probe-bad"):   {Values: map[string]float64{"err": 0.01}},
	}
	if i%2 == 0 {
		scripts[key("reach-bad")] = &Script{Unreachable: true}
	} else {
		scripts[key("inject-bad")] = &Script{DoFail: true}
	}
	return &Plan{ID: id, Concurrency: 2, RunTimeout: 30 * time.Second, Targets: []TargetSpec{ok, bad}}, scripts
}

// wantBadResult 返回 trickyPlan 中 bad 目标的预期结果。
func wantBadResult(i int) resultKey {
	if i%2 == 0 {
		return resultKey{Phase: PhaseAborted, Failure: ReasonTargetUnreachable, Cleanup: CleanupNotNeeded}
	}
	return resultKey{Phase: PhaseAborted, Failure: ReasonInjectFailed, Cleanup: CleanupNotNeeded}
}

// TestRunnerTrickyPlanIDsIsolation 易撞名计划 ID（斜杠/空格/中文/% 组合，
// 旧实现会映射到同一文件）并行运行：每个计划必须落到各自独立的状态文件，
// 结论互不污染；全部结束后再发起一次，验证重启续跑只读到自己的状态
// （终态目标零重放，结果逐字段不变）。全程真实落盘。
func TestRunnerTrickyPlanIDsIsolation(t *testing.T) {
	ids := []string{
		"env/生产", "env 生产", "env_生产",
		"a/b", "a b", "a_b", "a%2Fb",
		"计划-甲", "计划-乙",
	}
	dir := t.TempDir()
	scripts := map[string]*Script{}
	plans := make([]*Plan, 0, len(ids))
	for i, id := range ids {
		p, s := trickyPlan(i, id)
		plans = append(plans, p)
		mergeScripts(scripts, s)
	}
	exec := &countingExecutor{inner: NewSimExecutor(scripts)}
	r := NewRunner(exec, WithStateDir(dir), WithEngineOptions(WithLogger(t.Logf)))

	ctx := context.Background()
	for _, p := range plans {
		if err := r.Start(ctx, p); err != nil {
			t.Fatalf("start %q: %v", p.ID, err)
		}
	}
	reps := map[string]*Report{}
	for i, p := range plans {
		rep, err := r.Wait(p.ID)
		if err != nil {
			t.Fatalf("wait %q: %v", p.ID, err)
		}
		reps[p.ID] = rep
		got := resultsByID(rep)
		want := map[string]resultKey{
			"ok":  {Phase: PhaseCompleted, Verdict: VerdictSteady, Cleanup: CleanupSucceeded},
			"bad": wantBadResult(i),
		}
		for id, w := range want {
			if got[id] != w {
				t.Fatalf("plan %q target %s: want %+v, got %+v", p.ID, id, w, got[id])
			}
		}
		t.Logf("plan %q finished: ok=%+v bad=%+v", p.ID, got["ok"], got["bad"])
	}

	// 落盘文件数量与计划数一致，且每个文件只属于一个计划、只含该计划的目标。
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read state dir: %v", err)
	}
	files := map[string]string{} // file name -> plan id
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), stateFileSuffix) {
			data, err := os.ReadFile(filepath.Join(dir, e.Name()))
			if err != nil {
				t.Fatalf("read state file %s: %v", e.Name(), err)
			}
			var ps PlanState
			if err := json.Unmarshal(data, &ps); err != nil {
				t.Fatalf("state file %s corrupt: %v", e.Name(), err)
			}
			files[e.Name()] = ps.PlanID
			if len(ps.Targets) != 2 {
				t.Fatalf("state file %s (plan %q): has %d targets, want 2", e.Name(), ps.PlanID, len(ps.Targets))
			}
		}
	}
	if len(files) != len(plans) {
		t.Fatalf("want %d distinct state files, got %d: %v", len(plans), len(files), files)
	}
	owners := map[string]string{} // plan id -> file name
	for name, pid := range files {
		if other, dup := owners[pid]; dup {
			t.Fatalf("plan %q owns two state files: %s and %s", pid, other, name)
		}
		owners[pid] = name
	}
	for _, id := range ids {
		if _, ok := owners[id]; !ok {
			t.Fatalf("plan %q has no state file on disk", id)
		}
		t.Logf("plan %q owns state file %s", id, owners[id])
	}

	// 重启续跑：每个计划只读到自己的状态，终态目标绝不重放，结果逐字段不变。
	exec.mu.Lock()
	dosBefore := len(exec.doCommands)
	exec.mu.Unlock()
	for i, p := range plans {
		rep, err := r.Run(ctx, p)
		if err != nil {
			t.Fatalf("resume %q: %v", p.ID, err)
		}
		got, want := resultsByID(rep), resultsByID(reps[p.ID])
		for id, w := range want {
			if got[id] != w {
				t.Fatalf("resume changed plan %q target %s: first=%+v second=%+v", p.ID, id, w, got[id])
			}
		}
		_ = i
	}
	exec.mu.Lock()
	dosAfter := len(exec.doCommands)
	exec.mu.Unlock()
	if dosAfter != dosBefore {
		t.Fatalf("resume re-executed actions: before=%d after=%d", dosBefore, dosAfter)
	}
	t.Logf("resume of %d plans re-executed 0 actions; results identical", len(plans))
}

// TestRunnerRejectsUnsafePlanID 无法安全落盘的计划 ID（编码后超出文件名
// 长度上限）必须在发起时被明确拒绝：不创建任何状态文件、不留下运行记录，
// 且 Runner 拒绝后仍可正常发起其他计划。
func TestRunnerRejectsUnsafePlanID(t *testing.T) {
	dir := t.TempDir()
	r := NewRunner(NewSimExecutor(simScripts(nil)), WithStateDir(dir))

	bad := testPlan(testTarget("t1", false))
	bad.ID = strings.Repeat("长", 100) // 编码后 900+ 字节，超出文件名上限
	err := r.Start(context.Background(), bad)
	if !errors.Is(err, ErrPlanIDUnsafe) {
		t.Fatalf("want ErrPlanIDUnsafe, got %v", err)
	}
	t.Logf("overlong plan id rejected at start: %v", err)

	if _, err := r.Progress(bad.ID); !errors.Is(err, ErrPlanNotStarted) {
		t.Fatalf("rejected plan must not be tracked, got %v", err)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), stateFileSuffix) {
			t.Fatalf("rejected plan left state file: %s", e.Name())
		}
	}

	// Runner 未被拒绝操作污染：合法计划照常运行。
	good := testPlan(testTarget("t1", false))
	rep, err := r.Run(context.Background(), good)
	if err != nil {
		t.Fatalf("run valid plan after rejection: %v", err)
	}
	if rep.Results[0].Phase != PhaseCompleted {
		t.Fatalf("valid plan after rejection: %+v", rep.Results[0])
	}

	// 不落盘的 Runner（默认内存存储）不受文件名限制，同一 ID 可以运行。
	mem := NewRunner(NewSimExecutor(simScripts(nil)))
	if err := mem.Start(context.Background(), bad); err != nil {
		t.Fatalf("memory-backed runner must accept any valid plan id, got %v", err)
	}
	if _, err := mem.Wait(bad.ID); err != nil {
		t.Fatalf("memory-backed run: %v", err)
	}
	t.Logf("memory-backed runner accepted the same id (no persistence constraint)")
}

// gatedFileStore 包装真实 FileStore：首次 Save 时阻塞在 release 上，
// 用于复现“状态正在往盘上写”的窗口；释放后全部写真实落盘。
type gatedFileStore struct {
	inner     *FileStore
	enterSave chan struct{}
	release   chan struct{}
	once      sync.Once
}

func (g *gatedFileStore) Load() (*PlanState, error) { return g.inner.Load() }

func (g *gatedFileStore) Save(s *PlanState) error {
	g.once.Do(func() { close(g.enterSave) })
	<-g.release
	return g.inner.Save(s)
}

// TestSnapshotNotBlockedByDiskWrite 状态正在写盘（Save 被卡住）时，
// 进度查询必须立刻返回一致视图，不被写盘 I/O 阻塞；写盘恢复后
// 运行正常结束，盘上状态与最终报告一致（真实落盘路径）。
func TestSnapshotNotBlockedByDiskWrite(t *testing.T) {
	dir := t.TempDir()
	gs := &gatedFileStore{
		inner:     NewFileStore(filepath.Join(dir, "plan.state.json")),
		enterSave: make(chan struct{}),
		release:   make(chan struct{}),
	}
	eng := NewEngine(NewSimExecutor(simScripts(nil)), gs, WithLogger(t.Logf))
	plan := testPlan(testTarget("t1", false))

	done := make(chan *Report, 1)
	go func() {
		rep, _ := eng.Run(context.Background(), plan)
		done <- rep
	}()

	// 等到一次写盘真正被卡住（此时写盘持有持久化锁）。
	select {
	case <-gs.enterSave:
	case <-time.After(5 * time.Second):
		t.Fatal("no state save attempted")
	}

	// 写盘卡住期间，查询必须立刻返回且视图一致。
	type snapRes struct {
		p   PlanProgress
		lat time.Duration
	}
	snapCh := make(chan snapRes, 1)
	go func() {
		start := time.Now()
		snapCh <- snapRes{eng.Snapshot(), time.Since(start)}
	}()
	var snap PlanProgress
	select {
	case r := <-snapCh:
		snap = r.p
		t.Logf("snapshot returned in %s while a state save was blocked", r.lat)
	case <-time.After(2 * time.Second):
		close(gs.release)
		t.Fatal("snapshot blocked behind an in-flight disk write")
	}
	if !snap.Started || !snap.Running {
		t.Fatalf("want started+running during blocked save, got %+v", snap)
	}
	checkSnapshotInvariants(t, snap)

	// 放行写盘，运行正常结束；盘上状态与报告逐字段一致。
	close(gs.release)
	rep := <-done
	if rep.Results[0].Phase != PhaseCompleted {
		t.Fatalf("run after release: %+v", rep.Results[0])
	}
	data, err := os.ReadFile(filepath.Join(dir, "plan.state.json"))
	if err != nil {
		t.Fatalf("state file not persisted: %v", err)
	}
	var ps PlanState
	if err := json.Unmarshal(data, &ps); err != nil {
		t.Fatalf("persisted state corrupt: %v", err)
	}
	st, err := ps.Target("t1")
	if err != nil {
		t.Fatalf("persisted state: %v", err)
	}
	r0 := rep.Results[0]
	if st.Phase != r0.Phase || st.Verdict != r0.Verdict || st.Failure != r0.Failure || st.Cleanup != r0.Cleanup {
		t.Fatalf("persisted state disagrees with report: disk=%+v report=%+v",
			resultKey{st.Phase, st.Verdict, st.Failure, st.Cleanup, st.HealRounds}, keyOf(r0))
	}
	t.Logf("persisted state matches final report: %+v", keyOf(r0))
}
