package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// keyedTarget 构造使用独立脚本键的目标，便于在同一执行器内为不同目标编排不同行为。
func keyedTarget(id string, heal bool) TargetSpec {
	t := testTarget(id, heal)
	t.ReachableBy = "reach-" + id
	t.Inject.Command = "inject-" + id
	t.Recover.Command = "recover-" + id
	t.Probes[0].Name = "probe-" + id
	t.Rules[0].Probe = "probe-" + id
	if heal {
		t.Heal.RecoverAction.Command = "heal-" + id
		t.Heal.RecheckProbes[0].Name = "reprobe-" + id
		t.Heal.RecheckRules[0].Probe = "reprobe-" + id
	}
	return t
}

// keyedScripts 为目标生成默认健康脚本，可用 extra 按场景覆盖。
func keyedScripts(t TargetSpec, extra map[string]*Script) map[string]*Script {
	m := map[string]*Script{
		t.ReachableBy:     {},
		t.Inject.Command:  {},
		t.Recover.Command: {},
		t.Probes[0].Name:  {Values: map[string]float64{"err": 0.01}},
	}
	if t.Heal != nil {
		m[t.Heal.RecoverAction.Command] = &Script{}
		m[t.Heal.RecheckProbes[0].Name] = &Script{Values: map[string]float64{"err": 0.0}}
	}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func mergeScripts(dst, src map[string]*Script) {
	for k, v := range src {
		dst[k] = v
	}
}

// mixedPlan 构造 16 个目标、行为各异的计划：
// t00-t11 健康直达完成；t12-t13 违例后自愈收敛；t14 不可达；t15 注入失败。
func mixedPlan(id string, concurrency int) (*Plan, map[string]*Script) {
	scripts := map[string]*Script{}
	var targets []TargetSpec
	for i := 0; i < 16; i++ {
		tid := fmt.Sprintf("t%02d", i)
		switch {
		case i <= 11:
			ts := keyedTarget(tid, false)
			targets = append(targets, ts)
			mergeScripts(scripts, keyedScripts(ts, nil))
		case i <= 13:
			ts := keyedTarget(tid, true)
			targets = append(targets, ts)
			mergeScripts(scripts, keyedScripts(ts, map[string]*Script{
				"probe-" + tid: {Values: map[string]float64{"err": 0.5}}, // 违例 -> 自愈 -> 收敛
			}))
		case i == 14:
			ts := keyedTarget(tid, false)
			targets = append(targets, ts)
			mergeScripts(scripts, keyedScripts(ts, map[string]*Script{
				"reach-" + tid: {Unreachable: true},
			}))
		default:
			ts := keyedTarget(tid, false)
			targets = append(targets, ts)
			mergeScripts(scripts, keyedScripts(ts, map[string]*Script{
				"inject-" + tid: {DoFail: true},
			}))
		}
	}
	return &Plan{ID: id, Concurrency: concurrency, RunTimeout: 30 * time.Second, Targets: targets}, scripts
}

// resultKey 提取用于跨运行对比的结果字段（忽略时间戳与事件）。
type resultKey struct {
	Phase      Phase
	Verdict    Verdict
	Failure    FailureReason
	Cleanup    CleanupResult
	HealRounds int
}

func keyOf(r TargetResult) resultKey {
	return resultKey{r.Phase, r.Verdict, r.Failure, r.Cleanup, r.HealRounds}
}

func resultsByID(rep *Report) map[string]resultKey {
	m := map[string]resultKey{}
	for _, r := range rep.Results {
		m[r.TargetID] = keyOf(r)
	}
	return m
}

// eventKinds 提取目标事件种类序列，用于严格对比两次运行的过程一致性。
func eventKinds(rep *Report) map[string][]string {
	m := map[string][]string{}
	for _, r := range rep.Results {
		for _, ev := range r.Events {
			m[r.TargetID] = append(m[r.TargetID], ev.Kind)
		}
	}
	return m
}

// TestRunnerRejectsDuplicateStart 运行中重复发起同一计划必须被明确拒绝。
func TestRunnerRejectsDuplicateStart(t *testing.T) {
	exec := newBarrierExecutor(64)
	r := NewRunner(exec)
	plan := testPlan(testTarget("t1", false))

	if err := r.Start(context.Background(), plan); err != nil {
		t.Fatalf("first start: %v", err)
	}
	<-exec.starts // 确保运行确实在进行中

	err := r.Start(context.Background(), plan)
	if !errors.Is(err, ErrPlanAlreadyRunning) {
		t.Fatalf("want ErrPlanAlreadyRunning, got %v", err)
	}

	exec.close()
	rep, err := r.Wait(plan.ID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if rep.Results[0].Phase != PhaseCompleted {
		t.Fatalf("unexpected result: %+v", rep.Results[0])
	}
}

// TestRunnerResumeAfterCompletion 运行结束后再发起：按续跑规则继续，
// 已终态目标绝不重复执行任何动作。
func TestRunnerResumeAfterCompletion(t *testing.T) {
	dir := t.TempDir()
	exec := &countingExecutor{inner: NewSimExecutor(simScripts(nil))}
	r := NewRunner(exec, WithStateDir(dir))
	plan := testPlan(testTarget("t1", false), testTarget("t2", false))

	rep1, err := r.Run(context.Background(), plan)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	for _, res := range rep1.Results {
		if res.Phase != PhaseCompleted {
			t.Fatalf("first run: %+v", res)
		}
	}
	injectsAfterFirst := len(exec.doCommands)

	// 结束后允许再次发起；此次应全部走“终态跳过”，不再执行任何动作。
	rep2, err := r.Run(context.Background(), plan)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if len(exec.doCommands) != injectsAfterFirst {
		t.Fatalf("terminal targets re-executed actions: before=%d after=%d",
			injectsAfterFirst, len(exec.doCommands))
	}
	for i, res := range rep2.Results {
		if keyOf(res) != keyOf(rep1.Results[i]) {
			t.Fatalf("resume changed result: first=%+v second=%+v", keyOf(rep1.Results[i]), keyOf(res))
		}
	}
}

// TestRunnerProgressUnknownPlan 查询未发起的计划必须明确报错。
func TestRunnerProgressUnknownPlan(t *testing.T) {
	r := NewRunner(NewSimExecutor(nil))
	if _, err := r.Progress("nope"); !errors.Is(err, ErrPlanNotStarted) {
		t.Fatalf("want ErrPlanNotStarted, got %v", err)
	}
	if _, err := r.Wait("nope"); !errors.Is(err, ErrPlanNotStarted) {
		t.Fatalf("want ErrPlanNotStarted, got %v", err)
	}
}

// TestRunnerParallelPlansIsolation 三个行为不同的计划并行运行，
// 状态文件各自独立，结论互不污染。
func TestRunnerParallelPlansIsolation(t *testing.T) {
	dir := t.TempDir()

	planA, scriptsA := mixedPlan("plan-a", 16)
	planB, scriptsB := mixedPlan("plan-b", 4)
	planB.Targets = planB.Targets[:4]
	planC, scriptsC := mixedPlan("plan-c", 8)
	planC.Targets = planC.Targets[12:] // 自愈/不可达/注入失败组合

	scripts := map[string]*Script{}
	mergeScripts(scripts, scriptsA)
	mergeScripts(scripts, scriptsB)
	mergeScripts(scripts, scriptsC)

	r := NewRunner(NewSimExecutor(scripts), WithStateDir(dir))
	ctx := context.Background()
	for _, p := range []*Plan{planA, planB, planC} {
		if err := r.Start(ctx, p); err != nil {
			t.Fatalf("start %s: %v", p.ID, err)
		}
	}
	reps := map[string]*Report{}
	for _, p := range []*Plan{planA, planB, planC} {
		rep, err := r.Wait(p.ID)
		if err != nil {
			t.Fatalf("wait %s: %v", p.ID, err)
		}
		reps[p.ID] = rep
	}

	// 每个计划的状态文件必须独立存在且只含自己的目标。
	for _, p := range []*Plan{planA, planB, planC} {
		name, err := stateFileName(p.ID)
		if err != nil {
			t.Fatalf("state file name for %s: %v", p.ID, err)
		}
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("state file for %s: %v", p.ID, err)
		}
		var ps PlanState
		if err := json.Unmarshal(data, &ps); err != nil {
			t.Fatalf("state file for %s corrupt: %v", p.ID, err)
		}
		if ps.PlanID != p.ID {
			t.Fatalf("state file %s belongs to %s", path, ps.PlanID)
		}
		if len(ps.Targets) != len(p.Targets) {
			t.Fatalf("plan %s: state has %d targets, want %d", p.ID, len(ps.Targets), len(p.Targets))
		}
		for _, ts := range p.Targets {
			if _, ok := ps.Targets[ts.ID]; !ok {
				t.Fatalf("plan %s: target %s missing from its state file", p.ID, ts.ID)
			}
		}
	}

	// 结论抽查：plan-c 必须包含三种不同的异常链路，且互不错位。
	got := resultsByID(reps["plan-c"])
	want := map[string]resultKey{
		"t12": {Phase: PhaseCompleted, Verdict: VerdictSteady, Cleanup: CleanupSucceeded, HealRounds: 1},
		"t13": {Phase: PhaseCompleted, Verdict: VerdictSteady, Cleanup: CleanupSucceeded, HealRounds: 1},
		"t14": {Phase: PhaseAborted, Failure: ReasonTargetUnreachable, Cleanup: CleanupNotNeeded},
		"t15": {Phase: PhaseAborted, Failure: ReasonInjectFailed, Cleanup: CleanupNotNeeded},
	}
	for id, w := range want {
		if got[id] != w {
			t.Fatalf("plan-c target %s: want %+v, got %+v", id, w, got[id])
		}
	}
}

// TestRunnerHighConcurrencyWithQueries CI 级场景：
// 3 个计划并行（其中一个 16 目标、并发上限拉满），运行期间多 goroutine
// 持续查询进度；全部结束后，每个目标的结果与单独顺序运行完全一致。
// 状态全部真实落盘（t.TempDir 下的 FileStore）。
func TestRunnerHighConcurrencyWithQueries(t *testing.T) {
	planA, scriptsA := mixedPlan("ci-a", 16)
	planB, scriptsB := mixedPlan("ci-b", 4)
	planB.Targets = planB.Targets[:6]
	planC, scriptsC := mixedPlan("ci-c", 8)
	planC.Targets = planC.Targets[10:]
	plans := []*Plan{planA, planB, planC}

	scripts := map[string]*Script{}
	mergeScripts(scripts, scriptsA)
	mergeScripts(scripts, scriptsB)
	mergeScripts(scripts, scriptsC)

	// 参考值：每个计划单独顺序运行（各自独立落盘目录）。
	ref := map[string]*Report{}
	refKinds := map[string]map[string][]string{}
	for i, p := range plans {
		r := NewRunner(NewSimExecutor(scripts), WithStateDir(filepath.Join(t.TempDir(), fmt.Sprintf("ref-%d", i))))
		rep, err := r.Run(context.Background(), p)
		if err != nil {
			t.Fatalf("reference run %s: %v", p.ID, err)
		}
		ref[p.ID] = rep
		refKinds[p.ID] = eventKinds(rep)
	}

	// 并行运行 + 持续查询。
	dir := t.TempDir()
	r := NewRunner(NewSimExecutor(scripts), WithStateDir(dir))
	ctx := context.Background()
	for _, p := range plans {
		if err := r.Start(ctx, p); err != nil {
			t.Fatalf("start %s: %v", p.ID, err)
		}
	}

	var queryWG sync.WaitGroup
	stopQuery := make(chan struct{})
	var queryCount int64
	var mu sync.Mutex
	for _, p := range plans {
		for q := 0; q < 2; q++ { // 每个计划两个并发查询者
			queryWG.Add(1)
			go func(planID string) {
				defer queryWG.Done()
				for {
					select {
					case <-stopQuery:
						return
					default:
					}
					prog, err := r.Progress(planID)
					if err != nil {
						t.Errorf("progress %s: %v", planID, err)
						return
					}
					checkSnapshotInvariants(t, prog)
					mu.Lock()
					queryCount++
					mu.Unlock()
				}
			}(p.ID)
		}
	}

	reps := map[string]*Report{}
	for _, p := range plans {
		rep, err := r.Wait(p.ID)
		if err != nil {
			t.Fatalf("wait %s: %v", p.ID, err)
		}
		reps[p.ID] = rep
	}
	close(stopQuery)
	queryWG.Wait()
	t.Logf("completed %d concurrent progress queries", queryCount)
	if queryCount == 0 {
		t.Fatal("no progress query executed during parallel run")
	}

	// 并行 + 查询下的结果必须与单独顺序运行一致（结论与事件序列都一致）。
	for _, p := range plans {
		got, want := resultsByID(reps[p.ID]), resultsByID(ref[p.ID])
		if len(got) != len(want) {
			t.Fatalf("plan %s: got %d results, want %d", p.ID, len(got), len(want))
		}
		for id, w := range want {
			if got[id] != w {
				t.Fatalf("plan %s target %s: parallel=%+v sequential=%+v", p.ID, id, got[id], w)
			}
		}
		gotKinds, wantKinds := eventKinds(reps[p.ID]), refKinds[p.ID]
		for id, wk := range wantKinds {
			gk := gotKinds[id]
			if len(gk) != len(wk) {
				t.Fatalf("plan %s target %s: event count %d != sequential %d (%v vs %v)",
					p.ID, id, len(gk), len(wk), gk, wk)
			}
			for i := range wk {
				if gk[i] != wk[i] {
					t.Fatalf("plan %s target %s: event #%d %s != sequential %s", p.ID, id, i, gk[i], wk[i])
				}
			}
		}
		// 结束后进度视图必须为终态一致视图。
		prog, err := r.Progress(p.ID)
		if err != nil {
			t.Fatalf("final progress %s: %v", p.ID, err)
		}
		if prog.Running {
			t.Fatalf("plan %s still running after wait", p.ID)
		}
		for _, tp := range prog.Targets {
			if !tp.Phase.IsTerminal() {
				t.Fatalf("plan %s target %s: non-terminal phase %s after run", p.ID, tp.TargetID, tp.Phase)
			}
		}

		// 落盘状态必须与最终报告逐字段一致（落盘进度/结论与实际跑的对得上）。
		name, err := stateFileName(p.ID)
		if err != nil {
			t.Fatalf("state file name for %s: %v", p.ID, err)
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read persisted state for %s: %v", p.ID, err)
		}
		var ps PlanState
		if err := json.Unmarshal(data, &ps); err != nil {
			t.Fatalf("persisted state for %s corrupt: %v", p.ID, err)
		}
		if ps.PlanID != p.ID {
			t.Fatalf("state file %s belongs to %q, want %q", name, ps.PlanID, p.ID)
		}
		for _, res := range reps[p.ID].Results {
			st, err := ps.Target(res.TargetID)
			if err != nil {
				t.Fatalf("plan %s: persisted state missing target %s", p.ID, res.TargetID)
			}
			disk := resultKey{st.Phase, st.Verdict, st.Failure, st.Cleanup, st.HealRounds}
			if disk != keyOf(res) {
				t.Fatalf("plan %s target %s: persisted=%+v report=%+v", p.ID, res.TargetID, disk, keyOf(res))
			}
		}
		t.Logf("plan %s: persisted state matches report for all %d targets", p.ID, len(p.Targets))
	}
}
