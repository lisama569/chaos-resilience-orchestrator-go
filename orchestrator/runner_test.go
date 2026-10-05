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
		abs, _, err := planStateLocation(dir, p.ID)
		if err != nil {
			t.Fatalf("state location for %s: %v", p.ID, err)
		}
		data, err := os.ReadFile(abs)
		if err != nil {
			t.Fatalf("state file for %s: %v", p.ID, err)
		}
		var ps PlanState
		if err := json.Unmarshal(data, &ps); err != nil {
			t.Fatalf("state file for %s corrupt: %v", p.ID, err)
		}
		if ps.PlanID != p.ID {
			t.Fatalf("state file %s belongs to %s", abs, ps.PlanID)
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

	// plan-a 含全部 16 个目标：满并发下每个目标的阶段/结论/原因/清理都必须
	// 与单独顺序运行逐字段一致；失败目标必须给出具体原因与详情，不得含糊。
	refRunner := NewRunner(NewSimExecutor(scripts), WithStateDir(filepath.Join(t.TempDir(), "ref")))
	refRep, err := refRunner.Run(context.Background(), planA)
	if err != nil {
		t.Fatalf("sequential reference run: %v", err)
	}
	gotA, wantA := resultsByID(reps["plan-a"]), resultsByID(refRep)
	for id, w := range wantA {
		if gotA[id] != w {
			t.Fatalf("plan-a target %s differs under parallelism: parallel=%+v sequential=%+v",
				id, gotA[id], w)
		}
	}
	resByID := map[string]TargetResult{}
	for _, res := range reps["plan-a"].Results {
		resByID[res.TargetID] = res
	}
	for _, id := range []string{"t14", "t15"} {
		res := resByID[id]
		if res.FailureDetail == "" {
			t.Fatalf("target %s: aborted without concrete failure detail", id)
		}
		if res.Cleanup != CleanupNotNeeded {
			t.Fatalf("target %s: fault never injected, cleanup must be not_needed, got %s", id, res.Cleanup)
		}
		t.Logf("[failure-boundary] target %s phase=%s reason=%s detail=%q cleanup=%s",
			id, res.Phase, res.Failure, res.FailureDetail, res.Cleanup)
	}
	for id := 0; id <= 13; id++ {
		tid := fmt.Sprintf("t%02d", id)
		if resByID[tid].Phase != PhaseCompleted {
			t.Fatalf("target %s: expected completed, got %+v", tid, resByID[tid])
		}
	}
	t.Logf("[failure-boundary] plan-a: all 16 targets field-identical to sequential run under concurrency=16")
}

// TestRunnerHighConcurrencyWithQueries CI 级场景：
// 3 个计划并行（其中一个 16 目标、并发上限拉满，且计划 ID 含斜杠/空格/中文），
// 运行期间每计划多个 goroutine 高频查询进度；全部结束后，每个目标的阶段、
// 结论、失败原因、清理结果、自愈轮次与事件序列都必须与单独顺序运行逐字段一致。
// 状态全部真实落盘（t.TempDir 下的 FileStore），不使用内存桩。
func TestRunnerHighConcurrencyWithQueries(t *testing.T) {
	planA, scriptsA := mixedPlan("ci/生产 订单#1", 16) // 16 目标，并发上限拉满
	planB, scriptsB := mixedPlan("ci-灰度.2", 4)
	planB.Targets = planB.Targets[:6]
	planC, scriptsC := mixedPlan(`ci\回归_3`, 8)
	planC.Targets = planC.Targets[10:] // 自愈收敛 + 违例组合
	plans := []*Plan{planA, planB, planC}

	scripts := map[string]*Script{}
	mergeScripts(scripts, scriptsA)
	mergeScripts(scripts, scriptsB)
	mergeScripts(scripts, scriptsC)

	// 参考值：每个计划单独顺序运行（各自独立落盘目录）。
	ref := map[string]*Report{}
	refKinds := map[string]map[string][]string{}
	for i, p := range plans {
		rep, err := NewRunner(NewSimExecutor(scripts),
			WithStateDir(filepath.Join(t.TempDir(), fmt.Sprintf("ref-%d", i)))).
			Run(context.Background(), p)
		if err != nil {
			t.Fatalf("reference run %s: %v", p.ID, err)
		}
		ref[p.ID] = rep
		refKinds[p.ID] = eventKinds(rep)
		t.Logf("[reference] plan=%q targets=%d", p.ID, len(rep.Results))
	}

	// 并行运行 + 持续查询。
	r := NewRunner(NewSimExecutor(scripts), WithStateDir(t.TempDir()))
	ctx := context.Background()
	for _, p := range plans {
		if err := r.Start(ctx, p); err != nil {
			t.Fatalf("start %q: %v", p.ID, err)
		}
	}

	var queryWG sync.WaitGroup
	stopQuery := make(chan struct{})
	queryCounts := map[string]int64{}
	var countMu sync.Mutex
	for _, p := range plans {
		for q := 0; q < 3; q++ { // 每个计划三个并发高频查询者
			queryWG.Add(1)
			go func(planID string, worker int) {
				defer queryWG.Done()
				var n int64
				defer func() {
					countMu.Lock()
					queryCounts[planID] += n
					countMu.Unlock()
				}()
				for {
					select {
					case <-stopQuery:
						return
					default:
					}
					prog, err := r.Progress(planID)
					if err != nil {
						t.Errorf("progress %s worker %d: %v", planID, worker, err)
						return
					}
					checkSnapshotInvariants(t, prog)
					n++
				}
			}(p.ID, q)
		}
	}

	reps := map[string]*Report{}
	for _, p := range plans {
		rep, err := r.Wait(p.ID)
		if err != nil {
			t.Fatalf("wait %q: %v", p.ID, err)
		}
		reps[p.ID] = rep
	}
	close(stopQuery)
	queryWG.Wait()
	var totalQueries int64
	for id, n := range queryCounts {
		if n == 0 {
			t.Fatalf("plan %q had no progress query during the run", id)
		}
		totalQueries += n
		t.Logf("[queries] plan=%q queries=%d", id, n)
	}
	t.Logf("[queries] total concurrent read-only progress queries=%d", totalQueries)

	// 逐计划比对：并行 + 高频查询下的结果必须与单独顺序运行完全一致。
	for _, p := range plans {
		pid := p.ID
		t.Run("parity/"+pid, func(t *testing.T) {
			got, want := resultsByID(reps[pid]), resultsByID(ref[pid])
			if len(got) != len(want) {
				t.Fatalf("got %d results, want %d", len(got), len(want))
			}
			for id, w := range want {
				if got[id] != w {
					t.Fatalf("target %s: parallel=%+v sequential=%+v", id, got[id], w)
				}
			}
			gotKinds, wantKinds := eventKinds(reps[pid]), refKinds[pid]
			for id, wk := range wantKinds {
				gk := gotKinds[id]
				if len(gk) != len(wk) {
					t.Fatalf("target %s: event count %d != sequential %d", id, len(gk), len(wk))
				}
				for i := range wk {
					if gk[i] != wk[i] {
						t.Fatalf("target %s: event #%d %s != sequential %s", id, i, gk[i], wk[i])
					}
				}
			}
			// 结束后进度视图必须为终态一致视图。
			prog, err := r.Progress(pid)
			if err != nil {
				t.Fatalf("final progress: %v", err)
			}
			if prog.Running {
				t.Fatalf("plan still running after wait")
			}
			for _, tp := range prog.Targets {
				if !tp.Phase.IsTerminal() {
					t.Fatalf("target %s non-terminal phase %s after run", tp.TargetID, tp.Phase)
				}
			}
			t.Logf("[parity] plan=%q targets=%d fields+events identical to sequential run, queries=%d",
				pid, len(want), queryCounts[pid])
		})
	}
}

// TestRunnerExoticPlanIDsIsolatedOnDisk 覆盖业务：计划 ID 含斜杠、空格、
// 中文（旧的文件名安全化会把它们全部撞成同一文件）时，并行运行仍必须各自
// 落盘、状态互不污染、重启续跑读到的是自己的状态。全程真实 FileStore。
func TestRunnerExoticPlanIDsIsolatedOnDisk(t *testing.T) {
	dir := t.TempDir()
	// 旧规则下 "prod/a b"、"prod/a.b"、"prod/a_b" 都会被压成同一文件名，
	// 是最容易复现“两份运行写同一处”的一组 ID。
	planX, scriptsX := mixedPlan("prod/订单 svc", 16)
	planY, scriptsY := mixedPlan("prod/订单.svc", 8)
	planY.Targets = planY.Targets[:6]
	planZ, scriptsZ := mixedPlan(`prod\订单_svc`, 4)
	planZ.Targets = planZ.Targets[12:]
	plans := []*Plan{planX, planY, planZ}

	scripts := map[string]*Script{}
	mergeScripts(scripts, scriptsX)
	mergeScripts(scripts, scriptsY)
	mergeScripts(scripts, scriptsZ)

	r := NewRunner(NewSimExecutor(scripts), WithStateDir(dir))
	for _, p := range plans {
		if err := r.Start(context.Background(), p); err != nil {
			t.Fatalf("start %q: %v", p.ID, err)
		}
	}
	for _, p := range plans {
		rep, err := r.Wait(p.ID)
		if err != nil {
			t.Fatalf("wait %q: %v", p.ID, err)
		}
		if len(rep.Results) != len(p.Targets) {
			t.Fatalf("plan %q: got %d results, want %d", p.ID, len(rep.Results), len(p.Targets))
		}
	}

	// 三个不同 ID 必须对应三个不同的状态文件，且文件内 plan_id 与目标数都对得上。
	paths := map[string]bool{}
	for _, p := range plans {
		abs, _, err := planStateLocation(dir, p.ID)
		if err != nil {
			t.Fatalf("location %q: %v", p.ID, err)
		}
		if paths[abs] {
			t.Fatalf("plan %q shares state file with another plan: %s", p.ID, abs)
		}
		paths[abs] = true
		data, err := os.ReadFile(abs)
		if err != nil {
			t.Fatalf("read state for %q: %v", p.ID, err)
		}
		var ps PlanState
		if err := json.Unmarshal(data, &ps); err != nil {
			t.Fatalf("corrupt state for %q: %v", p.ID, err)
		}
		if ps.PlanID != p.ID {
			t.Fatalf("state file %s belongs to %q, want %q", abs, ps.PlanID, p.ID)
		}
		if len(ps.Targets) != len(p.Targets) {
			t.Fatalf("plan %q: state has %d targets, want %d", p.ID, len(ps.Targets), len(p.Targets))
		}
		t.Logf("plan %q -> %s (%d targets, isolated)", p.ID, filepath.Base(abs), len(ps.Targets))
	}

	// 用全新 Runner 指向同一目录续跑：每个计划必须读到自己的终态，
	// 全部走“终态跳过”，不执行任何动作，且结论字段不回退。
	exec := &countingExecutor{inner: NewSimExecutor(scripts)}
	r2 := NewRunner(exec, WithStateDir(dir))
	for _, p := range plans {
		rep, err := r2.Run(context.Background(), p)
		if err != nil {
			t.Fatalf("resume %q: %v", p.ID, err)
		}
		for _, res := range rep.Results {
			if !res.Phase.IsTerminal() {
				t.Fatalf("plan %q target %s lost terminal state on resume: %s", p.ID, res.TargetID, res.Phase)
			}
		}
	}
	if len(exec.doCommands) != 0 {
		t.Fatalf("resume re-executed actions on foreign/own state: %v", exec.doCommands)
	}
	t.Logf("resume of %d exotic-id plans executed 0 actions (each read its own state)", len(plans))
}

// TestRunnerLocationConflictRejected 自定义工厂把两个不同计划 ID 映射到
// 同一落点时，第二个计划必须在发起阶段被明确拒绝，绝不允许两份运行共用状态。
func TestRunnerLocationConflictRejected(t *testing.T) {
	dir := t.TempDir()
	shared := filepath.Join(dir, "shared.state.json")
	factory := func(planID string) (Store, string, error) {
		return NewFileStore(shared), shared, nil
	}
	r := NewRunner(NewSimExecutor(simScripts(nil)), WithStoreFactory(factory))

	planA := testPlan(testTarget("t1", false))
	planA.ID = "plan-a"
	if err := r.Start(context.Background(), planA); err != nil {
		t.Fatalf("first plan should start: %v", err)
	}
	// 不同 ID、同一落点：即使前一个仍在运行，也必须先报落点冲突。
	planB := testPlan(testTarget("t2", false))
	planB.ID = "plan-b"
	err := r.Start(context.Background(), planB)
	if !errors.Is(err, ErrPlanLocationConflict) {
		t.Fatalf("want ErrPlanLocationConflict, got %v", err)
	}
	t.Logf("conflicting location rejected at start: %v", err)

	if _, err := r.Wait(planA.ID); err != nil {
		t.Fatalf("wait plan-a: %v", err)
	}
	// 前一个结束后，冲突规则依旧成立（不能因为槽位空了就允许不同计划接管状态）。
	err = r.Start(context.Background(), planB)
	if !errors.Is(err, ErrPlanLocationConflict) {
		t.Fatalf("want ErrPlanLocationConflict after first run finished, got %v", err)
	}
	t.Logf("conflicting location still rejected after first run finished: %v", err)

	// 同一 ID 续跑复用同一落点是合法的，不得被冲突规则误伤。
	if err := r.Start(context.Background(), planA); err != nil {
		t.Fatalf("same-plan resume should be allowed: %v", err)
	}
	if _, err := r.Wait(planA.ID); err != nil {
		t.Fatalf("wait plan-a resume: %v", err)
	}
}

// TestRunnerUnsafePlanIDRejected 计划 ID 不适合落盘时，Start 必须直接拒绝
// 并说清原因，运行绝不被发起（随后查询应表现为未发起）。
func TestRunnerUnsafePlanIDRejected(t *testing.T) {
	dir := t.TempDir()
	r := NewRunner(NewSimExecutor(simScripts(nil)), WithStateDir(dir))
	plan := testPlan(testTarget("t1", false))
	plan.ID = "bad\x00plan"
	err := r.Start(context.Background(), plan)
	if !errors.Is(err, ErrPlanIDUnsafe) {
		t.Fatalf("want ErrPlanIDUnsafe, got %v", err)
	}
	t.Logf("unsafe plan id rejected at start: %v", err)
	if _, perr := r.Progress(plan.ID); !errors.Is(perr, ErrPlanNotStarted) {
		t.Fatalf("rejected plan must not be registered as started, got %v", perr)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("rejected start must not create state files, found %v", entries)
	}
}
