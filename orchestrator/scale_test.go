package orchestrator

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// scaleScripts 是规模测试的确定性脚本集：
// 绝大多数目标健康；特殊键模拟不可达、注入失败、清理失败与自愈收敛。
func scaleScripts() map[string]*Script {
	s := simScripts(map[string]*Script{
		"inject": {DoDelay: 2 * time.Millisecond},
		"probe":  {ProbeDelay: time.Millisecond, Values: map[string]float64{"err": 0.01}},
	})
	s["reach-dead"] = &Script{Unreachable: true}
	s["inject-fail"] = &Script{DoFail: true}
	s["recover-fail"] = &Script{DoFail: true}
	s["probe-heal"] = &Script{Values: map[string]float64{"err": 0.9}}
	s["reprobe-heal"] = &Script{Values: map[string]float64{"err": 0.0}}
	return s
}

// scaleTargets 生成 n 个目标，其中绝大多数健康，混入三类确定性异常目标，
// 使并发跑与顺序跑的结果对比覆盖成功与多种失败路径。
func scaleTargets(n int) []TargetSpec {
	var ts []TargetSpec
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("t%02d", i)
		switch i {
		case 3: // 注入前不可达
			spec := testTarget(id, false)
			spec.ReachableBy = "reach-dead"
			ts = append(ts, spec)
		case 7: // 注入动作确定性失败
			spec := testTarget(id, false)
			spec.Inject.Command = "inject-fail"
			ts = append(ts, spec)
		case 11: // 清理始终失败
			spec := testTarget(id, false)
			spec.Recover.Command = "recover-fail"
			ts = append(ts, spec)
		case 13: // 稳态违例 -> 自愈一轮收敛
			spec := testTarget(id, true)
			spec.Probes = []ProbeSpec{{Name: "probe-heal", MinSamples: 1}}
			spec.Rules = []RuleSpec{{Probe: "probe-heal", Metric: "err", Operator: "<=", Threshold: 0.05}}
			spec.Heal.RecheckProbes = []ProbeSpec{{Name: "reprobe-heal", MinSamples: 1}}
			spec.Heal.RecheckRules = []RuleSpec{{Probe: "reprobe-heal", Metric: "err", Operator: "<=", Threshold: 0.05}}
			ts = append(ts, spec)
		default:
			ts = append(ts, testTarget(id, false))
		}
	}
	return ts
}

// resultTuple 是用于并发/顺序结果对比的结论字段集合。
type resultTuple struct {
	Phase   Phase
	Verdict Verdict
	Failure FailureReason
	Cleanup CleanupResult
	Healing int
}

func tupleOf(r TargetResult) resultTuple {
	return resultTuple{r.Phase, r.Verdict, r.Failure, r.Cleanup, r.HealRounds}
}

// CI 规模场景：3 个计划并行，每个 16 个目标、并发上限拉满（16），
// 全程高频查询进度；结束后每个目标的结论必须与同计划顺序跑（并发=1）
// 完全一致，且每份进度快照内部一致、阶段不回退。
func TestScaleParallelPlansWithContinuousQueries(t *testing.T) {
	const nTargets = 16
	planIDs := []string{"scale-alpha", "scale-beta", "scale-gamma"}

	// 1) 参考基线：同一批计划分别以并发=1 顺序跑（真实文件落盘）。
	refDir := t.TempDir()
	ref := map[string]map[string]resultTuple{}
	for _, id := range planIDs {
		plan := namedPlan(id, 1, scaleTargets(nTargets)...)
		eng := NewEngine(NewSimExecutor(scaleScripts()), NewFileStore(filepath.Join(refDir, id, "state.json")))
		rep, err := eng.Run(context.Background(), plan)
		if err != nil {
			t.Fatalf("reference run %s: %v", id, err)
		}
		ref[id] = map[string]resultTuple{}
		for _, r := range rep.Results {
			ref[id][r.TargetID] = tupleOf(r)
		}
	}
	t.Logf("reference (sequential) runs done for %d plans x %d targets", len(planIDs), nTargets)

	// 2) 规模场景：3 计划并行，每个并发上限=目标数，全程持续查询。
	parDir := t.TempDir()
	sup := NewSupervisor(parDir, func(*Plan) Executor { return NewSimExecutor(scaleScripts()) })
	for _, id := range planIDs {
		if err := sup.Start(context.Background(), namedPlan(id, nTargets, scaleTargets(nTargets)...)); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
	}

	var queries atomic.Int64
	stop := make(chan struct{})
	var qw sync.WaitGroup
	for w := 0; w < 4; w++ {
		qw.Add(1)
		go func() {
			defer qw.Done()
			lastPhase := map[string]map[string]int{}
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, id := range planIDs {
					p, err := sup.Progress(id)
					if err != nil {
						t.Errorf("progress %s: %v", id, err)
						continue
					}
					queries.Add(1)
					checkProgressConsistent(t, p)
					lp := lastPhase[id]
					if lp == nil {
						lp = map[string]int{}
						lastPhase[id] = lp
					}
					for _, tp := range p.Targets {
						if prev, ok := lp[tp.TargetID]; ok && phaseOrder[tp.Phase] < prev {
							t.Errorf("plan %s target %s phase regressed %d -> %d", id, tp.TargetID, prev, phaseOrder[tp.Phase])
						}
						lp[tp.TargetID] = phaseOrder[tp.Phase]
					}
				}
			}
		}()
	}

	reports := map[string]*Report{}
	for _, id := range planIDs {
		rep, err := sup.Wait(id)
		if err != nil {
			t.Fatalf("wait %s: %v", id, err)
		}
		reports[id] = rep
	}
	close(stop)
	qw.Wait()
	t.Logf("parallel run finished; progress queries during run: %d", queries.Load())
	if queries.Load() < 100 {
		t.Fatalf("continuous querying not exercised: only %d queries", queries.Load())
	}

	// 3) 并发跑结果必须与顺序跑逐目标一致：阶段、结论、失败原因、清理、自愈轮次。
	for _, id := range planIDs {
		if len(reports[id].Results) != nTargets {
			t.Fatalf("plan %s results=%d", id, len(reports[id].Results))
		}
		for _, r := range reports[id].Results {
			want, ok := ref[id][r.TargetID]
			if !ok {
				t.Fatalf("plan %s: unexpected target %s", id, r.TargetID)
			}
			got := tupleOf(r)
			if got != want {
				t.Fatalf("plan %s target %s: parallel %+v != sequential %+v", id, r.TargetID, got, want)
			}
		}
		// 最终进度快照与报告一致，且状态真实落盘、可独立加载。
		p, err := sup.Progress(id)
		if err != nil || p.Status != RunFinished {
			t.Fatalf("plan %s final progress: %+v err=%v", id, p, err)
		}
		checkProgressConsistent(t, p)
		loaded, err := NewFileStore(filepath.Join(parDir, id, "state.json")).Load()
		if err != nil {
			t.Fatalf("plan %s state file: %v", id, err)
		}
		if len(loaded.Targets) != nTargets {
			t.Fatalf("plan %s persisted targets=%d", id, len(loaded.Targets))
		}
	}
	// 三份状态文件互相独立：计划 ID 不串。
	for _, id := range planIDs {
		loaded, _ := NewFileStore(filepath.Join(parDir, id, "state.json")).Load()
		if loaded.PlanID != id {
			t.Fatalf("state file pollution: want %s got %s", id, loaded.PlanID)
		}
	}
	t.Logf("parallel results identical to sequential reference for all %d plans x %d targets", len(planIDs), nTargets)
}
