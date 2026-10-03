package orchestrator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// checkProgressConsistent 校验一份进度快照的内部一致性：
// 阶段与结论/清理状态必须对得上；目标事件流序号必须严格递增
// （全局序号在目标间交错，目标视角是子序列，不允许倒序或重复）。
// 该函数可在任意协程中调用（只使用 t.Errorf，不会中断调用方）。
func checkProgressConsistent(t *testing.T, p PlanProgress) {
	t.Helper()
	for _, tp := range p.Targets {
		for i := 1; i < len(tp.RecentEvents); i++ {
			if tp.RecentEvents[i].Seq <= tp.RecentEvents[i-1].Seq {
				t.Errorf("target %s: event seq not increasing %d -> %d (torn read?)",
					tp.TargetID, tp.RecentEvents[i-1].Seq, tp.RecentEvents[i].Seq)
			}
		}
		if tp.Verdict != "" && phaseOrder[tp.Phase] < phaseOrder[PhaseProbing] {
			t.Errorf("target %s: verdict=%s but phase=%s (before probing)", tp.TargetID, tp.Verdict, tp.Phase)
		}
		switch tp.Phase {
		case PhaseCompleted:
			if tp.Verdict != VerdictSteady || tp.Cleanup != CleanupSucceeded || tp.Failure != ReasonNone {
				t.Errorf("target %s: completed but verdict=%s cleanup=%s failure=%s",
					tp.TargetID, tp.Verdict, tp.Cleanup, tp.Failure)
			}
		case PhaseAborted:
			if tp.Failure == ReasonNone || tp.Failure == "" {
				t.Errorf("target %s: aborted without failure reason", tp.TargetID)
			}
		case PhaseCleanupFailed:
			if tp.Cleanup != CleanupFailed {
				t.Errorf("target %s: cleanup_failed phase but cleanup=%s", tp.TargetID, tp.Cleanup)
			}
		}
		if tp.Cleanup == CleanupSucceeded && tp.FaultInjected {
			t.Errorf("target %s: cleanup succeeded but fault still marked injected", tp.TargetID)
		}
	}
}

// 运行前查询：明确返回 not_started，而不是编造进度。
func TestProgressBeforeRun(t *testing.T) {
	eng := NewEngine(NewSimExecutor(simScripts(nil)), NewMemoryStore())
	p := eng.Progress()
	if p.Status != RunNotStarted || len(p.Targets) != 0 {
		t.Fatalf("before run: %+v", p)
	}
	if _, err := eng.TargetProgress("x"); !errors.Is(err, ErrNotExist) {
		t.Fatalf("want ErrNotExist, got %v", err)
	}
}

// 运行中查询：注入动作被屏障挡住时，必须能查到目标正处于 injecting 阶段；
// 查询不阻塞执行，放行后实验正常完成，最终快照与报告一致。
func TestProgressQueriedDuringRun(t *testing.T) {
	exec := newBarrierExecutor(8)
	defer exec.close()
	plan := testPlan(testTarget("a", false), testTarget("b", false))
	plan.Concurrency = 2
	eng := NewEngine(exec, NewMemoryStore(), WithLogger(t.Logf))

	done := make(chan *Report, 1)
	go func() {
		rep, err := eng.Run(context.Background(), plan)
		if err != nil {
			t.Errorf("run: %v", err)
		}
		done <- rep
	}()

	<-exec.starts
	<-exec.starts // 两个目标都进入注入动作

	p := eng.Progress()
	if p.Status != RunRunning {
		t.Fatalf("mid-run status=%s", p.Status)
	}
	if len(p.Targets) != 2 {
		t.Fatalf("mid-run targets=%d", len(p.Targets))
	}
	checkProgressConsistent(t, p)
	for _, tp := range p.Targets {
		t.Logf("mid-run target=%s phase=%s verdict=%q cleanup=%s events=%d",
			tp.TargetID, tp.Phase, tp.Verdict, tp.Cleanup, len(tp.RecentEvents))
		if tp.Phase != PhaseInjecting {
			t.Fatalf("target %s mid-inject phase=%s", tp.TargetID, tp.Phase)
		}
		if tp.Cleanup != CleanupNotNeeded || tp.Verdict != "" {
			t.Fatalf("target %s: premature conclusion %+v", tp.TargetID, tp)
		}
	}
	tp, err := eng.TargetProgress("a")
	if err != nil || tp.Phase != PhaseInjecting {
		t.Fatalf("target progress: %+v err=%v", tp, err)
	}
	if _, err := eng.TargetProgress("nope"); !errors.Is(err, ErrUnknownTarget) {
		t.Fatalf("want ErrUnknownTarget, got %v", err)
	}

	exec.close()
	rep := <-done
	final := eng.Progress()
	if final.Status != RunFinished {
		t.Fatalf("final status=%s", final.Status)
	}
	checkProgressConsistent(t, final)
	for i, r := range rep.Results {
		fp := final.Targets[i]
		if fp.TargetID != r.TargetID || fp.Phase != r.Phase || fp.Verdict != r.Verdict ||
			fp.Cleanup != r.Cleanup || fp.HealRounds != r.HealRounds || fp.Failure != r.Failure {
			t.Fatalf("final progress mismatch with report: %+v vs %+v", fp, r)
		}
	}
}

// 高频并发查询 + 真实执行：多协程持续查询不得改变实验结果，
// 且每一份快照都必须内部一致、阶段单调不回退。
func TestProgressConcurrentQueriesConsistent(t *testing.T) {
	exec := NewSimExecutor(simScripts(map[string]*Script{
		"inject": {DoDelay: 5 * time.Millisecond},
		"probe":  {ProbeDelay: 2 * time.Millisecond, Values: map[string]float64{"err": 0.01}},
	}))
	plan := testPlan(testTarget("t1", false), testTarget("t2", false), testTarget("t3", false), testTarget("t4", false))
	plan.Concurrency = 4
	eng := NewEngine(exec, NewMemoryStore())

	done := make(chan struct{})
	go func() {
		rep, err := eng.Run(context.Background(), plan)
		if err != nil {
			t.Errorf("run: %v", err)
		}
		for _, r := range rep.Results {
			if r.Phase != PhaseCompleted || r.Verdict != VerdictSteady || r.Cleanup != CleanupSucceeded {
				t.Errorf("result changed by queries: %+v", r)
			}
		}
		close(done)
	}()

	// 8 个协程持续查询。每个协程各自校验自己读到的快照序列：
	// 同一协程的程序次序保证读到单调变新的快照，因此阶段不得回退。
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lastPhase := map[string]int{}
			for {
				select {
				case <-done:
					return
				default:
				}
				p := eng.Progress()
				checkProgressConsistent(t, p)
				for _, tp := range p.Targets {
					if prev, ok := lastPhase[tp.TargetID]; ok && phaseOrder[tp.Phase] < prev {
						t.Errorf("target %s phase regressed %d -> %d", tp.TargetID, prev, phaseOrder[tp.Phase])
					}
					lastPhase[tp.TargetID] = phaseOrder[tp.Phase]
				}
			}
		}()
	}
	<-done
	wg.Wait()
	final := eng.Progress()
	checkProgressConsistent(t, final)
	if final.Status != RunFinished || len(final.Targets) != 4 {
		t.Fatalf("final: %+v", final)
	}
	t.Logf("final targets=%d status=%s", len(final.Targets), final.Status)
}
