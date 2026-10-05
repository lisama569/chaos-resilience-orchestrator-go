package orchestrator

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestAdvanceRejectsRollbackAndTerminalRewrite(t *testing.T) {
	now := time.Unix(1000, 0)
	st := &TargetState{ID: "t"}

	if err := st.Advance(PhaseInjected, now); err != nil {
		t.Fatal(err)
	}
	// 注入后 -> 注入前，属于回退，必须拒绝。
	if err := st.Advance(PhaseInjecting, now); !errors.Is(err, ErrPhaseRollback) {
		t.Fatalf("want ErrPhaseRollback, got %v", err)
	}
	t.Logf("rollback injecting->injected rejected: %v", ErrPhaseRollback)

	// 进入终态后任何推进都必须被拒绝。
	if err := st.Advance(PhaseCompleted, now); err != nil {
		t.Fatal(err)
	}
	if err := st.Advance(PhaseCleaning, now); !errors.Is(err, ErrAlreadyTerminal) {
		t.Fatalf("want ErrAlreadyTerminal, got %v", err)
	}
	if err := st.Advance(PhaseAborted, now); !errors.Is(err, ErrAlreadyTerminal) {
		t.Fatalf("want ErrAlreadyTerminal, got %v", err)
	}
	t.Log("terminal rewrite rejected")
}

func TestMarkDoneRejectsDuplicateReport(t *testing.T) {
	st := &TargetState{ID: "t"}
	if err := st.MarkDone("inject", "a1", time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	// 同一步骤的重复完成上报必须被明确拒绝，而非静默接受。
	err := st.MarkDone("inject", "a1", time.Unix(2, 0))
	if !errors.Is(err, ErrStaleReport) {
		t.Fatalf("want ErrStaleReport, got %v", err)
	}
	t.Logf("duplicate report rejected: %v", err)
	if len(st.Done) != 1 {
		t.Fatalf("Done should contain exactly 1 entry, got %d", len(st.Done))
	}
}

func TestFileStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store := NewFileStore(path)

	if _, err := store.Load(); !errors.Is(err, ErrNotExist) {
		t.Fatalf("want ErrNotExist, got %v", err)
	}

	ps := &PlanState{
		PlanID:    "p",
		AttemptID: "a",
		Targets: map[string]*TargetState{
			"t": {ID: "t", Phase: PhaseCompleted, Cleanup: CleanupSucceeded,
				Done: []StepDone{{Name: "inject", At: time.Unix(1, 0)}}},
		},
	}
	if err := store.Save(ps); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.PlanID != "p" || got.Targets["t"].Phase != PhaseCompleted {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
	t.Logf("file store round-trip ok: phase=%s done=%v", got.Targets["t"].Phase, got.Targets["t"].Done)
}

// TestClonePlanStateDeepCopy 覆盖业务：状态在锁内改完、锁外落盘期间，
// 下一次变更绝不能写进正在落盘的副本（否则 race 下会出现写一半的状态文件）。
func TestClonePlanStateDeepCopy(t *testing.T) {
	ps := &PlanState{
		PlanID:    "p1",
		AttemptID: "p1#1",
		EventSeq:  2,
		Targets: map[string]*TargetState{
			"t1": {
				ID:            "t1",
				Phase:         PhaseProbing,
				Verdict:       VerdictSteady,
				Cleanup:       CleanupSucceeded,
				FaultInjected: true,
				Done:          []StepDone{{Name: "inject"}},
				Events: []Event{
					{Seq: 1, Kind: "a", Detail: map[string]any{"ok": true}},
					{Seq: 2, Kind: "b"},
				},
			},
		},
	}
	cp := clonePlanState(ps)
	if cp == ps {
		t.Fatal("clone shares top-level PlanState with original")
	}
	if cp.Targets["t1"] == ps.Targets["t1"] {
		t.Fatal("clone shares TargetState pointer with original")
	}

	// 对原状态做后续变更：追加事件、改阶段、改已完成步骤、改详情。
	st := ps.Targets["t1"]
	st.Phase = PhaseCompleted
	st.Events[0].Detail["ok"] = false
	st.Events = append(st.Events, Event{Seq: 3, Kind: "c"})
	st.Done = append(st.Done, StepDone{Name: "probe"})

	c := cp.Targets["t1"]
	if c.Phase != PhaseProbing {
		t.Fatalf("clone phase mutated: %s", c.Phase)
	}
	if len(c.Events) != 2 {
		t.Fatalf("clone events aliased, len=%d", len(c.Events))
	}
	if c.Events[0].Detail["ok"] != true {
		t.Fatalf("clone event detail aliased: %v", c.Events[0].Detail)
	}
	if len(c.Done) != 1 {
		t.Fatalf("clone done slice aliased, len=%d", len(c.Done))
	}
	t.Logf("deep clone isolated from subsequent mutation: %d events, %d done steps",
		len(c.Events), len(c.Done))
}
