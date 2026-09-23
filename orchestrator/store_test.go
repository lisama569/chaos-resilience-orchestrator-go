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
