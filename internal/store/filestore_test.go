package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
)

func mkState(id string) *model.ExperimentState {
	return &model.ExperimentState{
		ID:        model.ExperimentID(id),
		Status:    model.StatusPending,
		Version:   1,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

func TestCreateCommitLoad(t *testing.T) {
	dir := t.TempDir()
	st, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err := st.Create(ctx, mkState("e1")); err != nil {
		t.Fatal(err)
	}
	// 重复创建必须被拒绝
	if err := st.Create(ctx, mkState("e1")); err == nil {
		t.Fatal("duplicate create must fail")
	}

	got, err := st.Load(ctx, "e1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 1 {
		t.Fatalf("version=%d want 1", got.Version)
	}

	got.Status = model.StatusRunning
	got2, err := st.Commit(ctx, got, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got2.Version != 2 {
		t.Fatalf("version=%d want 2", got2.Version)
	}

	// 用过期版本再次提交 => 版本冲突，拒绝静默覆盖（重复上报防护）
	if _, err := st.Commit(ctx, got, 1); !errors.Is(err, model.ErrVersionConflict) {
		t.Fatalf("stale commit must return ErrVersionConflict, got %v", err)
	}

	// 并发串行提交：每次必须基于最新版本，旧快照一律拒绝
	fresh, _ := st.Load(ctx, "e1")
	fresh.Status = model.StatusPaused
	v3, err := st.Commit(ctx, fresh, fresh.Version)
	if err != nil {
		t.Fatal(err)
	}
	fresh2, _ := st.Load(ctx, "e1")
	fresh2.Status = model.StatusRunning
	if _, err := st.Commit(ctx, fresh2, v3.Version); err != nil {
		t.Fatalf("fresh commit should succeed: %v", err)
	}
}

func TestListSorted(t *testing.T) {
	dir := t.TempDir()
	st, _ := NewFileStore(dir)
	ctx := context.Background()
	for _, id := range []string{"b", "a", "c"} {
		if err := st.Create(ctx, mkState(id)); err != nil {
			t.Fatal(err)
		}
	}
	list, err := st.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 3 || list[0].ID != "a" || list[2].ID != "c" {
		t.Fatalf("list not sorted: %v", list)
	}
}
