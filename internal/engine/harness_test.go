package engine

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/action"
	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/probe"
	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/store"
)

type testHarness struct {
	rt   *action.LocalEnv
	st   *store.FileStore
	eng  *Engine
	logf *log.Logger
}

func newHarness(t *testing.T) *testHarness {
	t.Helper()
	dir := t.TempDir()
	clock := newFakeClock()
	rt := action.NewLocalEnv().WithClock(clock.Now)
	st, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	logger := log.New(os.Stdout, "[engine-test] ", log.LstdFlags)
	eng := New(st, rt, &stdLoggerAdapter{l: logger})
	return &testHarness{rt: rt, st: st, eng: eng, logf: logger}
}

type stdLoggerAdapter struct{ l *log.Logger }

func (a *stdLoggerAdapter) Printf(format string, args ...any) { a.l.Printf(format, args...) }

// fakeClock 提供单调确定的时间戳。
type fakeClock struct {
	t time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)} }
func (c *fakeClock) Now() time.Time {
	// 不使用锁：测试编排本身对时间的读取无强一致要求。
	return c.t
}

// act 构造动作规格的简写。
func act(kind string, timeout time.Duration, params map[string]string) model.ActionSpec {
	return model.ActionSpec{Kind: kind, Timeout: timeout, Params: params}
}

func pr(name, kind, key string, want bool, minHits int) model.ProbeSpec {
	return model.ProbeSpec{Name: name, Kind: kind, Key: key, Want: want, MinHits: minHits, Timeout: time.Second}
}

func (h *testHarness) run(t *testing.T, spec model.ExperimentSpec) *model.ExperimentState {
	t.Helper()
	st, err := h.eng.Run(context.Background(), spec)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return st
}

func (h *testHarness) load(t *testing.T, id string) *model.ExperimentState {
	t.Helper()
	st, err := h.st.Load(context.Background(), model.ExperimentID(id))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func init() {
	action.RegisterBuiltins()
	probe.RegisterBuiltins()
}
