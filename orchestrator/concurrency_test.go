package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"
)

// barrierExecutor 在 Do 中阻塞，直到 release 被调用，用于测量实际并发。
type barrierExecutor struct {
	mu      sync.Mutex
	active  int
	maxSeen int
	starts  chan struct{}
	release chan struct{}
	relOnce sync.Once
}

func newBarrierExecutor(buffer int) *barrierExecutor {
	return &barrierExecutor{
		starts:  make(chan struct{}, buffer),
		release: make(chan struct{}),
	}
}

func (b *barrierExecutor) Reachable(ctx context.Context, target, probeName string) error { return nil }

func (b *barrierExecutor) Do(ctx context.Context, act ActionSpec) error {
	b.mu.Lock()
	b.active++
	if b.active > b.maxSeen {
		b.maxSeen = b.active
	}
	b.mu.Unlock()
	b.starts <- struct{}{} // 缓冲足够大，永不阻塞测试线程

	select {
	case <-b.release:
	case <-ctx.Done():
	}
	b.mu.Lock()
	b.active--
	b.mu.Unlock()
	return nil
}

func (b *barrierExecutor) Probe(ctx context.Context, target string, p ProbeSpec) (Sample, error) {
	return Sample{Values: map[string]float64{"err": 0.0}}, nil
}

func (b *barrierExecutor) close() { b.relOnce.Do(func() { close(b.release) }) }

func TestEngineConcurrencyLimit(t *testing.T) {
	const n, limit = 5, 2
	exec := newBarrierExecutor(64)
	var targets []TargetSpec
	for i := 0; i < n; i++ {
		spec := testTarget("x", false)
		spec.ID = "t" + string(rune('a'+i))
		spec.Target = "svc-" + spec.ID
		spec.Inject.Target = spec.Target
		spec.Recover.Target = spec.Target
		targets = append(targets, spec)
	}
	plan := testPlan(targets...)
	plan.Concurrency = limit
	eng := NewEngine(exec, NewMemoryStore(), WithLogger(t.Logf))

	done := make(chan struct{})
	go func() {
		_, _ = eng.Run(context.Background(), plan)
		close(done)
	}()

	// 等到恰好 limit 个注入动作开始，此时不应有更多动作开始。
	for i := 0; i < limit; i++ {
		<-exec.starts
	}
	select {
	case <-exec.starts:
		t.Fatalf("concurrency exceeded limit %d", limit)
	case <-time.After(80 * time.Millisecond):
	}
	exec.mu.Lock()
	got := exec.maxSeen
	exec.mu.Unlock()
	if got != limit {
		t.Fatalf("want max concurrency %d, observed %d", limit, got)
	}
	t.Logf("observed max concurrency=%d as configured", got)

	exec.close()
	<-done
}
