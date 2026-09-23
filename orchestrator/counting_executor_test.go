package orchestrator

import (
	"context"
	"sync"
	"time"
)

// countingExecutor 包装一个执行器，记录实际发生的动作命令与探测次数，
// 用于验证“已完成步骤不重复执行”。
type countingExecutor struct {
	mu         sync.Mutex
	inner      Executor
	doCommands []string
	probes     int
	reaches    int
}

func (c *countingExecutor) Reachable(ctx context.Context, target, probeName string) error {
	c.mu.Lock()
	c.reaches++
	c.mu.Unlock()
	return c.inner.Reachable(ctx, target, probeName)
}

func (c *countingExecutor) Do(ctx context.Context, act ActionSpec) error {
	c.mu.Lock()
	c.doCommands = append(c.doCommands, act.Command)
	c.mu.Unlock()
	return c.inner.Do(ctx, act)
}

func (c *countingExecutor) Probe(ctx context.Context, target string, p ProbeSpec) (Sample, error) {
	c.mu.Lock()
	c.probes++
	c.mu.Unlock()
	return c.inner.Probe(ctx, target, p)
}

var _ Executor = (*countingExecutor)(nil)
var _ = time.Second
