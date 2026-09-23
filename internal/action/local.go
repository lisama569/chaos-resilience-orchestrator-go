package action

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"
)

// LocalEnv 是完全本地、确定性的受控环境：
// flags 表示故障/健康开关，metrics 是单调计数器，供 flaky 动作与探针使用。
type LocalEnv struct {
	mu      sync.Mutex
	flags   map[string]bool
	metrics map[string]int
	nowFn   func() time.Time
}

// NewLocalEnv 创建本地环境。
func NewLocalEnv() *LocalEnv {
	return &LocalEnv{
		flags:   map[string]bool{},
		metrics: map[string]int{},
		nowFn:   time.Now,
	}
}

// WithClock 注入时钟（测试可获得完全确定性的时间戳）。
func (e *LocalEnv) WithClock(f func() time.Time) *LocalEnv {
	e.nowFn = f
	return e
}

func (e *LocalEnv) Flag(key string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.flags[key]
}

func (e *LocalEnv) SetFlag(key string, v bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.flags[key] = v
}

func (e *LocalEnv) Metric(key string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.metrics[key]
}

func (e *LocalEnv) AddMetric(key string, delta int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.metrics[key] += delta
}

func (e *LocalEnv) Now() time.Time { return e.nowFn() }

func (e *LocalEnv) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// ---- 内置动作 ----

// setFlagAction 设置一个故障/健康开关。参数: key, value(true/false)
type setFlagAction struct{ name string }

func (a setFlagAction) Name() string { return a.name }
func (a setFlagAction) Execute(ctx context.Context, rt Runtime, p map[string]string) error {
	v, err := strconv.ParseBool(p["value"])
	if err != nil {
		return &FailureError{Kind: KindFailed, Detail: fmt.Sprintf("bad value %q", p["value"])}
	}
	rt.SetFlag(p["key"], v)
	return nil
}

// unreachableAction 始终报目标不可达。
type unreachableAction struct{}

func (unreachableAction) Name() string { return "unreachable" }
func (unreachableAction) Execute(ctx context.Context, rt Runtime, p map[string]string) error {
	return &FailureError{Kind: KindUnreachable, Detail: "simulated unreachable target"}
}

// flakyAction 前 fail_until 次执行失败，之后成功（计数器由 params.key 指定）。
type flakyAction struct{}

func (flakyAction) Name() string { return "flaky" }
func (flakyAction) Execute(ctx context.Context, rt Runtime, p map[string]string) error {
	n, _ := strconv.Atoi(p["fail_until"])
	key := p["key"]
	calls := rt.Metric(key) + 1
	rt.AddMetric(key, 1)
	if calls <= n {
		return &FailureError{Kind: KindFailed, Detail: fmt.Sprintf("flaky failure %d/%d", calls, n)}
	}
	if v, err := strconv.ParseBool(p["value"]); err == nil {
		rt.SetFlag(p["flag"], v)
	}
	return nil
}

// sleepAction 睡眠指定时长，用于制造超时/取消。参数: duration
type sleepAction struct{}

func (sleepAction) Name() string { return "sleep" }
func (sleepAction) Execute(ctx context.Context, rt Runtime, p map[string]string) error {
	d, err := time.ParseDuration(p["duration"])
	if err != nil {
		return &FailureError{Kind: KindFailed, Detail: err.Error()}
	}
	if err := rt.Sleep(ctx, d); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return &FailureError{Kind: KindTimeout, Detail: "sleep exceeded deadline"}
		}
		return ctx.Err()
	}
	if k := p["flag"]; k != "" {
		rt.SetFlag(k, true)
	}
	return nil
}

// incMetricAction 调整计数器。
type incMetricAction struct{}

func (incMetricAction) Name() string { return "inc-metric" }
func (incMetricAction) Execute(ctx context.Context, rt Runtime, p map[string]string) error {
	d, _ := strconv.Atoi(p["delta"])
	rt.AddMetric(p["key"], d)
	return nil
}

// RegisterBuiltins 注册内置的本地模拟动作。
func RegisterBuiltins() {
	Register("set-flag", func() Action { return setFlagAction{name: "set-flag"} })
	Register("clear-flag", func() Action {
		return setFlagAction{name: "clear-flag"}
	})
	Register("unreachable", func() Action { return unreachableAction{} })
	Register("flaky", func() Action { return flakyAction{} })
	Register("sleep", func() Action { return sleepAction{} })
	Register("inc-metric", func() Action { return incMetricAction{} })
}
