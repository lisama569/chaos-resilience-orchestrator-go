package probe

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/action"
	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
)

// Probe 是稳态证据采集器：返回一个确定的布尔观测值。
type Probe interface {
	Name() string
	Collect(ctx context.Context, key string) (bool, error)
}

// Factory 在构建时接收运行环境与静态参数。
type Factory func(rt action.Runtime, params map[string]string) Probe

var (
	registryMu sync.RWMutex
	registry   = map[string]Factory{}
)

// Register 注册探针工厂。
func Register(kind string, f Factory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[kind] = f
}

// Lookup 按 kind 查找探针工厂。
func Lookup(kind string) (Factory, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	f, ok := registry[kind]
	return f, ok
}

// Build 按 ProbeSpec 在给定运行环境上构造探针实例。
func Build(spec model.ProbeSpec, rt action.Runtime) (Probe, error) {
	f, ok := Lookup(spec.Kind)
	if !ok {
		return nil, fmt.Errorf("unknown probe kind %q", spec.Kind)
	}
	return f(rt, spec.Params), nil
}

// ---- 内置探针 ----

type flagProbe struct{ rt action.Runtime }

func (p flagProbe) Name() string { return "flag" }
func (p flagProbe) Collect(ctx context.Context, key string) (bool, error) {
	return p.rt.Flag(key), nil
}

type thresholdProbe struct {
	rt        action.Runtime
	threshold int
}

func (p thresholdProbe) Name() string { return "metric-threshold" }
func (p thresholdProbe) Collect(ctx context.Context, key string) (bool, error) {
	return p.rt.Metric(key) >= p.threshold, nil
}

type errorProbe struct{}

func (errorProbe) Name() string { return "error" }
func (errorProbe) Collect(ctx context.Context, key string) (bool, error) {
	return false, fmt.Errorf("probe %q unreachable", key)
}

// RegisterBuiltins 注册内置探针（flag / metric-threshold / error）。
func RegisterBuiltins() {
	Register("flag", func(rt action.Runtime, _ map[string]string) Probe { return flagProbe{rt: rt} })
	Register("metric-threshold", func(rt action.Runtime, p map[string]string) Probe {
		n, _ := strconv.Atoi(p["threshold"])
		return thresholdProbe{rt: rt, threshold: n}
	})
	Register("error", func(action.Runtime, map[string]string) Probe { return errorProbe{} })
}
