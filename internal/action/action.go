package action

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
)

// Runtime 提供动作执行时可访问的本地受控环境。
type Runtime interface {
	Flag(key string) bool
	SetFlag(key string, v bool)
	Metric(key string) int
	AddMetric(key string, delta int)
	Now() time.Time
	Sleep(ctx context.Context, d time.Duration) error
}

// Action 是故障注入 / 清理 / 自愈动作的统一接口。
type Action interface {
	Name() string
	Execute(ctx context.Context, rt Runtime, params map[string]string) error
}

// Factory 按 kind 构造动作。
type Factory func() Action

// 明确的失败类别，编排层据此映射为可区分的 model.FailureReason。
const (
	KindUnreachable = "unreachable"
	KindTimeout     = "timeout"
	KindFailed      = "failed"
)

var (
	ErrActionTimeout     = errors.New("action timeout")
	ErrTargetUnreachable = errors.New("target unreachable")
)

// FailureError 用于动作给出明确的失败类别。
type FailureError struct {
	Kind   string
	Detail string
}

func (e *FailureError) Error() string {
	if e.Detail == "" {
		return "action failure: " + e.Kind
	}
	return fmt.Sprintf("action failure: %s: %s", e.Kind, e.Detail)
}

// Unwrap 让超时/不可达错误可被 errors.Is 识别。
func (e *FailureError) Unwrap() error {
	switch e.Kind {
	case KindTimeout:
		return ErrActionTimeout
	case KindUnreachable:
		return ErrTargetUnreachable
	default:
		return nil
	}
}

var (
	registryMu sync.RWMutex
	registry   = map[string]Factory{}
)

// Register 注册一个动作工厂。
func Register(kind string, f Factory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[kind] = f
}

// Lookup 按 kind 查找动作工厂。
func Lookup(kind string) (Factory, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	f, ok := registry[kind]
	return f, ok
}

// Build 按 ActionSpec 构造动作实例。
func Build(spec model.ActionSpec) (Action, error) {
	f, ok := Lookup(spec.Kind)
	if !ok {
		return nil, fmt.Errorf("unknown action kind %q", spec.Kind)
	}
	return f(), nil
}

// ClassifyInject 把注入阶段的错误映射为可区分的失败原因。
func ClassifyInject(err error) model.FailureReason {
	switch {
	case errors.Is(err, ErrTargetUnreachable):
		return model.ReasonTargetUnreachable
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, ErrActionTimeout):
		return model.ReasonInjectTimeout
	default:
		return model.ReasonInjectFailed
	}
}

// ClassifyHeal 把自愈阶段的错误映射为可区分的失败原因。
func ClassifyHeal(err error) model.FailureReason {
	switch {
	case errors.Is(err, ErrTargetUnreachable):
		return model.ReasonTargetUnreachable
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, ErrActionTimeout):
		return model.ReasonHealTimeout
	default:
		return model.ReasonHealFailed
	}
}

// ClassifyCleanup 把清理阶段的错误映射为可区分的失败原因。
func ClassifyCleanup(err error) model.FailureReason {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, ErrActionTimeout):
		return model.ReasonCleanupTimeout
	default:
		return model.ReasonCleanupFailed
	}
}
