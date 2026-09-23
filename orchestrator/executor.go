package orchestrator

import (
	"context"
	"errors"
	"time"
)

// ReasonActionFailed 是执行器层面的通用动作失败；
// 引擎会根据动作种类（注入/自愈/清理）映射为具体的可区分原因。
const ReasonActionFailed FailureReason = "action_failed"

// ActionError 区分动作执行失败的原因：
// 目标不可达、动作超时、实验被取消、普通动作失败。
type ActionError struct {
	Reason      FailureReason
	Detail      string
	Timeout     bool
	Unreachable bool
}

func (e *ActionError) Error() string {
	if e == nil {
		return ""
	}
	if e.Reason == "" {
		return e.Detail
	}
	return string(e.Reason) + ": " + e.Detail
}

// AsActionError 从错误中提取 *ActionError（非动作错误返回 nil）。
func AsActionError(err error) *ActionError {
	var ae *ActionError
	if errors.As(err, &ae) {
		return ae
	}
	return nil
}

// Executor 负责具体执行故障注入/恢复/自愈动作与健康探测。
// 所有方法必须尊重 ctx 的取消与截止时间。
type Executor interface {
	// Reachable 在注入前确认目标可达；不可达必须返回 Unreachable 的 ActionError。
	Reachable(ctx context.Context, target, probeName string) error
	// Do 执行一个动作。超时必须映射为 ReasonActionTimeout，
	// 取消必须映射为 ReasonExperimentCanceled。
	Do(ctx context.Context, act ActionSpec) error
	// Probe 执行一次稳态探测并返回一个确定性样本。
	Probe(ctx context.Context, target string, p ProbeSpec) (Sample, error)
}

// Script 是模拟执行器对单个键（命令键或探测名）的脚本化行为定义。
type Script struct {
	Unreachable bool          // 健康检查始终不可达
	DoFail      bool          // Do 始终返回普通失败
	DoDelay     time.Duration // Do 前延迟（配合较短超时即可稳定复现超时）
	ProbeDelay  time.Duration // Probe 前延迟
	ProbeFail   bool          // Probe 始终失败
	// Values 仅探测脚本使用：metric -> 固定值，保证样本完全确定。
	Values map[string]float64
}

// SimExecutor 是完全本地、确定性的模拟执行器。
// 脚本以字符串键索引：动作用 ActionSpec.Command，探测/可达性用探测名。
type SimExecutor struct {
	scripts map[string]*Script
}

// NewSimExecutor 创建模拟执行器。
func NewSimExecutor(scripts map[string]*Script) *SimExecutor {
	return &SimExecutor{scripts: scripts}
}

func (e *SimExecutor) script(key string) *Script {
	if e.scripts == nil {
		return nil
	}
	return e.scripts[key]
}

// ctxError 把 context 结束原因映射为可区分的 ActionError。
func ctxError(prefix string, err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return &ActionError{Reason: ReasonActionTimeout, Timeout: true, Detail: prefix + " timed out"}
	case errors.Is(err, context.Canceled):
		return &ActionError{Reason: ReasonExperimentCanceled, Detail: prefix + " canceled"}
	default:
		return &ActionError{Reason: ReasonActionFailed, Detail: prefix + ": " + err.Error()}
	}
}

func (e *SimExecutor) Reachable(ctx context.Context, target, probeName string) error {
	if err := ctx.Err(); err != nil {
		return ctxError("reachability check("+probeName+")", err)
	}
	if s := e.script(probeName); s != nil && s.Unreachable {
		return &ActionError{
			Reason:      ReasonTargetUnreachable,
			Unreachable: true,
			Detail:      "target " + target + " unreachable via probe " + probeName,
		}
	}
	return nil
}

func (e *SimExecutor) Do(ctx context.Context, act ActionSpec) error {
	if err := ctx.Err(); err != nil {
		return ctxError("action "+act.Name, err)
	}
	s := e.script(act.Command)
	if s != nil && s.DoDelay > 0 {
		timer := time.NewTimer(s.DoDelay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctxError("action "+act.Name, ctx.Err())
		}
	}
	if s != nil && s.DoFail {
		return &ActionError{Reason: ReasonActionFailed, Detail: "action " + act.Name + " failed by script"}
	}
	return nil
}

func (e *SimExecutor) Probe(ctx context.Context, target string, p ProbeSpec) (Sample, error) {
	if err := ctx.Err(); err != nil {
		return Sample{}, ctxError("probe "+p.Name, err)
	}
	s := e.script(p.Name)
	if s != nil && s.ProbeDelay > 0 {
		timer := time.NewTimer(s.ProbeDelay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return Sample{}, ctxError("probe "+p.Name, ctx.Err())
		}
	}
	if s != nil && s.ProbeFail {
		return Sample{}, &ActionError{Reason: ReasonProbeError, Detail: "probe " + p.Name + " failed by script"}
	}
	values := map[string]float64{}
	if s != nil {
		for k, v := range s.Values {
			values[k] = v
		}
	}
	return Sample{Values: values}, nil
}
