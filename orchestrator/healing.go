package orchestrator

import "errors"

// HealOutcome 是自愈联动的最终结局。
type HealOutcome string

const (
	HealConverged    HealOutcome = "converged"     // 在有限轮次内复核恢复稳态
	HealNotConverged HealOutcome = "not_converged" // 达到轮次上限仍判定违例
	// HealInconclusive 复核时依据不足或相互矛盾：不得默认收敛，明确中止。
	HealInconclusive HealOutcome = "inconclusive"
	HealActionFailed HealOutcome = "action_failed" // 自愈动作执行失败，明确中止
	HealCanceled     HealOutcome = "canceled"      // 被取消
)

// ErrHealMisconfigured 表示自愈策略缺少必要配置或轮次上限非法。
var ErrHealMisconfigured = errors.New("heal policy misconfigured")

// ProgressSink 是控制器向引擎推进状态的回调接口，
// 使自愈逻辑不直接依赖持久化细节。
type ProgressSink interface {
	// FireHealAction 请求执行一轮自愈动作。
	// 若该轮动作已确认执行过（典型场景：中断恢复），实现必须跳过执行并
	// 返回 fired=false，而不是静默重复触发；同一轮内的直接重复请求应返回错误。
	FireHealAction(round int, act ActionSpec) (fired bool, err error)
	// Recheck 在一次自愈动作后重新采集证据并做确定性判定。
	Recheck(round int) (*Evaluation, error)
}

// Healer 驱动“自愈 -> 复核 -> 再自愈”的有限循环。
type Healer struct{}

// NewHealer 创建自愈控制器。
func NewHealer() *Healer { return &Healer{} }

// Run 执行自愈联动直到收敛、达到上限、动作失败、依据不可判定或被取消。
//
// roundsDone 用于中断恢复：已完整完成（含复核）的轮次不会重跑；
// 已触发动作但未完成复核的轮次不会重复触发动作，只补做复核。
//
// 每轮最多触发一次自愈动作，跨轮重试受 MaxRounds 硬上限约束，
// 因此不可能无限重试，也不可能在同一轮内对同一动作重复触发。
func (h *Healer) Run(policy *HealPolicy, roundsDone int, sink ProgressSink) (outcome HealOutcome, rounds int, err error) {
	if policy == nil || policy.RecoverAction == nil {
		return HealActionFailed, roundsDone, ErrHealMisconfigured
	}
	if policy.MaxRounds < 1 {
		return HealActionFailed, roundsDone, ErrHealMisconfigured
	}
	if sink == nil {
		return HealActionFailed, roundsDone, errors.New("nil progress sink")
	}

	rounds = roundsDone
	for round := roundsDone; round < policy.MaxRounds; round++ {
		fired, ferr := sink.FireHealAction(round, *policy.RecoverAction)
		if ferr != nil {
			if ae := AsActionError(ferr); ae != nil && ae.Reason == ReasonExperimentCanceled {
				return HealCanceled, rounds, ferr
			}
			return HealActionFailed, rounds, ferr
		}
		_ = fired // 恢复场景下可能为 false（动作此前已触发），仍需补做复核

		ev, rerr := sink.Recheck(round)
		if rerr != nil {
			if ae := AsActionError(rerr); ae != nil && ae.Reason == ReasonExperimentCanceled {
				return HealCanceled, rounds, rerr
			}
			// 复核本身无法执行（而非得出依据不足结论），按动作失败中止。
			return HealActionFailed, rounds, rerr
		}
		rounds = round + 1

		switch ev.Verdict {
		case VerdictSteady:
			return HealConverged, rounds, nil
		case VerdictViolated:
			// 继续下一轮（若已是最后一轮，循环结束后判未收敛）。
			continue
		case VerdictInconclusive, VerdictContradictory:
			// 依据不足或矛盾：禁止默认收敛，明确中止，原因由调用方区分。
			return HealInconclusive, rounds, nil
		default:
			return HealInconclusive, rounds, errors.New("unknown verdict: " + string(ev.Verdict))
		}
	}
	return HealNotConverged, rounds, nil
}
