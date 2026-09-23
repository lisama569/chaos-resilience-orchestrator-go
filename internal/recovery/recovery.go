package recovery

import (
	"fmt"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
)

// Decision 表示重启后对未完成实验的明确处置。
type Decision string

const (
	DecisionContinue Decision = "continue" // 续跑：已完成步骤保留，running 步骤重置后续跑
	DecisionAbort    Decision = "abort"    // 终止：不再继续实验，但清理已注入故障
	DecisionNone     Decision = "none"     // 已处终态，无需处理
)

// Plan 描述恢复决策及其依据，保证可解释。
type Plan struct {
	ExperimentID model.ExperimentID
	Decision     Decision
	Reason       string
	// ResetSteps 是续跑/中止前必须重置的 running 步骤：
	// 崩溃（进程被杀）发生在"动作执行中、结果未落盘"的窗口，running 不代表已完成，
	// 因此只能重置为 pending 重新判定，绝不允许被当作已完成步骤静默接受。
	ResetSteps []model.StepID
}

// Decide 依据实验状态与 resume 策略给出明确处置：
//
//	succeeded/failed/aborted（终态）         -> none，不重复执行
//	paused（优雅中断，ctx 已取消、状态已落盘） -> 按 resume_policy：resume 续跑，abort 清理后终止
//	running（非正常退出/进程崩溃遗留）        -> 按 resume_policy 同上，但必须先重置 running 步骤
//	pending（创建后从未启动）                 -> 按 resume_policy：resume 启动，abort 直接终止
func Decide(st *model.ExperimentState) Plan {
	plan := Plan{ExperimentID: st.ID}

	for _, step := range st.Steps {
		if step.Status == model.StepRunning {
			plan.ResetSteps = append(plan.ResetSteps, step.ID)
		}
	}

	switch {
	case st.Status.IsTerminal():
		plan.Decision = DecisionNone
		plan.Reason = fmt.Sprintf("experiment already terminal: %s", st.Status)
		return plan
	case st.Spec.ResumePolicy == model.ResumeAbort:
		plan.Decision = DecisionAbort
		plan.Reason = fmt.Sprintf("resume_policy=abort, status=%s", st.Status)
	default:
		plan.Decision = DecisionContinue
		plan.Reason = fmt.Sprintf("resume_policy=resume, status=%s", st.Status)
	}
	return plan
}
