package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
)

// commitMu 串行化本进程内的所有状态提交。
var stepCommitMu sync.Mutex

// initSteps 按计划为每个目标预先生成全部确定性步骤：
// reach(可选)、inject、cleanup、每轮 observe，以及每次自愈尝试与自愈后的重新观测。
// 步骤 ID 完全由 (target, phase, attempt, round) 决定，重启后可稳定复现。
func initSteps(spec model.ExperimentSpec) []*model.StepState {
	var steps []*model.StepState
	add := func(t model.TargetID, p model.Phase, attempt, round int) {
		steps = append(steps, &model.StepState{
			ID:      model.MakeStepID(t, p, attempt, round),
			Target:  t,
			Phase:   p,
			Attempt: attempt,
			Round:   round,
			Status:  model.StepPending,
		})
	}
	for _, t := range spec.Targets {
		if t.Reach != nil {
			add(t.ID, model.PhaseReachability, 0, 0)
		}
		add(t.ID, model.PhaseInject, 0, 0)
		maxHeal := 0
		if t.Heal != nil {
			maxHeal = t.Heal.MaxAttempts
		}
		// 初始观测 + 每次自愈调用之后的重新观测。
		add(t.ID, model.PhaseObserve, 0, 0)
		for attempt := 1; attempt <= maxHeal; attempt++ {
			add(t.ID, model.PhaseHeal, attempt, 0)
			add(t.ID, model.PhaseObserve, attempt, 0)
		}
		// 清理带有限重试：attempt 0..maxCleanupAttempts-1，成功后其余标记 skipped。
		for a := 0; a < maxCleanupAttempts; a++ {
			add(t.ID, model.PhaseCleanup, a, 0)
		}
	}
	return steps
}

// updateState 串行化地读取最新状态、施加变更并 CAS 提交。
// 并发目标各自只修改自己的步骤，但共享同一份实验文档，因此必须串行提交。
// mutate 返回的错误（如重复上报、非法迁移）会被原样传播，且不会写入任何状态。
func (e *Engine) updateState(ctx context.Context, mutate func(*model.ExperimentState) error) (*model.ExperimentState, error) {
	stepCommitMu.Lock()
	defer stepCommitMu.Unlock()

	cur, err := e.store.Load(ctx, e.experimentID)
	if err != nil {
		return nil, err
	}
	expected := cur.Version
	if err := mutate(cur); err != nil {
		return nil, err
	}
	cur.Version = expected + 1
	cur.UpdatedAt = e.rt.Now()
	return e.store.Commit(ctx, cur, expected)
}

// startStep 将 pending 步骤标记为 running。
// 若步骤已是终态，返回 terminal=true：调用方必须跳过，绝不允许重复执行已确认完成的步骤。
func (e *Engine) startStep(ctx context.Context, key model.StepKey) (st *model.ExperimentState, terminal bool, err error) {
	terminal = false
	st, err = e.updateState(ctx, func(s *model.ExperimentState) error {
		step := model.FindStep(s.Steps, key)
		if step == nil {
			return fmt.Errorf("internal: step %v not found", key)
		}
		if step.Status.IsTerminal() {
			e.log.Printf("step already terminal, skip re-execution: id=%s status=%s", step.ID, step.Status)
			terminal = true
			return nil
		}
		if err := model.CheckStepTransition(step.Status, model.StepRunning); err != nil {
			return err
		}
		step.Status = model.StepRunning
		now := e.rt.Now()
		step.StartedAt = &now
		return nil
	})
	return st, terminal, err
}

// finishStep 把 running 步骤落到终态；重复上报相同终态会被显式拒绝（ErrDuplicateReport）。
func (e *Engine) finishStep(ctx context.Context, key model.StepKey,
	status model.StepStatus, reason model.FailureReason, detail string) (st *model.ExperimentState, err error) {
	st, err = e.updateState(ctx, func(s *model.ExperimentState) error {
		step := model.FindStep(s.Steps, key)
		if step == nil {
			return fmt.Errorf("internal: step %v not found", key)
		}
		if step.Status.IsTerminal() {
			return fmt.Errorf("%w: step=%s status=%s report=%s",
				model.ErrDuplicateReport, step.ID, step.Status, status)
		}
		if e2 := model.CheckStepTransition(step.Status, status); e2 != nil {
			return fmt.Errorf("step=%s: %w", step.ID, e2)
		}
		step.Status = status
		step.Reason = reason
		step.Detail = detail
		now := e.rt.Now()
		step.FinishedAt = &now
		return nil
	})
	return st, err
}

// skipRemaining 把目标尚未开始的 pending 步骤标记 skipped（取消/前置失败时使用）。
// running 步骤一律不动（结果未知，交给恢复规则重置）；
// 清理步骤永不跳过：只要注入可能生效，就必须保留 cleanup 步骤以保证最终清理。
func (e *Engine) skipRemaining(ctx context.Context, target model.TargetID, reason model.FailureReason) (*model.ExperimentState, error) {
	return e.updateState(ctx, func(s *model.ExperimentState) error {
		now := e.rt.Now()
		for _, step := range s.Steps {
			if step.Target != target || step.Status.IsTerminal() {
				continue
			}
			if step.Status != model.StepPending || step.Phase == model.PhaseCleanup {
				continue
			}
			step.Status = model.StepSkipped
			if step.Reason == model.ReasonNone {
				step.Reason = reason
			}
			step.FinishedAt = &now
		}
		return nil
	})
}

// markResult 持久化单个目标的最终结果（用于崩溃恢复时识别注入是否已生效）。
func (e *Engine) markResult(ctx context.Context, r *model.TargetResult) (*model.ExperimentState, error) {
	return e.updateState(ctx, func(s *model.ExperimentState) error {
		cp := *r
		if s.Results == nil {
			s.Results = map[model.TargetID]*model.TargetResult{}
		}
		s.Results[r.ID] = &cp
		return nil
	})
}

// resetRunningSteps 供恢复路径使用：把所有 running 步骤重置为 pending。
func (e *Engine) resetRunningSteps(ctx context.Context) (*model.ExperimentState, error) {
	return e.updateState(ctx, func(s *model.ExperimentState) error {
		for _, step := range s.Steps {
			if step.Status == model.StepRunning {
				step.Status = model.StepPending
				step.StartedAt = nil
				step.FinishedAt = nil
				step.Reason = ""
				step.Detail = ""
			}
		}
		return nil
	})
}

// skipStep 把指定步骤标记 skipped；已是终态的步骤保持不变（幂等）。
func (e *Engine) skipStep(ctx context.Context, key model.StepKey, reason model.FailureReason) (*model.ExperimentState, error) {
	return e.updateState(ctx, func(s *model.ExperimentState) error {
		step := model.FindStep(s.Steps, key)
		if step == nil || step.Status.IsTerminal() {
			return nil
		}
		now := e.rt.Now()
		if step.Status == model.StepRunning {
			step.FinishedAt = &now
		}
		step.Status = model.StepSkipped
		if reason != model.ReasonNone {
			step.Reason = reason
		}
		return nil
	})
}

// stepStatus 读取某个步骤的当前持久化状态。
func (e *Engine) stepStatus(ctx context.Context, key model.StepKey) model.StepStatus {
	cur, err := e.store.Load(ctx, e.experimentID)
	if err != nil {
		return model.StepPending
	}
	step := model.FindStep(cur.Steps, key)
	if step == nil {
		return model.StepPending
	}
	return step.Status
}

// phaseOrder 定义单目标流水线的阶段顺序。
var phaseOrder = map[model.Phase]int{
	model.PhaseReachability: 0,
	model.PhaseInject:       1,
	model.PhaseObserve:      2,
	model.PhaseHeal:         3,
	model.PhaseCleanup:      4,
}

// skipFrom 跳过指定阶段及之后（除调用方另行说明外）的全部未终态步骤。
// 注入未生效时：observe/heal/cleanup 全部跳过。
func (e *Engine) skipFrom(ctx context.Context, target model.TargetID, from model.Phase, reason model.FailureReason) (*model.ExperimentState, error) {
	return e.updateState(ctx, func(s *model.ExperimentState) error {
		now := e.rt.Now()
		for _, step := range s.Steps {
			if step.Target != target || step.Status != model.StepPending {
				continue
			}
			if phaseOrder[step.Phase] >= phaseOrder[from] {
				step.Status = model.StepSkipped
				if step.Reason == model.ReasonNone {
					step.Reason = reason
				}
				step.FinishedAt = &now
			}
		}
		return nil
	})
}

// skipAllBut 跳过除指定阶段外该目标的全部未终态步骤（不可达时使用）。
func (e *Engine) skipAllBut(ctx context.Context, target model.TargetID, keep model.Phase, reason model.FailureReason) (*model.ExperimentState, error) {
	return e.updateState(ctx, func(s *model.ExperimentState) error {
		now := e.rt.Now()
		for _, step := range s.Steps {
			if step.Target != target || step.Status != model.StepPending || step.Phase == keep {
				continue
			}
			step.Status = model.StepSkipped
			if step.Reason == model.ReasonNone {
				step.Reason = reason
			}
			step.FinishedAt = &now
		}
		return nil
	})
}

// mergeResult 把阶段性信息（证据等）合并进已持久化的目标结果，
// 不覆盖最终判定字段；供观测过程中即时落盘证据使用。
func (e *Engine) mergeResult(ctx context.Context, r *model.TargetResult) (*model.ExperimentState, error) {
	return e.updateState(ctx, func(s *model.ExperimentState) error {
		if s.Results == nil {
			s.Results = map[model.TargetID]*model.TargetResult{}
		}
		cur := s.Results[r.ID]
		if cur == nil {
			cp := *r
			s.Results[r.ID] = &cp
			return nil
		}
		cur.Evidence = dedupEvidence(append(cur.Evidence, r.Evidence...))
		return nil
	})
}
