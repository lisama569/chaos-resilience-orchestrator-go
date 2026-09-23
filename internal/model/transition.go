package model

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidTransition = errors.New("invalid state transition")
	ErrDuplicateReport   = errors.New("duplicate step report rejected")
	ErrStateRollback     = errors.New("state rollback rejected")
	ErrVersionConflict   = errors.New("version conflict")
)

// StepKey 唯一标识"某目标某阶段第 attempt 次尝试的第 round 轮"，用于重复上报检测。
// 首轮观测 attempt=0；第 n 次自愈后的重新观测 attempt=n。
type StepKey struct {
	Target  TargetID
	Phase   Phase
	Attempt int
	Round   int
}

// Key 返回步骤的去重键。
func (s *StepState) Key() StepKey {
	return StepKey{s.Target, s.Phase, s.Attempt, s.Round}
}

// MakeStepID 生成确定性步骤 ID，保证重启后同一可重试步骤仍能被识别。
func MakeStepID(t TargetID, p Phase, attempt, round int) StepID {
	return StepID(fmt.Sprintf("%s-%s-a%d-r%d", t, p, attempt, round))
}

// FindStep 在步骤列表中按去重键查找步骤。
func FindStep(steps []*StepState, k StepKey) *StepState {
	for _, s := range steps {
		if s.Key() == k {
			return s
		}
	}
	return nil
}

// stepTransitions 定义步骤状态机：终态（succeeded/failed/skipped）不可回退，
// 同一终态的重复迁移必须由调用方在持久化层显式拒绝（ErrDuplicateReport）。
var stepTransitions = map[StepStatus]map[StepStatus]bool{
	StepPending: {StepRunning: true, StepSkipped: true},
	StepRunning: {StepSucceeded: true, StepFailed: true},
}

// CanTransitionStep 判定步骤状态是否允许迁移。
func CanTransitionStep(from, to StepStatus) bool {
	if from == to {
		return false // 重复上报相同状态不是合法迁移
	}
	return stepTransitions[from][to]
}

var experimentTransitions = map[ExperimentStatus]map[ExperimentStatus]bool{
	StatusPending: {StatusRunning: true, StatusAborted: true},
	StatusRunning: {StatusPaused: true, StatusSucceeded: true, StatusFailed: true, StatusAborted: true},
	StatusPaused:  {StatusRunning: true, StatusAborted: true},
}

// CanTransitionExperiment 判定实验状态是否允许迁移。
// succeeded/failed/aborted 为终态，任何回退（包括 paused->failed 之类的越级）都被拒绝。
func CanTransitionExperiment(from, to ExperimentStatus) bool {
	if from == to {
		return false
	}
	return experimentTransitions[from][to]
}

// IsTerminal 判断实验是否已处于终态。
func (s ExperimentStatus) IsTerminal() bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusAborted
}

// IsTerminal 判断步骤是否已处于终态。
func (s StepStatus) IsTerminal() bool {
	return s == StepSucceeded || s == StepFailed || s == StepSkipped
}

// CheckStepTransition 校验步骤迁移，回退返回 ErrStateRollback，
// 非法迁移返回 ErrInvalidTransition。
func CheckStepTransition(from, to StepStatus) error {
	if from.IsTerminal() {
		return fmt.Errorf("%w: %s -> %s", ErrStateRollback, from, to)
	}
	if !CanTransitionStep(from, to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
	}
	return nil
}

// CheckExperimentTransition 校验实验状态迁移。
func CheckExperimentTransition(from, to ExperimentStatus) error {
	if from.IsTerminal() {
		return fmt.Errorf("%w: %s -> %s", ErrStateRollback, from, to)
	}
	if !CanTransitionExperiment(from, to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
	}
	return nil
}
