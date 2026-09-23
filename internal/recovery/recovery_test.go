package recovery

import (
	"strconv"
	"testing"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
)

func mkSt(status model.ExperimentStatus, policy model.ResumePolicy, stepStatuses ...model.StepStatus) *model.ExperimentState {
	st := &model.ExperimentState{
		ID:     "exp",
		Status: status,
		Spec:   model.ExperimentSpec{ID: "exp", ResumePolicy: policy},
	}
	for i, ss := range stepStatuses {
		st.Steps = append(st.Steps, &model.StepState{
			ID:     model.StepID(strconv.Itoa(i)),
			Target: "t",
			Phase:  model.PhaseInject,
			Status: ss,
		})
	}
	return st
}

func TestDecide(t *testing.T) {
	cases := []struct {
		name       string
		st         *model.ExperimentState
		decision   Decision
		resetCount int
	}{
		{"terminal succeeded -> none",
			mkSt(model.StatusSucceeded, model.ResumeResume), DecisionNone, 0},
		{"terminal failed -> none",
			mkSt(model.StatusFailed, model.ResumeResume), DecisionNone, 0},
		{"terminal aborted -> none",
			mkSt(model.StatusAborted, model.ResumeAbort), DecisionNone, 0},
		{"paused + resume policy -> continue, no running steps",
			mkSt(model.StatusPaused, model.ResumeResume, model.StepSucceeded, model.StepPending),
			DecisionContinue, 0},
		{"paused + abort policy -> abort",
			mkSt(model.StatusPaused, model.ResumeAbort), DecisionAbort, 0},
		{"crashed running + resume -> continue and reset running steps",
			mkSt(model.StatusRunning, model.ResumeResume,
				model.StepSucceeded, model.StepRunning, model.StepRunning),
			DecisionContinue, 2},
		{"crashed running + abort -> abort and reset running steps",
			mkSt(model.StatusRunning, model.ResumeAbort, model.StepRunning),
			DecisionAbort, 1},
		{"never started pending + resume -> continue",
			mkSt(model.StatusPending, model.ResumeResume), DecisionContinue, 0},
		{"never started pending + abort -> abort",
			mkSt(model.StatusPending, model.ResumeAbort), DecisionAbort, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := Decide(tc.st)
			if p.Decision != tc.decision {
				t.Fatalf("decision=%s want %s (reason=%q)", p.Decision, tc.decision, p.Reason)
			}
			if len(p.ResetSteps) != tc.resetCount {
				t.Fatalf("reset=%v want %d steps", p.ResetSteps, tc.resetCount)
			}
		})
	}
}
