package model

import (
	"errors"
	"testing"
)

func TestStepTransitions(t *testing.T) {
	cases := []struct {
		from, to StepStatus
		ok       bool
	}{
		{StepPending, StepRunning, true},
		{StepRunning, StepSucceeded, true},
		{StepRunning, StepFailed, true},
		{StepPending, StepSkipped, true},
		{StepSucceeded, StepRunning, false},   // 终态回退
		{StepFailed, StepSucceeded, false},    // 终态改写
		{StepSucceeded, StepSucceeded, false}, // 重复上报
		{StepPending, StepSucceeded, false},   // 越级
	}
	for _, c := range cases {
		if got := CanTransitionStep(c.from, c.to); got != c.ok {
			t.Errorf("CanTransitionStep(%s,%s)=%v want %v", c.from, c.to, got, c.ok)
		}
	}

	err := CheckStepTransition(StepSucceeded, StepRunning)
	if !errors.Is(err, ErrStateRollback) {
		t.Fatalf("terminal rollback must be ErrStateRollback, got %v", err)
	}
	err = CheckStepTransition(StepPending, StepSucceeded)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("illegal transition must be ErrInvalidTransition, got %v", err)
	}
}

func TestExperimentTransitions(t *testing.T) {
	ok := [][2]ExperimentStatus{
		{StatusPending, StatusRunning},
		{StatusRunning, StatusPaused},
		{StatusRunning, StatusSucceeded},
		{StatusRunning, StatusFailed},
		{StatusRunning, StatusAborted},
		{StatusPaused, StatusRunning},
		{StatusPending, StatusAborted},
	}
	for _, c := range ok {
		if !CanTransitionExperiment(c[0], c[1]) {
			t.Errorf("expected allowed %s->%s", c[0], c[1])
		}
	}
	bad := [][2]ExperimentStatus{
		{StatusSucceeded, StatusRunning},
		{StatusFailed, StatusRunning},
		{StatusAborted, StatusRunning},
		{StatusPaused, StatusSucceeded}, // 越级
		{StatusRunning, StatusRunning},
	}
	for _, c := range bad {
		if CanTransitionExperiment(c[0], c[1]) {
			t.Errorf("expected rejected %s->%s", c[0], c[1])
		}
	}
}

func TestStepKeyDeterministic(t *testing.T) {
	s1 := &StepState{Target: "t", Phase: PhaseHeal, Attempt: 2, Round: 1}
	s2 := &StepState{Target: "t", Phase: PhaseHeal, Attempt: 2, Round: 1}
	s3 := &StepState{Target: "t", Phase: PhaseHeal, Attempt: 1, Round: 1}
	if s1.Key() != s2.Key() {
		t.Fatal("identical steps must have equal keys")
	}
	if s1.Key() == s3.Key() {
		t.Fatal("different attempt must produce different keys")
	}
	if got := MakeStepID("t", PhaseObserve, 1, 2); got != MakeStepID("t", PhaseObserve, 1, 2) {
		t.Fatal("step id must be deterministic")
	}
}
