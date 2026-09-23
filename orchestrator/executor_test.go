package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSimReachableDistinguishesUnreachable(t *testing.T) {
	exec := NewSimExecutor(map[string]*Script{"p": {Unreachable: true}})
	err := exec.Reachable(context.Background(), "svc", "p")
	ae := AsActionError(err)
	if ae == nil || !ae.Unreachable || ae.Reason != ReasonTargetUnreachable {
		t.Fatalf("want target_unreachable ActionError, got %v", err)
	}
	t.Logf("unreachable classified: reason=%s detail=%s", ae.Reason, ae.Detail)
}

func TestSimDoTimeoutClassified(t *testing.T) {
	exec := NewSimExecutor(map[string]*Script{"slow": {DoDelay: 200 * time.Millisecond}})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := exec.Do(ctx, ActionSpec{Name: "a", Command: "slow"})
	ae := AsActionError(err)
	if ae == nil || !ae.Timeout || ae.Reason != ReasonActionTimeout {
		t.Fatalf("want action_timeout, got %v", err)
	}
	t.Logf("timeout classified: reason=%s", ae.Reason)
}

func TestSimDoCancelClassified(t *testing.T) {
	exec := NewSimExecutor(map[string]*Script{"slow": {DoDelay: 500 * time.Millisecond}})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	err := exec.Do(ctx, ActionSpec{Name: "a", Command: "slow"})
	ae := AsActionError(err)
	if ae == nil || ae.Reason != ReasonExperimentCanceled {
		t.Fatalf("want experiment_canceled, got %v", err)
	}
	t.Logf("cancel classified: reason=%s", ae.Reason)
}

func TestSimDoFailAndDeterministicProbe(t *testing.T) {
	exec := NewSimExecutor(map[string]*Script{
		"boom":  {DoFail: true},
		"probe": {Values: map[string]float64{"err": 0.42}},
	})
	if err := exec.Do(context.Background(), ActionSpec{Name: "a", Command: "boom"}); err == nil {
		t.Fatal("want scripted failure")
	} else if ae := AsActionError(err); ae == nil || ae.Reason != ReasonActionFailed {
		t.Fatalf("want action_failed, got %v", err)
	}
	s1, err := exec.Probe(context.Background(), "svc", ProbeSpec{Name: "probe"})
	if err != nil {
		t.Fatal(err)
	}
	s2, _ := exec.Probe(context.Background(), "svc", ProbeSpec{Name: "probe"})
	if s1.Values["err"] != 0.42 || s2.Values["err"] != 0.42 {
		t.Fatalf("probe samples must be deterministic, got %v %v", s1.Values, s2.Values)
	}
	t.Logf("deterministic sample err=%v across calls", s1.Values["err"])
}

func TestSimProbeFailClassified(t *testing.T) {
	exec := NewSimExecutor(map[string]*Script{"p": {ProbeFail: true}})
	_, err := exec.Probe(context.Background(), "svc", ProbeSpec{Name: "p"})
	ae := AsActionError(err)
	if ae == nil || ae.Reason != ReasonProbeError {
		t.Fatalf("want probe_error, got %v", err)
	}
	if AsActionError(nil) != nil {
		t.Fatal("nil error must map to nil ActionError")
	}
	if !errors.Is(err, err) {
		t.Fatal("sanity")
	}
}
