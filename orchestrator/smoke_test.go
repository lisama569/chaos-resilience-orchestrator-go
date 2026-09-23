package orchestrator

import (
	"context"
	"testing"
	"time"
)

func smokePlan() *Plan {
	return &Plan{
		ID:          "p1",
		Concurrency: 2,
		RunTimeout:  10 * time.Second,
		Targets: []TargetSpec{
			{
				ID:          "t1",
				Target:      "svc-a",
				ReachableBy: "reach",
				Inject:      ActionSpec{Name: "inject-latency", Kind: ActionInject, Target: "svc-a", Command: "inject", Timeout: time.Second},
				Recover:     ActionSpec{Name: "recover", Kind: ActionRecover, Target: "svc-a", Command: "recover", Timeout: time.Second},
				Probes:      []ProbeSpec{{Name: "health", MinSamples: 1}},
				Rules:       []RuleSpec{{Probe: "health", Metric: "error_rate", Operator: "<", Threshold: 0.5, MaxBreaches: 0}},
			},
		},
	}
}

func TestSmokeSteady(t *testing.T) {
	exec := NewSimExecutor(map[string]*Script{
		"health": {Values: map[string]float64{"error_rate": 0.01}},
	})
	eng := NewEngine(exec, NewMemoryStore(), WithLogger(t.Logf))
	rep, err := eng.Run(context.Background(), smokePlan())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	r := rep.Results[0]
	t.Logf("phase=%s verdict=%s failure=%s cleanup=%s", r.Phase, r.Verdict, r.Failure, r.Cleanup)
	if r.Phase != PhaseCompleted || r.Verdict != VerdictSteady || r.Cleanup != CleanupSucceeded {
		t.Fatalf("unexpected: %+v", r)
	}
}
