package policy

import (
	"log"
	"os"
	"testing"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
)

func mkTarget(rules ...model.ProbeSpec) model.TargetSpec {
	return model.TargetSpec{ID: "t1", Probes: rules}
}

func ev(name string, values ...bool) []model.Evidence {
	var out []model.Evidence
	for i, v := range values {
		out = append(out, model.Evidence{Probe: name, Round: i + 1, Value: v})
	}
	return out
}

func TestEvaluate(t *testing.T) {
	rule := model.ProbeSpec{Name: "p", Want: true, MinHits: 2}

	cases := []struct {
		name     string
		spec     model.TargetSpec
		evidence []model.Evidence
		want     model.Conclusion
		reason   model.FailureReason
		healthy  bool
	}{
		{
			name:     "all rules satisfied -> steady",
			spec:     mkTarget(rule),
			evidence: ev("p", true, true),
			want:     model.ConclusionSteady, healthy: true,
		},
		{
			name:     "value mismatch -> not steady (not default pass)",
			spec:     mkTarget(rule),
			evidence: ev("p", false, false),
			want:     model.ConclusionNotSteady,
		},
		{
			name:     "hits below threshold -> not steady",
			spec:     mkTarget(rule),
			evidence: ev("p", true),
			want:     model.ConclusionNotSteady,
		},
		{
			name:     "no evidence at all -> inconclusive/insufficient, never default pass",
			spec:     mkTarget(rule),
			evidence: nil,
			want:     model.ConclusionInconclusive,
			reason:   model.ReasonProbeInsufficient,
		},
		{
			name:     "contradictory observations -> inconclusive/contradictory",
			spec:     mkTarget(rule),
			evidence: ev("p", true, false),
			want:     model.ConclusionInconclusive,
			reason:   model.ReasonProbeContradictory,
		},
		{
			name: "one rule satisfied, one missing -> insufficient wins",
			spec: mkTarget(
				rule,
				model.ProbeSpec{Name: "q", Want: true, MinHits: 1},
			),
			evidence: append(ev("p", true, true), ev("q")...),
			want:     model.ConclusionInconclusive,
			reason:   model.ReasonProbeInsufficient,
		},
	}

	logger := log.New(os.Stdout, "[policy-test] ", log.LstdFlags)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger.Printf("INPUT target=%s rules=%d evidence=%v", tc.spec.ID, len(tc.spec.Probes), tc.evidence)
			v := New(tc.spec).Evaluate(tc.evidence)
			for _, b := range v.Basis {
				logger.Printf("BASIS %s", b)
			}
			logger.Printf("VERDICT outcome=%s healthy=%v reason=%s", v.Outcome, v.Healthy, v.Reason)
			if v.Outcome != tc.want {
				t.Fatalf("outcome = %s, want %s", v.Outcome, tc.want)
			}
			if v.Healthy != tc.healthy {
				t.Fatalf("healthy = %v, want %v", v.Healthy, tc.healthy)
			}
			if tc.reason != model.ReasonNone && v.Reason != tc.reason {
				t.Fatalf("reason = %s, want %s", v.Reason, tc.reason)
			}
		})
	}
}

// TestDeterministicBasisOrder 保证依据输出顺序确定、可复现。
func TestDeterministicBasisOrder(t *testing.T) {
	spec := mkTarget(
		model.ProbeSpec{Name: "z", Want: true, MinHits: 1},
		model.ProbeSpec{Name: "a", Want: true, MinHits: 1},
		model.ProbeSpec{Name: "m", Want: true, MinHits: 1},
	)
	evidence := append(append(ev("z", true), ev("a", true)...), ev("m", true)...)
	v := New(spec).Evaluate(evidence)
	wantPrefix := []string{"rule=a", "rule=m", "rule=z"}
	for i, p := range wantPrefix {
		if len(v.Basis) <= i || v.Basis[i][:len(p)] != p {
			t.Fatalf("basis[%d]=%q, want prefix %q; full=%v", i, v.Basis[i], p, v.Basis)
		}
	}
}
