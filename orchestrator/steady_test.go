package orchestrator

import (
	"errors"
	"testing"
)

func fixedSample(values map[string]map[string]float64) SampleFunc {
	return func(name string) (Sample, error) {
		if v, ok := values[name]; ok {
			return Sample{Values: v}, nil
		}
		return Sample{}, errors.New("no script for probe " + name)
	}
}

func evalProbes() []ProbeSpec {
	return []ProbeSpec{{Name: "p", MinSamples: 1}}
}

// 依据满足条件 -> steady；关键输入与依据都打印到测试日志。
func TestEvaluateSteady(t *testing.T) {
	ev := NewEvaluator(nil)
	rules := []RuleSpec{{Probe: "p", Metric: "err", Operator: "<=", Threshold: 0.05}}
	e, err := ev.Evaluate(0, evalProbes(), rules, fixedSample(map[string]map[string]float64{
		"p": {"err": 0.01},
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input err=0.01 rule<=0.05 => verdict=%s basis=%s", e.Verdict, e.BasisSummary)
	if e.Verdict != VerdictSteady {
		t.Fatalf("want steady, got %s", e.Verdict)
	}
}

func TestEvaluateViolated(t *testing.T) {
	ev := NewEvaluator(nil)
	rules := []RuleSpec{{Probe: "p", Metric: "err", Operator: "<=", Threshold: 0.05}}
	e, err := ev.Evaluate(0, evalProbes(), rules, fixedSample(map[string]map[string]float64{
		"p": {"err": 0.9},
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input err=0.9 rule<=0.05 => verdict=%s basis=%s", e.Verdict, e.BasisSummary)
	if e.Verdict != VerdictViolated {
		t.Fatalf("want violated, got %s", e.Verdict)
	}
}

// 探测异常 -> 依据不足，不得默认通过。
func TestEvaluateInconclusiveOnProbeError(t *testing.T) {
	ev := NewEvaluator(nil)
	rules := []RuleSpec{{Probe: "p", Metric: "err", Operator: "<=", Threshold: 0.05}}
	e, err := ev.Evaluate(0, evalProbes(), rules, func(name string) (Sample, error) {
		t.Logf("probe %s returns error", name)
		return Sample{}, errors.New("connection refused")
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("probe error => verdict=%s basis=%s", e.Verdict, e.BasisSummary)
	if e.Verdict != VerdictInconclusive {
		t.Fatalf("want inconclusive, got %s", e.Verdict)
	}
}

// MinSamples=2 但只拿到 1 个有效样本 -> 依据不足。
func TestEvaluateInsufficientSamples(t *testing.T) {
	ev := NewEvaluator(nil)
	probes := []ProbeSpec{{Name: "p", MinSamples: 2}}
	rules := []RuleSpec{{Probe: "p", Metric: "err", Operator: "<=", Threshold: 0.05}}
	calls := 0
	e, err := ev.Evaluate(0, probes, rules, func(name string) (Sample, error) {
		calls++
		if calls == 2 {
			return Sample{}, errors.New("transient probe failure")
		}
		return Sample{Values: map[string]float64{"err": 0.01}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("1/2 valid samples => verdict=%s basis=%s", e.Verdict, e.BasisSummary)
	if e.Verdict != VerdictInconclusive {
		t.Fatalf("want inconclusive, got %s", e.Verdict)
	}
}

// 同一指标规则内，样本越限数未超 MaxBreaches 容忍度 => 仍判通过。
func TestEvaluateBreachesWithinTolerance(t *testing.T) {
	ev := NewEvaluator(nil)
	probes := []ProbeSpec{{Name: "p", MinSamples: 3}}
	rules := []RuleSpec{{Probe: "p", Metric: "err", Operator: "<=", Threshold: 0.05, MaxBreaches: 1}}
	seq := 0
	e, err := ev.Evaluate(0, probes, rules, func(name string) (Sample, error) {
		seq++
		v := 0.01
		if seq == 1 {
			v = 0.5
		}
		return Sample{Values: map[string]float64{"err": v}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("1 breach in 3 samples, tolerance 1 => verdict=%s basis=%s", e.Verdict, e.BasisSummary)
	if e.Verdict != VerdictSteady {
		t.Fatalf("want steady, got %s", e.Verdict)
	}
}

// 规则之间既有通过又有违例 -> 依据相互矛盾，不得默认通过。
func TestEvaluateContradictoryRules(t *testing.T) {
	ev := NewEvaluator(nil)
	probes := []ProbeSpec{{Name: "lat", MinSamples: 1}, {Name: "err", MinSamples: 1}}
	rules := []RuleSpec{
		{Probe: "lat", Metric: "p99", Operator: "<=", Threshold: 100},   // 满足
		{Probe: "err", Metric: "rate", Operator: "<=", Threshold: 0.05}, // 违反
	}
	e, err := ev.Evaluate(0, probes, rules, fixedSample(map[string]map[string]float64{
		"lat": {"p99": 10},
		"err": {"rate": 0.9},
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("one pass one fail => verdict=%s basis=%s", e.Verdict, e.BasisSummary)
	if e.Verdict != VerdictContradictory {
		t.Fatalf("want contradictory, got %s", e.Verdict)
	}
}

func TestValidateRulesRejectsContradictoryConfig(t *testing.T) {
	probes := []ProbeSpec{{Name: "p"}}
	dup := []RuleSpec{
		{Probe: "p", Metric: "m", Operator: "<=", Threshold: 1},
		{Probe: "p", Metric: "m", Operator: ">=", Threshold: 2},
	}
	if err := ValidateRules(dup, probes); !errors.Is(err, errDuplicateRule) {
		t.Fatalf("want errDuplicateRule, got %v", err)
	}
	badOp := []RuleSpec{{Probe: "p", Metric: "m", Operator: "~", Threshold: 1}}
	if err := ValidateRules(badOp, probes); !errors.Is(err, errUnknownOperator) {
		t.Fatalf("want errUnknownOperator, got %v", err)
	}
	missing := []RuleSpec{{Probe: "nope", Metric: "m", Operator: "<=", Threshold: 1}}
	if err := ValidateRules(missing, probes); !errors.Is(err, errRuleNoProbe) {
		t.Fatalf("want errRuleNoProbe, got %v", err)
	}
	if err := ValidateRules(nil, probes); !errors.Is(err, errEmptyRuleset) {
		t.Fatalf("want errEmptyRuleset, got %v", err)
	}
}
