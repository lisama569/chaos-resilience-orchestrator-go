package policy

import (
	"fmt"
	"sort"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
)

// Verdict 是单个目标的确定性判定结果。
type Verdict struct {
	Target  model.TargetID
	Healthy bool
	Outcome model.Conclusion
	Reason  model.FailureReason
	// Basis 记录判定依据（每条规则的证据计数与结论），保证可解释、可复现。
	Basis    []string
	Evidence []model.Evidence
}

// Policy 依据配置的规则与证据计算判定结论。
// 证据不足（规则没有任何观测）=> inconclusive/probe_insufficient；
// 证据相互矛盾（同一探针在不同轮次给出不同值）=> inconclusive/probe_contradictory；
// 二者都不是时，全部规则满足才是 steady，否则 not_steady。
type Policy struct {
	target model.TargetSpec
}

// New 为单个目标构造判定策略。
func New(spec model.TargetSpec) *Policy {
	return &Policy{target: spec}
}

// Evaluate 对已采集证据做确定性判定。规则按名称排序，保证依据输出稳定。
func (p *Policy) Evaluate(evidence []model.Evidence) Verdict {
	v := Verdict{Target: p.target.ID, Evidence: evidence}

	// 按探针名归组证据。
	byProbe := map[string][]model.Evidence{}
	for _, ev := range evidence {
		byProbe[ev.Probe] = append(byProbe[ev.Probe], ev)
	}

	ruleOK := true
	rules := append([]model.ProbeSpec(nil), p.target.Probes...)
	sort.Slice(rules, func(i, j int) bool { return rules[i].Name < rules[j].Name })

	for _, r := range rules {
		hits := byProbe[r.Name]
		sort.Slice(hits, func(i, j int) bool { return hits[i].Round < hits[j].Round })

		// 依据不足：规则没有任何观测证据，不能默认通过。
		if len(hits) == 0 {
			v.Outcome = model.ConclusionInconclusive
			v.Reason = model.ReasonProbeInsufficient
			v.Basis = append(v.Basis, fmt.Sprintf("rule=%s want=%v min_hits=%d -> NO EVIDENCE => inconclusive(insufficient)",
				r.Name, r.Want, r.MinHits))
			ruleOK = false
			continue
		}

		// 相互矛盾：同一探针在不同轮次出现不同观测值。
		first := hits[0].Value
		contradiction := false
		values := make([]bool, 0, len(hits))
		for _, h := range hits {
			values = append(values, h.Value)
			if h.Value != first {
				contradiction = true
			}
		}
		if contradiction {
			v.Outcome = model.ConclusionInconclusive
			v.Reason = model.ReasonProbeContradictory
			v.Basis = append(v.Basis, fmt.Sprintf("rule=%s observations=%v -> CONTRADICTION => inconclusive(contradictory)",
				r.Name, values))
			ruleOK = false
			continue
		}

		// 确定性判定：值必须等于期望值，且命中次数不少于阈值。
		ok := first == r.Want && len(hits) >= r.MinHits
		if !ok {
			ruleOK = false
		}
		v.Basis = append(v.Basis, fmt.Sprintf("rule=%s want=%v min_hits=%d observations=%v(%d) -> rule_satisfied=%v",
			r.Name, r.Want, r.MinHits, values, len(values), ok))
	}

	if v.Outcome == model.ConclusionInconclusive {
		v.Healthy = false
		return v
	}
	if ruleOK {
		v.Outcome = model.ConclusionSteady
		v.Healthy = true
		return v
	}
	v.Outcome = model.ConclusionNotSteady
	v.Healthy = false
	return v
}
