package orchestrator

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// RuleResult 是单条规则的判定依据，必须可解释、可复现。
type RuleResult struct {
	Probe       string  `json:"probe"`
	Metric      string  `json:"metric"`
	Operator    string  `json:"operator"`
	Threshold   float64 `json:"threshold"`
	Breaches    int     `json:"breaches"`     // 超出阈值的样本数
	SampleCount int     `json:"sample_count"` // 实际参与判定的有效样本数
	Passed      bool    `json:"passed"`
	Reason      string  `json:"reason"` // 未通过/不可判定的具体说明
}

// Evaluation 是一次稳态判定的完整结论与依据。
type Evaluation struct {
	Verdict      Verdict      `json:"verdict"`
	Round        int          `json:"round"`
	At           time.Time    `json:"at"`
	RuleResults  []RuleResult `json:"rule_results"`
	BasisSummary string       `json:"basis_summary"` // 人类可读的判定依据摘要
}

// SampleFunc 采集一次指定探测的样本；失败由评估器计入“依据不足”。
type SampleFunc func(name string) (Sample, error)

// 判定规则相关错误。
var (
	errUnknownOperator = errors.New("unknown rule operator")
	errDuplicateRule   = errors.New("duplicate rule for same probe+metric")
	errRuleNoProbe     = errors.New("rule references probe not declared")
	errEmptyRuleset    = errors.New("empty ruleset: refusing to default-pass without any basis")
)

// Evaluator 按可配置规则进行确定性稳态判定。
type Evaluator struct {
	now func() time.Time
}

// NewEvaluator 创建评估器，clock 为 nil 时使用系统时钟（测试可注入固定时钟）。
func NewEvaluator(clock func() time.Time) *Evaluator {
	if clock == nil {
		clock = time.Now
	}
	return &Evaluator{now: clock}
}

// ValidateRules 检查规则配置是否合法：
// 操作符未知、规则引用未声明探测、同一 probe+metric 出现多条规则，
// 都视为配置自相矛盾，直接拒绝，绝不带着歧义继续判定。
func ValidateRules(rules []RuleSpec, probes []ProbeSpec) error {
	if len(rules) == 0 {
		return errEmptyRuleset
	}
	known := map[string]bool{}
	var minSample map[string]int
	minSample = map[string]int{}
	for _, p := range probes {
		known[p.Name] = true
		minSample[p.Name] = p.MinSamples
	}
	seen := map[string]bool{}
	for i, r := range rules {
		switch r.Operator {
		case ">=", "<=", ">", "<", "==":
		default:
			return fmt.Errorf("%w: %q (rule #%d)", errUnknownOperator, r.Operator, i)
		}
		if !known[r.Probe] {
			return fmt.Errorf("%w: probe=%q metric=%q", errRuleNoProbe, r.Probe, r.Metric)
		}
		key := r.Probe + "\x00" + r.Metric
		if seen[key] {
			return fmt.Errorf("%w: probe=%q metric=%q", errDuplicateRule, r.Probe, r.Metric)
		}
		seen[key] = true
	}
	return nil
}

// Evaluate 采集证据并依据规则得出确定性结论。
// round 为判定轮次（初次为 0，自愈后复核依次递增）。
//
// 结论优先级（任何情况下都不默认通过）：
//  1. 探测异常、样本不足、指标缺失 -> Inconclusive（依据不足）
//  2. 规则之间既有通过又有违例     -> Contradictory（依据相互矛盾）
//  3. 全部违例                    -> Violated
//  4. 全部通过                    -> Steady
func (ev *Evaluator) Evaluate(round int, probes []ProbeSpec, rules []RuleSpec, sample SampleFunc) (*Evaluation, error) {
	if err := ValidateRules(rules, probes); err != nil {
		return nil, fmt.Errorf("invalid steady-state rules: %w", err)
	}
	if sample == nil {
		return nil, errors.New("nil sample function")
	}

	probeOrder := make(map[string]int, len(probes))
	minSamples := make(map[string]int, len(probes))
	for i, p := range probes {
		probeOrder[p.Name] = i
		minSamples[p.Name] = p.MinSamples
	}

	// 每个探测按 MinSamples 次采集确定性样本（默认 1 次）；
	// 任一采集异常导致有效样本不足，即依据不足，绝不默认通过。
	samples := make(map[string][]Sample, len(probes))
	var problems []string
	for _, p := range probes {
		n := p.MinSamples
		if n < 1 {
			n = 1
		}
		for i := 0; i < n; i++ {
			s, err := sample(p.Name)
			if err != nil {
				problems = append(problems, fmt.Sprintf("probe %q sample #%d error: %v", p.Name, i, err))
				continue
			}
			samples[p.Name] = append(samples[p.Name], s)
		}
	}

	results := make([]RuleResult, 0, len(rules))
	passed, failed := 0, 0
	for _, r := range rules {
		rr := RuleResult{
			Probe:     r.Probe,
			Metric:    r.Metric,
			Operator:  r.Operator,
			Threshold: r.Threshold,
		}
		got := samples[r.Probe]
		need := minSamples[r.Probe]
		if need < 1 {
			need = 1
		}
		if len(got) == 0 {
			rr.Reason = fmt.Sprintf("no valid sample for probe %q (probe error)", r.Probe)
			results = append(results, rr)
			problems = append(problems, rr.Reason)
			continue
		}
		// 样本数量不足本身就是依据不足，无论指标是否越界都不得判通过。
		if len(got) < need {
			rr.SampleCount = len(got)
			rr.Reason = fmt.Sprintf("insufficient samples: have %d, need %d", len(got), need)
			results = append(results, rr)
			problems = append(problems, fmt.Sprintf("probe %q: %s", r.Probe, rr.Reason))
			continue
		}
		for _, s := range got {
			v, ok := s.Values[r.Metric]
			if !ok {
				rr.Reason = fmt.Sprintf("metric %q missing from a probe %q sample", r.Metric, r.Probe)
				break
			}
			rr.SampleCount++
			if isBreach(r.Operator, v, r.Threshold) {
				rr.Breaches++
			}
		}
		if rr.Reason != "" {
			results = append(results, rr)
			problems = append(problems, rr.Reason)
			continue
		}
		// MaxBreaches 默认 0：任何越限都算违例。
		if rr.Breaches > r.MaxBreaches {
			rr.Passed = false
			rr.Reason = fmt.Sprintf("breaches %d exceed allowed %d (samples=%d)",
				rr.Breaches, r.MaxBreaches, rr.SampleCount)
			failed++
		} else {
			rr.Passed = true
			rr.Reason = fmt.Sprintf("breaches %d within allowed %d (samples=%d, op %s threshold %g)",
				rr.Breaches, r.MaxBreaches, rr.SampleCount, r.Operator, r.Threshold)
			passed++
		}
		results = append(results, rr)
	}
	// 保持规则顺序稳定输出。
	sort.SliceStable(results, func(i, j int) bool {
		if results[i].Probe != results[j].Probe {
			return probeOrder[results[i].Probe] < probeOrder[results[j].Probe]
		}
		return results[i].Metric < results[j].Metric
	})

	verdict := VerdictSteady
	switch {
	case len(problems) > 0:
		verdict = VerdictInconclusive
	case passed > 0 && failed > 0:
		verdict = VerdictContradictory
	case failed > 0:
		verdict = VerdictViolated
	default:
		verdict = VerdictSteady
	}

	return &Evaluation{
		Verdict:      verdict,
		Round:        round,
		At:           ev.now(),
		RuleResults:  results,
		BasisSummary: buildBasisSummary(verdict, results, problems),
	}, nil
}

// isBreach 判定一个样本是否违反规则。
// operator 表达的是“稳态保持所需满足的条件”，因此条件不成立即为越限：
//
//	op ">=" 阈值 -> 稳态要求 v >= threshold；v < threshold 即越限；
//	op "<=" 阈值 -> 稳态要求 v <= threshold；v > threshold 即越限，以此类推。
func isBreach(op string, v, threshold float64) bool {
	switch op {
	case ">=":
		return v < threshold
	case "<=":
		return v > threshold
	case ">":
		return v <= threshold
	case "<":
		return v >= threshold
	case "==":
		return v != threshold
	default:
		return false
	}
}

func buildBasisSummary(v Verdict, rr []RuleResult, problems []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "verdict=%s; rules:", v)
	for _, r := range rr {
		fmt.Fprintf(&b, " [%s.%s passed=%t breaches=%d samples=%d]",
			r.Probe, r.Metric, r.Passed, r.Breaches, r.SampleCount)
	}
	if len(problems) > 0 {
		b.WriteString("; problems: ")
		b.WriteString(strings.Join(problems, " | "))
	}
	return b.String()
}
