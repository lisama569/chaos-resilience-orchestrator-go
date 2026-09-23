package orchestrator

import (
	"time"
)

// testTarget 构造一个带确定性脚本键约定的目标规格：
//
//	reach   注入前可达性探测
//	inject  注入命令
//	recover 清理命令
//	probe   稳态探测（健康指标 err，规则 err <= 0.05）
//	reprobe 自愈后复核探测（健康指标 err）
//	heal    自愈动作命令
func testTarget(id string, heal bool) TargetSpec {
	t := TargetSpec{
		ID:          id,
		Target:      "svc-" + id,
		ReachableBy: "reach",
		Inject:      ActionSpec{Name: "inj", Kind: ActionInject, Command: "inject", Timeout: time.Second},
		Recover:     ActionSpec{Name: "rec", Kind: ActionRecover, Command: "recover", Timeout: time.Second},
		Probes:      []ProbeSpec{{Name: "probe", MinSamples: 1}},
		Rules:       []RuleSpec{{Probe: "probe", Metric: "err", Operator: "<=", Threshold: 0.05}},
	}
	if heal {
		t.Heal = &HealPolicy{
			MaxRounds:     2,
			RecoverAction: &ActionSpec{Name: "heal-act", Kind: ActionHeal, Command: "heal", Timeout: time.Second},
			RecheckProbes: []ProbeSpec{{Name: "reprobe", MinSamples: 1}},
			RecheckRules:  []RuleSpec{{Probe: "reprobe", Metric: "err", Operator: "<=", Threshold: 0.05}},
		}
	}
	return t
}

func testPlan(targets ...TargetSpec) *Plan {
	return &Plan{ID: "test-plan", Concurrency: 2, RunTimeout: 10 * time.Second, Targets: targets}
}

func simScripts(extra map[string]*Script) map[string]*Script {
	base := map[string]*Script{
		"reach":   {},
		"inject":  {},
		"recover": {},
		"heal":    {},
		"probe":   {Values: map[string]float64{"err": 0.01}},
		"reprobe": {Values: map[string]float64{"err": 0.0}},
	}
	for k, v := range extra {
		base[k] = v
	}
	return base
}
