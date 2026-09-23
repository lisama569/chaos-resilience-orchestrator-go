// demo 是一个完全本地运行的编排演示：
//
//	steady       —— 故障注入后稳态保持（清理成功）
//	violated-heal —— 稳态违例后触发自愈，复核收敛
//	unreachable  —— 目标不可达，不注入、不清理
//
// 状态文件默认写入 ./demo-state.json，删除该文件相当于全新开始；
// 保留该文件再次运行则触发“续跑”：已终态目标不会被重复执行。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/lisama569/chaos-resilience-orchestrator-go/orchestrator"
)

func main() {
	statePath := flag.String("state", "./demo-state.json", "状态文件路径（删除=全新运行，保留=续跑）")
	flag.Parse()

	plan := buildPlan()
	exec := buildExecutor()
	store := orchestrator.NewFileStore(*statePath)

	eng := orchestrator.NewEngine(exec, store, orchestrator.WithLogger(func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	report, err := eng.Run(ctx, plan)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n==================== 汇总报告 ====================")
	fmt.Printf("plan=%s timed_out=%v canceled=%v\n", report.PlanID, report.TimedOut, report.Canceled)
	for _, r := range report.Results {
		fmt.Printf("- target=%-14s phase=%-15s verdict=%-13s reason=%-24s cleanup=%s heal_rounds=%d\n",
			r.TargetID, r.Phase, r.Verdict, r.Failure, r.Cleanup, r.HealRounds)
		if r.FailureDetail != "" {
			fmt.Printf("    detail: %s\n", r.FailureDetail)
		}
	}
	fmt.Printf("详细状态与审计事件见: %s\n", *statePath)
}

func buildPlan() *orchestrator.Plan {
	return &orchestrator.Plan{
		ID:          "demo-plan",
		Concurrency: 2,
		RunTimeout:  30 * time.Second,
		Targets: []orchestrator.TargetSpec{
			{
				ID:          "steady",
				Target:      "svc-steady",
				ReachableBy: "reach-ok",
				Inject:      orchestrator.ActionSpec{Name: "inject-delay", Kind: orchestrator.ActionInject, Command: "inject-ok", Timeout: 2 * time.Second},
				Recover:     orchestrator.ActionSpec{Name: "remove-delay", Kind: orchestrator.ActionRecover, Command: "recover-ok", Timeout: 2 * time.Second},
				Probes:      []orchestrator.ProbeSpec{{Name: "probe-steady", MinSamples: 1}},
				Rules: []orchestrator.RuleSpec{
					{Probe: "probe-steady", Metric: "error_rate", Operator: "<=", Threshold: 0.05, MaxBreaches: 0},
				},
			},
			{
				ID:          "violated-heal",
				Target:      "svc-broken",
				ReachableBy: "reach-ok",
				Inject:      orchestrator.ActionSpec{Name: "inject-error", Kind: orchestrator.ActionInject, Command: "inject-ok", Timeout: 2 * time.Second},
				Recover:     orchestrator.ActionSpec{Name: "remove-error", Kind: orchestrator.ActionRecover, Command: "recover-ok", Timeout: 2 * time.Second},
				Probes:      []orchestrator.ProbeSpec{{Name: "probe-broken", MinSamples: 1}},
				Rules: []orchestrator.RuleSpec{
					{Probe: "probe-broken", Metric: "error_rate", Operator: "<=", Threshold: 0.05, MaxBreaches: 0},
				},
				Heal: &orchestrator.HealPolicy{
					MaxRounds:     2,
					RecoverAction: &orchestrator.ActionSpec{Name: "restart-svc", Kind: orchestrator.ActionHeal, Command: "heal-ok", Timeout: 2 * time.Second},
					RecheckProbes: []orchestrator.ProbeSpec{{Name: "probe-healed", MinSamples: 1}},
					RecheckRules: []orchestrator.RuleSpec{
						{Probe: "probe-healed", Metric: "error_rate", Operator: "<=", Threshold: 0.05, MaxBreaches: 0},
					},
				},
			},
			{
				ID:          "unreachable",
				Target:      "svc-dead",
				ReachableBy: "reach-dead",
				Inject:      orchestrator.ActionSpec{Name: "inject", Kind: orchestrator.ActionInject, Command: "inject-ok", Timeout: 2 * time.Second},
				Recover:     orchestrator.ActionSpec{Name: "recover", Kind: orchestrator.ActionRecover, Command: "recover-ok", Timeout: 2 * time.Second},
				Probes:      []orchestrator.ProbeSpec{{Name: "probe-x", MinSamples: 1}},
				Rules: []orchestrator.RuleSpec{
					{Probe: "probe-x", Metric: "error_rate", Operator: "<=", Threshold: 0.05, MaxBreaches: 0},
				},
			},
		},
	}
}

func buildExecutor() orchestrator.Executor {
	return orchestrator.NewSimExecutor(map[string]*orchestrator.Script{
		"reach-ok":     {},
		"reach-dead":   {Unreachable: true},
		"inject-ok":    {},
		"recover-ok":   {},
		"heal-ok":      {},
		"probe-steady": {Values: map[string]float64{"error_rate": 0.01}},
		"probe-broken": {Values: map[string]float64{"error_rate": 0.9}},
		"probe-healed": {Values: map[string]float64{"error_rate": 0.0}},
		"probe-x":      {Values: map[string]float64{"error_rate": 0.0}},
	})
}
