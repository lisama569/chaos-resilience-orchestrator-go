package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/action"
	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/engine"
	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/probe"
	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	stateDir := os.Getenv("CHAOS_STATE_DIR")
	if stateDir == "" {
		stateDir = ".chaos-state"
	}

	rt := action.NewLocalEnv()
	action.RegisterBuiltins()
	probe.RegisterBuiltins()

	st, err := store.NewFileStore(stateDir)
	must(err)
	eng := engine.New(st, rt, engine.NewStdLogger())

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	switch os.Args[1] {
	case "demo":
		spec := demoSpec()
		runDemo(ctx, eng, spec)
	case "run":
		if len(os.Args) < 3 {
			usage()
			os.Exit(2)
		}
		spec, err := loadPlan(os.Args[2])
		must(err)
		runDemo(ctx, eng, spec)
	case "resume":
		if len(os.Args) < 3 {
			usage()
			os.Exit(2)
		}
		out, err := eng.Resume(ctx, model.ExperimentID(os.Args[2]))
		must(err)
		printSummary(out)
	case "list":
		list, err := st.List(ctx)
		must(err)
		for _, e := range list {
			fmt.Printf("%s\t%s\t%s\t%s\n", e.ID, e.Status, e.Conclusion, e.Reason)
		}
	case "interrupt":
		if len(os.Args) < 3 {
			usage()
			os.Exit(2)
		}
		must(eng.Interrupt(model.ExperimentID(os.Args[2])))
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: orchestrator <command>
  demo              run the built-in local demo experiment
  run <plan.json>   run an experiment plan from JSON
  resume <id>       resume or terminate an unfinished experiment by policy
  interrupt <id>    gracefully interrupt a running experiment
  list              list experiments and their status

environment:
  CHAOS_STATE_DIR   state directory (default .chaos-state)`)
}

func runDemo(ctx context.Context, eng *engine.Engine, spec model.ExperimentSpec) {
	out, err := eng.Run(ctx, spec)
	if err != nil {
		must(err)
	}
	printSummary(out)
}

func printSummary(st *model.ExperimentState) {
	fmt.Printf("\n==== experiment %s ====\n", st.ID)
	fmt.Printf("status:     %s\n", st.Status)
	fmt.Printf("conclusion: %s\n", st.Conclusion)
	fmt.Printf("reason:     %s\n", dash(st.Reason))
	for _, t := range st.Spec.Targets {
		r := st.Results[t.ID]
		if r == nil {
			fmt.Printf("target %-8s (no result)\n", t.ID)
			continue
		}
		fmt.Printf("target %-8s verdict=%-12s healthy=%-5v healed=%d cleaned=%v reason=%s\n",
			t.ID, r.Verdict, r.Healthy, r.HealRuns, r.CleanedUp, dash(r.Reason))
	}
}

func dash(r model.FailureReason) string {
	if r == model.ReasonNone {
		return "-"
	}
	return string(r)
}

func loadPlan(path string) (model.ExperimentSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return model.ExperimentSpec{}, err
	}
	var spec model.ExperimentSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return model.ExperimentSpec{}, err
	}
	return spec, nil
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// demoSpec 构造一个完全本地、确定性的三目标实验：
//   - alpha：注入后始终稳态
//   - beta ：前两轮观测不达标，一次自愈后收敛
//   - gamma：目标不可达（用于展示可区分的失败原因）
func demoSpec() model.ExperimentSpec {
	return model.ExperimentSpec{
		ID:            "demo-experiment",
		Concurrency:   2,
		ObserveEvery:  10,
		ObserveRounds: 2,
		ResumePolicy:  model.ResumeResume,
		Targets: []model.TargetSpec{
			{
				ID: "alpha",
				Inject: model.ActionSpec{Kind: "set-flag", Timeout: 500,
					Params: map[string]string{"key": "alpha/injected", "value": "true"}},
				Cleanup: model.ActionSpec{Kind: "set-flag", Timeout: 500,
					Params: map[string]string{"key": "alpha/injected", "value": "false"}},
				Probes: []model.ProbeSpec{{
					Name: "alpha-injected-flag", Kind: "flag",
					Key: "alpha/injected", Want: true, MinHits: 2,
				}},
			},
			{
				ID: "beta",
				Inject: model.ActionSpec{Kind: "set-flag", Timeout: 500,
					Params: map[string]string{"key": "beta/svc", "value": "false"}},
				Cleanup: model.ActionSpec{Kind: "set-flag", Timeout: 500,
					Params: map[string]string{"key": "beta/svc", "value": "true"}},
				Heal: &model.HealPolicySpec{
					MaxAttempts: 2,
					Action: model.ActionSpec{Kind: "set-flag", Timeout: 500,
						Params: map[string]string{"key": "beta/svc", "value": "true"}},
				},
				Probes: []model.ProbeSpec{{
					Name: "beta-svc-flag", Kind: "flag",
					Key: "beta/svc", Want: true, MinHits: 2,
				}},
			},
			{
				ID:    "gamma",
				Reach: &model.ActionSpec{Kind: "unreachable", Timeout: 500},
				Inject: model.ActionSpec{Kind: "set-flag", Timeout: 500,
					Params: map[string]string{"key": "gamma/x", "value": "true"}},
				Cleanup: model.ActionSpec{Kind: "set-flag", Timeout: 500,
					Params: map[string]string{"key": "gamma/x", "value": "false"}},
				Probes: []model.ProbeSpec{{
					Name: "gamma-flag", Kind: "flag",
					Key: "gamma/x", Want: true, MinHits: 2,
				}},
			},
		},
	}
}
