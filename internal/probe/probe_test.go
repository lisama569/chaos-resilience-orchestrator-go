package probe

import (
	"context"
	"testing"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/action"
	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
)

func TestBuiltinProbes(t *testing.T) {
	rt := action.NewLocalEnv()
	rt.SetFlag("healthy", true)
	rt.AddMetric("count", 5)
	RegisterBuiltins()

	flag, err := Build(model.ProbeSpec{Kind: "flag"}, rt)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := flag.Collect(context.Background(), "healthy"); err != nil || !v {
		t.Fatalf("flag probe = %v,%v", v, err)
	}

	th, err := Build(model.ProbeSpec{
		Kind:   "metric-threshold",
		Params: map[string]string{"threshold": "3"},
	}, rt)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := th.Collect(context.Background(), "count"); !v {
		t.Fatal("threshold probe should be true when metric 5 >= 3")
	}

	bad, err := Build(model.ProbeSpec{Kind: "error"}, rt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bad.Collect(context.Background(), "x"); err == nil {
		t.Fatal("error probe must return an error")
	}

	if _, err := Build(model.ProbeSpec{Kind: "nope"}, rt); err == nil {
		t.Fatal("unknown probe kind must error")
	}
}
