package action

import (
	"context"
	"errors"
	"log"
	"os"
	"testing"
	"time"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
)

func TestMain(m *testing.M) {
	RegisterBuiltins()
	os.Exit(m.Run())
}

func TestSetFlagAndMetric(t *testing.T) {
	rt := NewLocalEnv()
	a, _ := Lookup("set-flag")
	act := a()
	if err := act.Execute(context.Background(), rt, map[string]string{"key": "k", "value": "true"}); err != nil {
		t.Fatal(err)
	}
	if !rt.Flag("k") {
		t.Fatal("flag should be set")
	}
	// 清理动作必须能复位
	if err := act.Execute(context.Background(), rt, map[string]string{"key": "k", "value": "false"}); err != nil {
		t.Fatal(err)
	}
	if rt.Flag("k") {
		t.Fatal("flag should be cleared after cleanup")
	}
}

func TestFlakyFailsThenSucceeds(t *testing.T) {
	rt := NewLocalEnv()
	f, _ := Lookup("flaky")
	act := f()
	params := map[string]string{
		"key": "calls", "fail_until": "2",
		"flag": "svc", "value": "true",
	}
	logger := log.New(os.Stdout, "[action-test] ", log.LstdFlags)
	for i := 1; i <= 4; i++ {
		err := act.Execute(context.Background(), rt, params)
		logger.Printf("INPUT flaky call=%d fail_until=2 err=%v flag=%v",
			i, err, rt.Flag("svc"))
		if i <= 2 {
			if err == nil {
				t.Fatalf("call %d must fail", i)
			}
			var fe *FailureError
			if !errors.As(err, &fe) || fe.Kind != KindFailed {
				t.Fatalf("call %d error must classify as failed, got %v", i, err)
			}
		} else {
			if err != nil {
				t.Fatalf("call %d must succeed, got %v", i, err)
			}
		}
	}
	if !rt.Flag("svc") {
		t.Fatal("svc flag must be set once flaky starts succeeding")
	}
}

func TestTimeoutClassification(t *testing.T) {
	if got := ClassifyInject(&FailureError{Kind: KindTimeout}); got != model.ReasonInjectTimeout {
		t.Fatalf("got %s", got)
	}
	if got := ClassifyInject(&FailureError{Kind: KindUnreachable}); got != model.ReasonTargetUnreachable {
		t.Fatalf("got %s", got)
	}
	if got := ClassifyInject(errors.New("boom")); got != model.ReasonInjectFailed {
		t.Fatalf("got %s", got)
	}
	if got := ClassifyHeal(&FailureError{Kind: KindTimeout}); got != model.ReasonHealTimeout {
		t.Fatalf("got %s", got)
	}
	if got := ClassifyCleanup(context.DeadlineExceeded); got != model.ReasonCleanupTimeout {
		t.Fatalf("got %s", got)
	}
	// 取消与超时必须可区分
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := ClassifyInject(ctx.Err()); got != model.ReasonInjectFailed {
		t.Fatalf("plain cancel maps to generic failed, got %s", got)
	}
}

func TestSleepActionRespectsDeadline(t *testing.T) {
	rt := NewLocalEnv()
	f, _ := Lookup("sleep")
	act := f()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := act.Execute(ctx, rt, map[string]string{"duration": "2s"})
	if !errors.Is(err, ErrActionTimeout) {
		t.Fatalf("must timeout with ErrActionTimeout, got %v", err)
	}
}
