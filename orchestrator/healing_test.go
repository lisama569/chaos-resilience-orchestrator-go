package orchestrator

import (
	"errors"
	"sync"
	"testing"
)

// fakeSink 记录动作触发轮次，并按脚本返回复核结论。
type fakeSink struct {
	mu          sync.Mutex
	firedRounds []int
	rechecks    []int
	verdicts    map[int]Verdict
	fireErr     map[int]error
	doneSteps   map[int]bool // 模拟持久化中“本轮动作已触发”
}

func (f *fakeSink) FireHealAction(round int, act ActionSpec) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.doneSteps == nil {
		f.doneSteps = map[int]bool{}
	}
	if f.fireErr == nil {
		f.fireErr = map[int]error{}
	}
	if f.doneSteps[round] {
		return false, nil // 已触发过：跳过，不重复执行
	}
	if err := f.fireErr[round]; err != nil {
		return false, err
	}
	f.firedRounds = append(f.firedRounds, round)
	f.doneSteps[round] = true
	return true, nil
}

func (f *fakeSink) Recheck(round int) (*Evaluation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rechecks = append(f.rechecks, round)
	return &Evaluation{Verdict: f.verdicts[round], Round: round + 1}, nil
}

func policy(n int) *HealPolicy {
	return &HealPolicy{MaxRounds: n, RecoverAction: &ActionSpec{Name: "h", Kind: ActionHeal}}
}

func TestHealConverges(t *testing.T) {
	sink := &fakeSink{verdicts: map[int]Verdict{0: VerdictSteady}}
	out, rounds, err := NewHealer().Run(policy(3), 0, sink)
	if err != nil || out != HealConverged || rounds != 1 {
		t.Fatalf("out=%s rounds=%d err=%v", out, rounds, err)
	}
	t.Logf("converged after rounds=%d fired=%v", rounds, sink.firedRounds)
}

func TestHealNotConvergedWithinLimit(t *testing.T) {
	sink := &fakeSink{verdicts: map[int]Verdict{0: VerdictViolated, 1: VerdictViolated, 2: VerdictViolated}}
	out, rounds, err := NewHealer().Run(policy(3), 0, sink)
	if err != nil || out != HealNotConverged || rounds != 3 {
		t.Fatalf("out=%s rounds=%d err=%v", out, rounds, err)
	}
	if len(sink.firedRounds) != 3 {
		t.Fatalf("heal action must fire at most once per round, got %v", sink.firedRounds)
	}
	t.Logf("stopped at hard limit: fired=%v, no infinite retry", sink.firedRounds)
}

// 恢复场景：roundsDone=1 表示第 0 轮已完整完成；不得重跑第 0 轮。
func TestHealResumeDoesNotReplayDoneRound(t *testing.T) {
	sink := &fakeSink{
		verdicts:  map[int]Verdict{1: VerdictSteady},
		doneSteps: map[int]bool{0: true},
	}
	out, rounds, err := NewHealer().Run(policy(3), 1, sink)
	if err != nil || out != HealConverged || rounds != 2 {
		t.Fatalf("out=%s rounds=%d err=%v", out, rounds, err)
	}
	for _, r := range sink.firedRounds {
		if r == 0 {
			t.Fatal("round 0 action must not be re-fired after resume")
		}
	}
	t.Logf("resume fired=%v rechecks=%v, completed round not replayed", sink.firedRounds, sink.rechecks)
}

// 动作在崩溃前已触发但未复核：恢复后跳过触发，只补复核。
func TestHealResumeSkipsFiredActionButRechecks(t *testing.T) {
	sink := &fakeSink{
		verdicts:  map[int]Verdict{0: VerdictSteady},
		doneSteps: map[int]bool{0: true},
	}
	out, _, err := NewHealer().Run(policy(3), 0, sink)
	if err != nil || out != HealConverged {
		t.Fatalf("out=%s err=%v", out, err)
	}
	if len(sink.firedRounds) != 0 || len(sink.rechecks) != 1 {
		t.Fatalf("want 0 fires (deduped) + 1 recheck, got fires=%v rechecks=%v", sink.firedRounds, sink.rechecks)
	}
	t.Log("already-fired action not duplicated; recheck still performed")
}

func TestHealInconclusiveAborts(t *testing.T) {
	sink := &fakeSink{verdicts: map[int]Verdict{0: VerdictInconclusive}}
	out, rounds, err := NewHealer().Run(policy(3), 0, sink)
	if err != nil || out != HealInconclusive || rounds != 1 {
		t.Fatalf("out=%s rounds=%d err=%v", out, rounds, err)
	}
	t.Logf("inconclusive recheck aborts (no default convergence): rounds=%d", rounds)
}

func TestHealActionFailureAborts(t *testing.T) {
	sink := &fakeSink{fireErr: map[int]error{0: errors.New("boom")}}
	out, rounds, err := NewHealer().Run(policy(3), 0, sink)
	if err == nil || out != HealActionFailed || rounds != 0 {
		t.Fatalf("out=%s rounds=%d err=%v", out, rounds, err)
	}
	t.Logf("heal action failure explicitly aborted: %v", err)
}

func TestHealCanceledMapsToOutcome(t *testing.T) {
	sink := &fakeSink{fireErr: map[int]error{
		0: &ActionError{Reason: ReasonExperimentCanceled, Detail: "ctx canceled"},
	}}
	out, _, err := NewHealer().Run(policy(3), 0, sink)
	if out != HealCanceled {
		t.Fatalf("out=%s err=%v", out, err)
	}
	var ae *ActionError
	if !errors.As(err, &ae) || ae.Reason != ReasonExperimentCanceled {
		t.Fatalf("want canceled ActionError, got %v", err)
	}
	t.Logf("cancellation distinguished: out=%s", out)
}

func TestHealRejectsMisconfiguration(t *testing.T) {
	if _, _, err := NewHealer().Run(nil, 0, &fakeSink{}); !errors.Is(err, ErrHealMisconfigured) {
		t.Fatalf("want ErrHealMisconfigured, got %v", err)
	}
	p := &HealPolicy{MaxRounds: 0, RecoverAction: &ActionSpec{}}
	if _, _, err := NewHealer().Run(p, 0, &fakeSink{}); !errors.Is(err, ErrHealMisconfigured) {
		t.Fatalf("want ErrHealMisconfigured, got %v", err)
	}
}
