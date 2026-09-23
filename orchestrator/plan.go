package orchestrator

import (
	"errors"
	"fmt"
)

// 计划校验错误。
var (
	// ErrPlanInvalid 表示计划静态校验失败（含判定规则自相矛盾）。
	ErrPlanInvalid     = errors.New("plan invalid")
	errEmptyPlanID     = errors.New("plan id is empty")
	errNoTargets       = errors.New("plan has no targets")
	errBadConcurrency  = errors.New("plan concurrency must be >= 1")
	errEmptyTargetID   = errors.New("target id is empty")
	errBadInjectKind   = errors.New("inject action must have kind inject")
	errBadRecoverKind  = errors.New("recover action must have kind recover")
	errBadHealKind     = errors.New("heal action must have kind heal")
	errDuplicateTarget = errors.New("duplicate target id")
)

// ValidatePlan 对计划做静态校验，任何配置歧义（含稳态规则自相矛盾）
// 都在运行前被拒绝，避免在实验中途才发现无法判定。
func ValidatePlan(p *Plan) error {
	if p == nil {
		return errors.New("nil plan")
	}
	if p.ID == "" {
		return errEmptyPlanID
	}
	if len(p.Targets) == 0 {
		return errNoTargets
	}
	if p.Concurrency < 1 {
		return fmt.Errorf("%w: got %d", errBadConcurrency, p.Concurrency)
	}
	seen := map[string]bool{}
	for i := range p.Targets {
		t := &p.Targets[i]
		if t.ID == "" {
			return fmt.Errorf("target #%d: %w", i, errEmptyTargetID)
		}
		if seen[t.ID] {
			return fmt.Errorf("%w: %q", errDuplicateTarget, t.ID)
		}
		seen[t.ID] = true
		if t.Target == "" {
			return fmt.Errorf("target %q: empty target address", t.ID)
		}
		if t.Inject.Kind != ActionInject {
			return fmt.Errorf("target %q: %w", t.ID, errBadInjectKind)
		}
		if t.Recover.Kind != ActionRecover {
			return fmt.Errorf("target %q: %w", t.ID, errBadRecoverKind)
		}
		if t.Inject.Target == "" {
			t.Inject.Target = t.Target
		}
		if t.Recover.Target == "" {
			t.Recover.Target = t.Target
		}
		if err := ValidateRules(t.Rules, t.Probes); err != nil {
			return fmt.Errorf("target %q: %w", t.ID, err)
		}
		if t.Heal != nil {
			if t.Heal.RecoverAction == nil || t.Heal.MaxRounds < 1 {
				return fmt.Errorf("target %q: %w: healer requires action and max_rounds>=1", t.ID, ErrHealMisconfigured)
			}
			if t.Heal.RecoverAction.Kind != ActionHeal {
				return fmt.Errorf("target %q: %w", t.ID, errBadHealKind)
			}
			if t.Heal.RecoverAction.Target == "" {
				t.Heal.RecoverAction.Target = t.Target
			}
			if len(t.Heal.RecheckRules) == 0 {
				return fmt.Errorf("target %q: %w: healer requires recheck rules", t.ID, ErrHealMisconfigured)
			}
			if err := ValidateRules(t.Heal.RecheckRules, t.Heal.RecheckProbes); err != nil {
				return fmt.Errorf("target %q heal recheck: %w", t.ID, err)
			}
		}
	}
	return nil
}
