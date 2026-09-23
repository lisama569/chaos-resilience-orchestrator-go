package model

import "time"

// ---- 标识符 ----

type ExperimentID string
type TargetID string
type StepID string

// ---- 阶段 / 状态 / 结论 / 失败原因（穷举枚举，保证失败原因可区分）----

type Phase string

const (
	PhaseReachability Phase = "reachability"
	PhaseInject       Phase = "inject"
	PhaseObserve      Phase = "observe"
	PhaseHeal         Phase = "heal"
	PhaseCleanup      Phase = "cleanup"
)

type StepStatus string

const (
	StepPending   StepStatus = "pending"
	StepRunning   StepStatus = "running"
	StepSucceeded StepStatus = "succeeded"
	StepFailed    StepStatus = "failed"
	StepSkipped   StepStatus = "skipped"
)

type ExperimentStatus string

const (
	StatusPending   ExperimentStatus = "pending"
	StatusRunning   ExperimentStatus = "running"
	StatusPaused    ExperimentStatus = "paused"
	StatusSucceeded ExperimentStatus = "succeeded"
	StatusFailed    ExperimentStatus = "failed"
	StatusAborted   ExperimentStatus = "aborted"
)

type Conclusion string

const (
	ConclusionSteady       Conclusion = "steady"
	ConclusionNotSteady    Conclusion = "not_steady"
	ConclusionInconclusive Conclusion = "inconclusive"
)

type FailureReason string

const (
	ReasonNone               FailureReason = ""
	ReasonTargetUnreachable  FailureReason = "target_unreachable"
	ReasonInjectTimeout      FailureReason = "inject_timeout"
	ReasonInjectFailed       FailureReason = "inject_failed"
	ReasonProbeTimeout       FailureReason = "probe_timeout"
	ReasonProbeError         FailureReason = "probe_error"
	ReasonProbeInsufficient  FailureReason = "probe_insufficient"
	ReasonProbeContradictory FailureReason = "probe_contradictory"
	ReasonHealTimeout        FailureReason = "heal_timeout"
	ReasonHealFailed         FailureReason = "heal_failed"
	ReasonHealExhausted      FailureReason = "heal_exhausted"
	ReasonCleanupTimeout     FailureReason = "cleanup_timeout"
	ReasonCleanupFailed      FailureReason = "cleanup_failed"
	ReasonCanceled           FailureReason = "canceled"
)

type ResumePolicy string

const (
	ResumeAbort  ResumePolicy = "abort"
	ResumeResume ResumePolicy = "resume"
)

// ---- 计划配置 ----

type ActionSpec struct {
	Name    string            `json:"name"`
	Kind    string            `json:"kind"`
	Params  map[string]string `json:"params,omitempty"`
	Timeout time.Duration     `json:"timeout"`
}

type ProbeSpec struct {
	Name    string            `json:"name"`
	Kind    string            `json:"kind"`
	Key     string            `json:"key"`
	Params  map[string]string `json:"params,omitempty"`
	Timeout time.Duration     `json:"timeout"`
	Want    bool              `json:"want"`
	MinHits int               `json:"min_hits"`
}

type HealPolicySpec struct {
	MaxAttempts int        `json:"max_attempts"`
	Action      ActionSpec `json:"action"`
}

type TargetSpec struct {
	ID TargetID `json:"id"`
	// Reach 为可选的可达性检查动作；为 nil 时视为可达。
	Reach       *ActionSpec     `json:"reach,omitempty"`
	Inject      ActionSpec      `json:"inject"`
	Cleanup     ActionSpec      `json:"cleanup"`
	Heal        *HealPolicySpec `json:"heal,omitempty"`
	Probes      []ProbeSpec     `json:"probes"`
	RequiredFor Conclusion      `json:"required_for"`
}

type ExperimentSpec struct {
	ID            ExperimentID  `json:"id"`
	Targets       []TargetSpec  `json:"targets"`
	Concurrency   int           `json:"concurrency"`
	ObserveEvery  time.Duration `json:"observe_every"`
	ObserveRounds int           `json:"observe_rounds"`
	ResumePolicy  ResumePolicy  `json:"resume_policy"`
}

// ---- 运行态 ----

type Evidence struct {
	Probe   string        `json:"probe"`
	Attempt int           `json:"attempt"`
	Round   int           `json:"round"`
	Value   bool          `json:"value"`
	At      time.Time     `json:"at"`
	Latency time.Duration `json:"latency"`
}

type StepState struct {
	ID         StepID        `json:"id"`
	Target     TargetID      `json:"target"`
	Phase      Phase         `json:"phase"`
	Attempt    int           `json:"attempt"`
	Round      int           `json:"round"`
	Status     StepStatus    `json:"status"`
	Reason     FailureReason `json:"reason,omitempty"`
	Detail     string        `json:"detail,omitempty"`
	StartedAt  *time.Time    `json:"started_at,omitempty"`
	FinishedAt *time.Time    `json:"finished_at,omitempty"`
}

type TargetResult struct {
	ID        TargetID      `json:"id"`
	Healthy   bool          `json:"healthy"`
	Verdict   Conclusion    `json:"verdict"`
	Reason    FailureReason `json:"reason,omitempty"`
	HealRuns  int           `json:"heal_runs"`
	Evidence  []Evidence    `json:"evidence,omitempty"`
	CleanedUp bool          `json:"cleaned_up"`
}

type ExperimentState struct {
	ID         ExperimentID               `json:"id"`
	Spec       ExperimentSpec             `json:"spec"`
	Status     ExperimentStatus           `json:"status"`
	Conclusion Conclusion                 `json:"conclusion"`
	Reason     FailureReason              `json:"reason,omitempty"`
	Steps      []*StepState               `json:"steps"`
	Results    map[TargetID]*TargetResult `json:"results,omitempty"`
	Version    int                        `json:"version"`
	CreatedAt  time.Time                  `json:"created_at"`
	UpdatedAt  time.Time                  `json:"updated_at"`
}
