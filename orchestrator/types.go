// Package orchestrator 提供可完全本地运行的韧性验证编排能力：
// 用受控的故障注入驱动稳态判定与自愈动作，并保证并发、部分失败、
// 重复触发与中断恢复场景下结果可复现、可解释。
package orchestrator

import "time"

// ActionKind 描述一个动作的语义类别。
type ActionKind string

const (
	// ActionInject 故障注入动作（必须可清理）。
	ActionInject ActionKind = "inject"
	// ActionRecover 恢复（清理）动作，应设计为幂等。
	ActionRecover ActionKind = "recover"
	// ActionHeal 自愈动作，仅在稳态判定确认违例后触发。
	ActionHeal ActionKind = "heal"
)

// Phase 是单个目标实验所处的生命周期阶段。
// 阶段只能按定义的顺序前进，任何回退都会被明确拒绝。
type Phase string

const (
	PhaseNew           Phase = ""               // 尚未开始
	PhaseInjecting     Phase = "injecting"      // 正在注入故障
	PhaseInjected      Phase = "injected"       // 故障已注入（崩溃恢复的边界）
	PhaseProbing       Phase = "probing"        // 正在采集稳态证据
	PhaseHealing       Phase = "healing"        // 稳态违例，正在自愈
	PhaseCleaning      Phase = "cleaning"       // 正在清理已注入的故障
	PhaseCompleted     Phase = "completed"      // 正常终态
	PhaseAborted       Phase = "aborted"        // 异常终态（含判定中止、不可达等）
	PhaseCleanupFailed Phase = "cleanup_failed" // 终态：故障清理失败，可能残留半生效状态
)

// phaseOrder 定义阶段的前进顺序，用于拒绝状态回退。
var phaseOrder = map[Phase]int{
	PhaseNew:           0,
	PhaseInjecting:     1,
	PhaseInjected:      2,
	PhaseProbing:       3,
	PhaseHealing:       4,
	PhaseCleaning:      5,
	PhaseCompleted:     6,
	PhaseAborted:       6,
	PhaseCleanupFailed: 6,
}

// IsTerminal 报告阶段是否为终态。
func (p Phase) IsTerminal() bool { return phaseOrder[p] >= 6 }

// FailureReason 是可区分的失败原因枚举。
// 不同异常链路必须映射到不同原因，禁止笼统地归为“失败”。
type FailureReason string

const (
	ReasonNone                  FailureReason = ""                       // 未失败
	ReasonTargetUnreachable     FailureReason = "target_unreachable"     // 目标不可达（注入前健康检查失败）
	ReasonInjectFailed          FailureReason = "inject_failed"          // 故障注入动作失败
	ReasonActionTimeout         FailureReason = "action_timeout"         // 动作执行超时
	ReasonExperimentCanceled    FailureReason = "experiment_canceled"    // 实验被外部取消
	ReasonProbeError            FailureReason = "probe_error"            // 稳态探测执行异常，依据不足
	ReasonEvidenceInsufficient  FailureReason = "evidence_insufficient"  // 证据数量不足
	ReasonEvidenceContradictory FailureReason = "evidence_contradictory" // 证据相互矛盾
	ReasonHealNotConverged      FailureReason = "heal_not_converged"     // 自愈在有限次数内未收敛
	ReasonHealActionFailed      FailureReason = "heal_action_failed"     // 自愈动作本身失败
	ReasonSteadyViolated        FailureReason = "steady_violated"        // 稳态被确认破坏（且无自愈策略可联动）
	ReasonPersistFailed         FailureReason = "persist_failed"         // 状态持久化失败
	ReasonRecoverFailed         FailureReason = "recover_failed"         // 清理动作失败（达到重试上限）
	ReasonStateConflict         FailureReason = "state_conflict"         // 状态回退/重复上报被拒绝
	ReasonPlanInvalid           FailureReason = "plan_invalid"           // 计划配置非法（含判定规则自相矛盾）
	ReasonInterrupted           FailureReason = "interrupted_by_restart" // 中断时故障已注入，重启后只清理并中止
)

// MaxCleanupAttempts 是清理动作的最大尝试次数（含首次），
// 防止清理阶段自身无限重试；达到上限即明确标记 cleanup_failed。
const MaxCleanupAttempts = 3

// Verdict 是一次稳态判定的结论。
type Verdict string

const (
	VerdictSteady        Verdict = "steady"        // 稳态保持
	VerdictViolated      Verdict = "violated"      // 有确定证据表明稳态被破坏
	VerdictInconclusive  Verdict = "inconclusive"  // 依据不足（探测异常/证据缺失）
	VerdictContradictory Verdict = "contradictory" // 依据相互矛盾
)

// CleanupResult 描述故障清理的最终结果，必须与实验结论可区分。
type CleanupResult string

const (
	CleanupNotNeeded CleanupResult = "not_needed" // 未成功注入任何故障，无需清理
	CleanupSucceeded CleanupResult = "succeeded"  // 故障确认已清理
	CleanupFailed    CleanupResult = "failed"     // 清理失败，可能存在残留
)

// ActionSpec 描述一个待执行的动作。
type ActionSpec struct {
	Name    string        `json:"name"`
	Kind    ActionKind    `json:"kind"`
	Target  string        `json:"target"`  // 目标标识（如 host:port / 服务名）
	Command string        `json:"command"` // 模拟执行器中的命令键
	Timeout time.Duration `json:"timeout"` // 单次动作超时；0 表示使用引擎默认值
}

// ProbeSpec 描述一条稳态判据所需的探测。
type ProbeSpec struct {
	Name       string        `json:"name"`
	Timeout    time.Duration `json:"timeout"`
	MinSamples int           `json:"min_samples"` // 至少需要的有效样本数（依据不足时不得通过）
}

// RuleSpec 描述一条确定性的判定规则。
// 判定完全由样本与规则参数决定，不引入随机或时间相关的默认通过。
type RuleSpec struct {
	Probe  string `json:"probe"`
	Metric string `json:"metric"`
	// Operator 与 Threshold 共同表达“稳态保持必须满足的条件”：
	// 例如 operator="<="、threshold=0.05 表示 error_rate 必须 <= 0.05，
	// 样本不满足该条件即记为一次越限（breach）。
	Operator    string  `json:"operator"` // ">=" , "<=" , ">" , "<" , "=="
	Threshold   float64 `json:"threshold"`
	MaxBreaches int     `json:"max_breaches"` // 允许越限的样本数上限（含）
}

// HealPolicy 描述自愈联动策略。
type HealPolicy struct {
	MaxRounds     int         `json:"max_rounds"`     // 自愈轮次硬上限，防止无限重试
	RecoverAction *ActionSpec `json:"recover_action"` // 每轮触发的自愈动作（同名动作不重复触发）
	RecheckProbes []ProbeSpec `json:"recheck_probes"` // 每轮后的复核探测
	RecheckRules  []RuleSpec  `json:"recheck_rules"`  // 复核规则
}

// TargetSpec 描述针对单个目标的一次完整实验。
type TargetSpec struct {
	ID          string      `json:"id"`
	Target      string      `json:"target"`
	Inject      ActionSpec  `json:"inject"`
	Recover     ActionSpec  `json:"recover"`
	ReachableBy string      `json:"reachable_by"` // 注入前可达性探测使用的探测名
	Probes      []ProbeSpec `json:"probes"`
	Rules       []RuleSpec  `json:"rules"`
	Heal        *HealPolicy `json:"heal,omitempty"`
}

// Plan 是一次编排运行的完整计划。
type Plan struct {
	ID          string        `json:"id"`
	Concurrency int           `json:"concurrency"` // 目标级并发上限，必须 >= 1
	RunTimeout  time.Duration `json:"run_timeout"` // 整体运行超时；0 表示不限
	Targets     []TargetSpec  `json:"targets"`
}

// Sample 是一次探测得到的确定性指标样本。
type Sample struct {
	At     time.Time          `json:"at"`
	Values map[string]float64 `json:"values"`
}

// Event 是追加写的审计事件，保证过程可解释。
type Event struct {
	Seq      int            `json:"seq"`
	At       time.Time      `json:"at"`
	TargetID string         `json:"target_id"`
	Kind     string         `json:"kind"`
	Detail   map[string]any `json:"detail,omitempty"`
}

// TargetResult 是单个目标的最终实验结果。
type TargetResult struct {
	TargetID      string        `json:"target_id"`
	Phase         Phase         `json:"phase"`
	Verdict       Verdict       `json:"verdict"`
	Failure       FailureReason `json:"failure_reason"`
	FailureDetail string        `json:"failure_detail,omitempty"`
	Cleanup       CleanupResult `json:"cleanup"`
	HealRounds    int           `json:"heal_rounds"`
	Events        []Event       `json:"events"`
}

// Report 是一次计划运行的汇总报告。
type Report struct {
	PlanID   string         `json:"plan_id"`
	At       time.Time      `json:"at"`
	Results  []TargetResult `json:"results"`
	TimedOut bool           `json:"timed_out"`
	Canceled bool           `json:"canceled"`
}
