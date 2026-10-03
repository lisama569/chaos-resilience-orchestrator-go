package orchestrator

import (
	"sort"
	"time"
)

// ProgressEventLimit 是进度视图中每个目标携带的最近事件条数上限。
// 快照内事件序号保证连续（是完整事件流的一个尾部），不会出现缺段。
const ProgressEventLimit = 20

// RunStatus 描述一次计划运行的生命周期，供运行期进度查询使用。
type RunStatus string

const (
	// RunNotStarted 尚未开始（或从未运行）。
	RunNotStarted RunStatus = "not_started"
	// RunRunning 正在按并发上限执行中。
	RunRunning RunStatus = "running"
	// RunFinished 本轮运行已结束（报告已产出）。
	RunFinished RunStatus = "finished"
)

// TargetProgress 是单个目标在某一时刻的一致进度视图：
// 阶段、稳态结论、自愈轮次、清理状态与最近事件来自同一次状态发布，
// 不会出现阶段与结论对不上、事件缺段或读到写了一半的内容。
type TargetProgress struct {
	TargetID      string        `json:"target_id"`
	Phase         Phase         `json:"phase"`
	Verdict       Verdict       `json:"verdict"`
	Failure       FailureReason `json:"failure_reason"`
	FailureDetail string        `json:"failure_detail,omitempty"`
	Cleanup       CleanupResult `json:"cleanup"`
	HealRounds    int           `json:"heal_rounds"`
	FaultInjected bool          `json:"fault_injected"`
	// RecentEvents 是该目标事件流的尾部（最多 ProgressEventLimit 条），
	// 序号单调递增且连续。
	RecentEvents []Event   `json:"recent_events,omitempty"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// PlanProgress 是整个计划在某一时刻的一致快照。
type PlanProgress struct {
	PlanID    string           `json:"plan_id"`
	AttemptID string           `json:"attempt_id,omitempty"`
	Status    RunStatus        `json:"status"`
	Targets   []TargetProgress `json:"targets"` // 按 TargetID 排序，输出稳定
	UpdatedAt time.Time        `json:"updated_at"`
}

// clonePlanState 深拷贝计划状态。拷贝在持锁（stateMu）状态下进行，
// 拷贝完成后即视为不可变对象发布，查询方无需再加锁。
func clonePlanState(ps *PlanState) *PlanState {
	if ps == nil {
		return nil
	}
	cp := &PlanState{
		PlanID:    ps.PlanID,
		AttemptID: ps.AttemptID,
		EventSeq:  ps.EventSeq,
		UpdatedAt: ps.UpdatedAt,
		Targets:   make(map[string]*TargetState, len(ps.Targets)),
	}
	for id, t := range ps.Targets {
		tc := *t
		tc.Done = append([]StepDone(nil), t.Done...)
		tc.Events = append([]Event(nil), t.Events...)
		cp.Targets[id] = &tc
	}
	return cp
}

// publishSnapshot 在状态提交（持久化成功）后发布新的不可变快照。
// 调用方必须持有 stateMu。
func (e *Engine) publishSnapshot() {
	e.snapshot.Store(clonePlanState(e.state))
}

// progressOf 把不可变快照转换为对外进度视图。
func progressOf(ps *PlanState, status RunStatus) PlanProgress {
	p := PlanProgress{Status: status, Targets: []TargetProgress{}}
	if ps == nil {
		return p
	}
	p.PlanID = ps.PlanID
	p.AttemptID = ps.AttemptID
	p.UpdatedAt = ps.UpdatedAt
	for _, t := range ps.Targets {
		p.Targets = append(p.Targets, targetProgressOf(t))
	}
	sort.Slice(p.Targets, func(i, j int) bool { return p.Targets[i].TargetID < p.Targets[j].TargetID })
	return p
}

// targetProgressOf 从（不可变的）目标状态构造单目标进度视图。
func targetProgressOf(t *TargetState) TargetProgress {
	evs := t.Events
	if len(evs) > ProgressEventLimit {
		evs = evs[len(evs)-ProgressEventLimit:]
	}
	return TargetProgress{
		TargetID:      t.ID,
		Phase:         t.Phase,
		Verdict:       t.Verdict,
		Failure:       t.Failure,
		FailureDetail: t.FailureDetail,
		Cleanup:       t.Cleanup,
		HealRounds:    t.HealRounds,
		FaultInjected: t.FaultInjected,
		RecentEvents:  append([]Event(nil), evs...),
		UpdatedAt:     t.UpdatedAt,
	}
}

// Progress 返回计划当前进度的一致快照。
//
// 查询只读取已发布的不可变快照（原子加载），不与执行路径争抢状态锁，
// 因此任意频率的查询都不会拖慢或改变正在执行的实验。
// 运行尚未开始时返回 RunNotStarted 的空视图。
func (e *Engine) Progress() PlanProgress {
	return progressOf(e.snapshot.Load(), e.runStatus.Load().(RunStatus))
}

// TargetProgress 返回单个目标的当前进度；目标不存在时返回 ErrUnknownTarget。
func (e *Engine) TargetProgress(id string) (TargetProgress, error) {
	snap := e.snapshot.Load()
	if snap == nil {
		return TargetProgress{}, ErrNotExist
	}
	t, ok := snap.Targets[id]
	if !ok {
		return TargetProgress{}, ErrUnknownTarget
	}
	return targetProgressOf(t), nil
}
