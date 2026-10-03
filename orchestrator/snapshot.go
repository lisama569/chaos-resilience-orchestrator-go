package orchestrator

import (
	"sort"
	"time"
)

// TargetProgress 是单个目标在某一时刻的进度视图。
// 所有字段来自同一次持锁快照，阶段、结论、自愈轮次、清理状态与事件
// 彼此一致，不会出现“阶段已推进但结论还是旧值”的拼接读。
type TargetProgress struct {
	TargetID      string        `json:"target_id"`
	Phase         Phase         `json:"phase"`
	Verdict       Verdict       `json:"verdict"`
	Failure       FailureReason `json:"failure_reason"`
	FailureDetail string        `json:"failure_detail,omitempty"`
	Cleanup       CleanupResult `json:"cleanup"`
	HealRounds    int           `json:"heal_rounds"`
	FaultInjected bool          `json:"fault_injected"`
	UpdatedAt     time.Time     `json:"updated_at"`
	Events        []Event       `json:"events"`
}

// PlanProgress 是一次计划运行在某一时刻的一致视图。
// 查询是只读的：不修改任何状态，不参与执行路径的排序，
// 因此查询频率不会影响实验过程与最终结果。
type PlanProgress struct {
	PlanID    string           `json:"plan_id"`
	AttemptID string           `json:"attempt_id"`
	Running   bool             `json:"running"` // 引擎当前是否有正在执行的动作
	Started   bool             `json:"started"` // 是否已加载/初始化过运行状态
	UpdatedAt time.Time        `json:"updated_at"`
	Targets   []TargetProgress `json:"targets"` // 按目标 ID 排序，保证视图确定
}

// Snapshot 返回当前运行进度的一致快照。
//
// 实现要点：
//   - 在与状态写入相同的互斥锁下一次性拷贝全部字段，保证是单一时刻的视图；
//   - 深拷贝事件切片与事件详情，调用方对快照的任何修改都不会污染运行状态；
//   - 锁内只做内存拷贝，不做任何 I/O，因此查询本身不会拖慢正在执行的动作；
//   - 引擎尚未开始运行（从未调用 Run）时返回 Started=false 的零值视图。
func (e *Engine) Snapshot() PlanProgress {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()

	p := PlanProgress{Running: e.running}
	if e.state == nil {
		return p
	}
	p.Started = true
	p.PlanID = e.state.PlanID
	p.AttemptID = e.state.AttemptID
	p.UpdatedAt = e.state.UpdatedAt

	ids := make([]string, 0, len(e.state.Targets))
	for id := range e.state.Targets {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	p.Targets = make([]TargetProgress, 0, len(ids))
	for _, id := range ids {
		st := e.state.Targets[id]
		p.Targets = append(p.Targets, TargetProgress{
			TargetID:      st.ID,
			Phase:         st.Phase,
			Verdict:       st.Verdict,
			Failure:       st.Failure,
			FailureDetail: st.FailureDetail,
			Cleanup:       st.Cleanup,
			HealRounds:    st.HealRounds,
			FaultInjected: st.FaultInjected,
			UpdatedAt:     st.UpdatedAt,
			Events:        copyEvents(st.Events),
		})
	}
	return p
}

// copyEvents 深拷贝事件切片（含详情 map），隔离调用方与运行状态。
func copyEvents(evs []Event) []Event {
	if evs == nil {
		return nil
	}
	out := make([]Event, len(evs))
	for i, ev := range evs {
		out[i] = ev
		if ev.Detail != nil {
			d := make(map[string]any, len(ev.Detail))
			for k, v := range ev.Detail {
				d[k] = v
			}
			out[i].Detail = d
		}
	}
	return out
}
