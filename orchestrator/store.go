package orchestrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// 持久化层定义的哨兵错误：调用方必须显式处理，禁止静默接受。
var (
	// ErrPhaseRollback 表示试图把阶段从后序阶段改回前序阶段。
	ErrPhaseRollback = errors.New("phase rollback rejected")
	// ErrStaleReport 表示步骤完成上报晚于已确认状态（重复上报）。
	ErrStaleReport = errors.New("stale or duplicate completion report rejected")
	// ErrAlreadyTerminal 表示目标已是终态，拒绝再次推进。
	ErrAlreadyTerminal = errors.New("target already in terminal phase")
	// ErrNotExist 表示尚无持久化状态（首次运行）。
	ErrNotExist = errors.New("state file does not exist")
	// ErrUnknownTarget 表示状态中不存在指定目标。
	ErrUnknownTarget = errors.New("unknown target in state")
	// ErrPersistFailed 表示状态持久化失败。
	ErrPersistFailed = errors.New("state persistence failed")
)

// StepDone 记录一个已确认完成的步骤，用于重启后不重复执行。
type StepDone struct {
	Name      string    `json:"name"` // 步骤唯一名，如 "inject" / "probe:latency:0" / "heal:1"
	At        time.Time `json:"at"`
	AttemptID string    `json:"attempt_id,omitempty"` // 所属运行尝试，用于区分跨重启的重复上报
}

// TargetState 是单个目标的可持久化状态。
type TargetState struct {
	ID            string        `json:"id"`
	AttemptID     string        `json:"attempt_id"`
	Phase         Phase         `json:"phase"`
	Verdict       Verdict       `json:"verdict"`
	Failure       FailureReason `json:"failure_reason"`
	FailureDetail string        `json:"failure_detail,omitempty"`
	Cleanup       CleanupResult `json:"cleanup"`
	HealRounds    int           `json:"heal_rounds"`
	FaultInjected bool          `json:"fault_injected"` // 故障是否确认注入成功（决定是否必须清理）
	Done          []StepDone    `json:"done"`
	Events        []Event       `json:"events"`
	UpdatedAt     time.Time     `json:"updated_at"`
}

// PlanState 是整个计划的可持久化状态。
type PlanState struct {
	PlanID    string                  `json:"plan_id"`
	AttemptID string                  `json:"attempt_id"`
	Targets   map[string]*TargetState `json:"targets"`
	EventSeq  int                     `json:"event_seq"`
	UpdatedAt time.Time               `json:"updated_at"`
}

// Store 是实验状态存储。
type Store interface {
	Load() (*PlanState, error)
	Save(state *PlanState) error
}

// IsStepDone 报告某步骤是否已确认完成。
func (st *TargetState) IsStepDone(name string) bool {
	for _, d := range st.Done {
		if d.Name == name {
			return true
		}
	}
	return false
}

// MarkDone 上报一个步骤已完成。重复上报同一步骤会被明确拒绝，
// 而不是静默幂等接受，以免掩盖重复执行。
func (st *TargetState) MarkDone(name, attemptID string, at time.Time) error {
	if st.IsStepDone(name) {
		return fmt.Errorf("%w: step=%q target=%q", ErrStaleReport, name, st.ID)
	}
	st.Done = append(st.Done, StepDone{Name: name, At: at, AttemptID: attemptID})
	st.UpdatedAt = at
	return nil
}

// Advance 把目标推进到新阶段。
//
// 规则：
//   - 已是终态 -> ErrAlreadyTerminal（终态不可改写，保证完成步骤不重复执行）；
//   - 阶段顺序回退 -> ErrPhaseRollback；
//   - 仅允许从非终态进入终态（completed/aborted/cleanup_failed）。
func (st *TargetState) Advance(to Phase, at time.Time) error {
	if st.Phase.IsTerminal() {
		return fmt.Errorf("%w: target=%q phase=%s", ErrAlreadyTerminal, st.ID, st.Phase)
	}
	if phaseOrder[to] != 0 && phaseOrder[to] < phaseOrder[st.Phase] {
		return fmt.Errorf("%w: %s -> %s (target=%q)", ErrPhaseRollback, st.Phase, to, st.ID)
	}
	st.Phase = to
	st.UpdatedAt = at
	return nil
}

// Target 返回指定目标状态，不存在返回 ErrUnknownTarget。
func (ps *PlanState) Target(id string) (*TargetState, error) {
	if t, ok := ps.Targets[id]; ok {
		return t, nil
	}
	return nil, fmt.Errorf("%w: %s", ErrUnknownTarget, id)
}

// AppendEvent 以单调递增序号追加审计事件。
func (ps *PlanState) AppendEvent(at time.Time, targetID, kind string, detail map[string]any) Event {
	ps.EventSeq++
	ev := Event{Seq: ps.EventSeq, At: at, TargetID: targetID, Kind: kind, Detail: detail}
	if targetID != "" {
		if t, ok := ps.Targets[targetID]; ok {
			t.Events = append(t.Events, ev)
		}
	}
	ps.UpdatedAt = at
	return ev
}

// clonePlanState 深拷贝整个计划状态，使锁外落盘的副本与后续状态变更
// 不共享任何 map/slice 底层内存，杜绝“边写盘边改”导致的数据竞争或
// 半更新内容到达磁盘。
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
	for id, st := range ps.Targets {
		cp.Targets[id] = cloneTargetState(st)
	}
	return cp
}

// cloneTargetState 深拷贝单个目标状态（含已完成步骤、事件与事件详情）。
func cloneTargetState(st *TargetState) *TargetState {
	if st == nil {
		return nil
	}
	cp := &TargetState{
		ID:            st.ID,
		AttemptID:     st.AttemptID,
		Phase:         st.Phase,
		Verdict:       st.Verdict,
		Failure:       st.Failure,
		FailureDetail: st.FailureDetail,
		Cleanup:       st.Cleanup,
		HealRounds:    st.HealRounds,
		FaultInjected: st.FaultInjected,
		UpdatedAt:     st.UpdatedAt,
		Done:          make([]StepDone, len(st.Done)),
		Events:        make([]Event, len(st.Events)),
	}
	copy(cp.Done, st.Done)
	for i, ev := range st.Events {
		cp.Events[i] = ev
		if ev.Detail != nil {
			d := make(map[string]any, len(ev.Detail))
			for k, v := range ev.Detail {
				d[k] = v
			}
			cp.Events[i].Detail = d
		}
	}
	return cp
}

// FileStore 以单个 JSON 文件持久化状态；写入采用临时文件+原子改名，
// 避免进程在写入中途被杀死留下损坏的状态文件。
type FileStore struct {
	mu   sync.Mutex
	path string
}

// NewFileStore 创建文件存储。
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

func (s *FileStore) Load() (*PlanState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotExist
	}
	if err != nil {
		return nil, err
	}
	var ps PlanState
	if err := json.Unmarshal(data, &ps); err != nil {
		return nil, fmt.Errorf("corrupt state file %s: %w", s.path, err)
	}
	return &ps, nil
}

func (s *FileStore) Save(state *PlanState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// MemoryStore 是测试用内存存储。
type MemoryStore struct {
	mu    sync.Mutex
	state *PlanState
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func (s *MemoryStore) Load() (*PlanState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == nil {
		return nil, ErrNotExist
	}
	cp := *s.state
	return &cp, nil
}

func (s *MemoryStore) Save(state *PlanState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
	return nil
}
