package orchestrator

import (
	"sync"
	"time"
)

// slowFileStore 包装真实 FileStore，在每次 Save（真实落盘）前后插入延迟，
// 让“写盘很慢”成为可稳定观测的条件，而不是用内存桩绕过落盘路径。
// 同时记录写盘并发度：持久化必须被串行化，绝不能出现两个 Save 重叠。
type slowFileStore struct {
	inner     Store
	delay     time.Duration
	mu        sync.Mutex
	active    int
	maxActive int
	saves     int
}

func newSlowFileStore(path string, delay time.Duration) *slowFileStore {
	return &slowFileStore{inner: NewFileStore(path), delay: delay}
}

func (s *slowFileStore) Load() (*PlanState, error) { return s.inner.Load() }

func (s *slowFileStore) Save(state *PlanState) error {
	s.mu.Lock()
	s.active++
	s.saves++
	if s.active > s.maxActive {
		s.maxActive = s.active
	}
	s.mu.Unlock()

	time.Sleep(s.delay) // 模拟慢盘：期间查询必须仍能即时返回

	err := s.inner.Save(state)

	s.mu.Lock()
	s.active--
	s.mu.Unlock()
	return err
}

func (s *slowFileStore) stats() (saves, maxActive int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saves, s.maxActive
}
