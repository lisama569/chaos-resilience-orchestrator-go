package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
)

// FileStore 使用 JSON 文件 + 原子重命名实现持久化，
// 以单调递增的 Version 做 CAS：版本不符一律拒绝，杜绝状态回退/重复上报被静默接受。
type FileStore struct {
	dir string
}

// NewFileStore 打开（必要时创建）状态目录。
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(filepath.Join(dir, "experiments"), 0o755); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) path(id model.ExperimentID) string {
	return filepath.Join(s.dir, "experiments", string(id)+".json")
}

func (s *FileStore) lockPath() string { return filepath.Join(s.dir, ".lock") }

// withLock 对整个存储加文件锁，串行化并发提交。
func (s *FileStore) withLock(fn func() error) error {
	f, err := os.OpenFile(s.lockPath(), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return fn()
}

func writeAtomic(path string, st *model.ExperimentState) error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readFile(path string) (*model.ExperimentState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var st model.ExperimentState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("corrupt state file %s: %w", path, err)
	}
	return &st, nil
}

// Create 新建实验；已存在同名实验则报错，避免重复创建覆盖状态。
func (s *FileStore) Create(ctx context.Context, st *model.ExperimentState) error {
	return s.withLock(func() error {
		if _, err := os.Stat(s.path(st.ID)); err == nil {
			return fmt.Errorf("experiment %s already exists", st.ID)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		st.Version = 1
		return writeAtomic(s.path(st.ID), st)
	})
}

// Load 读取实验状态。
func (s *FileStore) Load(ctx context.Context, id model.ExperimentID) (*model.ExperimentState, error) {
	var out *model.ExperimentState
	err := s.withLock(func() error {
		st, err := readFile(s.path(id))
		if err != nil {
			return err
		}
		out = st
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Commit 提交新版本：必须严格满足 expectedVersion + 1，
// 版本冲突（含并发提交、重复上报、状态回退）一律返回 ErrVersionConflict。
func (s *FileStore) Commit(ctx context.Context, st *model.ExperimentState, expectedVersion int) (*model.ExperimentState, error) {
	var out *model.ExperimentState
	err := s.withLock(func() error {
		cur, err := readFile(s.path(st.ID))
		if err != nil {
			return err
		}
		if cur.Version != expectedVersion {
			return fmt.Errorf("%w: current=%d, expected=%d",
				model.ErrVersionConflict, cur.Version, expectedVersion)
		}
		// 新版本号由存储层单调分配，调用方无法伪造或回退。
		st.Version = expectedVersion + 1
		if err := writeAtomic(s.path(st.ID), st); err != nil {
			return err
		}
		cp := *st
		out = &cp
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// List 按 ID 排序列出全部实验。
func (s *FileStore) List(ctx context.Context) ([]*model.ExperimentState, error) {
	var out []*model.ExperimentState
	err := s.withLock(func() error {
		entries, err := os.ReadDir(filepath.Join(s.dir, "experiments"))
		if err != nil {
			return err
		}
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			if filepath.Ext(e.Name()) == ".json" {
				names = append(names, e.Name())
			}
		}
		sort.Strings(names)
		for _, n := range names {
			st, err := readFile(filepath.Join(s.dir, "experiments", n))
			if err != nil {
				return err
			}
			out = append(out, st)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
