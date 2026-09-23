package store

import (
	"context"

	"github.com/lisama569/chaos-resilience-orchestrator-go/internal/model"
)

// Store 是实验状态的持久化接口，基于版本号做 CAS，
// 拒绝状态回退与重复上报。
type Store interface {
	Create(ctx context.Context, st *model.ExperimentState) error
	Load(ctx context.Context, id model.ExperimentID) (*model.ExperimentState, error)
	// Commit 期望 expectedVersion 与当前版本一致，否则返回 model.ErrVersionConflict。
	Commit(ctx context.Context, st *model.ExperimentState, expectedVersion int) (*model.ExperimentState, error)
	List(ctx context.Context) ([]*model.ExperimentState, error)
}
