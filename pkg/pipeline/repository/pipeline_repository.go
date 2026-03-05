package repository

import (
	"context"
	"time"

	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
)

// Mutex is returned by AcquireLock. Caller must defer Unlock().
// Defined as interface to avoid leaking redsync types (GEAR R04).
type Mutex interface {
	Unlock() (bool, error)
}

// PipelineRepository handles all Redis state for pipeline execution.
type PipelineRepository interface {
	GetState(ctx context.Context, contactID, conversationID int64) (*model.PipelineState, error)
	SetState(ctx context.Context, contactID, conversationID int64, state *model.PipelineState) error
	ClearState(ctx context.Context, contactID, conversationID int64) error

	AppendToBuffer(ctx context.Context, contactID, conversationID int64, content string) error
	GetBuffer(ctx context.Context, contactID, conversationID int64) ([]string, error)

	SetTimer(ctx context.Context, contactID, conversationID int64, ttl time.Duration) error
	DeleteTimer(ctx context.Context, contactID, conversationID int64) error
	TimerExists(ctx context.Context, contactID, conversationID int64) (bool, error)

	AcquireLock(ctx context.Context, contactID, conversationID int64) (Mutex, error)

	// ScanStates returns up to maxPairs contactID+conversationID pairs that have a state key in Redis.
	// Used by PipelineService.Start() for recovery after restart (NFR-01).
	// If more keys exist than maxPairs, the result is truncated and a warning is logged.
	ScanStates(ctx context.Context, maxPairs int) ([]model.PairID, error)

	Ping(ctx context.Context) error
}
