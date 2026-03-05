package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/redis/go-redis/v9"

	brtErrors "github.com/EvolutionAPI/evo-bot-runtime/internal/errors"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
)

type redisPipelineRepository struct {
	rdb *redis.Client
	rs  *redsync.Redsync
}

// NewPipelineRepository constructs the repository. Returns interface (GEAR R03).
func NewPipelineRepository(rdb *redis.Client, rs *redsync.Redsync) PipelineRepository {
	return &redisPipelineRepository{rdb: rdb, rs: rs}
}

func (r *redisPipelineRepository) SetState(ctx context.Context, contactID, conversationID int64, state *model.PipelineState) error {
	b, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("pipeline.repository.set_state: %w", err)
	}
	return r.rdb.Set(ctx, stateKey(contactID, conversationID), b, 0).Err()
	// TTL = 0 → no expiry; cleared explicitly by ClearState
}

func (r *redisPipelineRepository) GetState(ctx context.Context, contactID, conversationID int64) (*model.PipelineState, error) {
	b, err := r.rdb.Get(ctx, stateKey(contactID, conversationID)).Bytes()
	if err == redis.Nil {
		return nil, nil // no state = no active pipeline
	}
	if err != nil {
		return nil, fmt.Errorf("pipeline.repository.get_state: %w", err)
	}
	var s model.PipelineState
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("pipeline.repository.get_state unmarshal: %w", err)
	}
	return &s, nil
}

func (r *redisPipelineRepository) ClearState(ctx context.Context, contactID, conversationID int64) error {
	keys := []string{
		stateKey(contactID, conversationID),
		bufferKey(contactID, conversationID),
		timerKey(contactID, conversationID),
	}
	return r.rdb.Del(ctx, keys...).Err()
}

func (r *redisPipelineRepository) AppendToBuffer(ctx context.Context, contactID, conversationID int64, content string) error {
	return r.rdb.RPush(ctx, bufferKey(contactID, conversationID), content).Err()
}

func (r *redisPipelineRepository) GetBuffer(ctx context.Context, contactID, conversationID int64) ([]string, error) {
	return r.rdb.LRange(ctx, bufferKey(contactID, conversationID), 0, -1).Result()
}

func (r *redisPipelineRepository) SetTimer(ctx context.Context, contactID, conversationID int64, ttl time.Duration) error {
	return r.rdb.Set(ctx, timerKey(contactID, conversationID), "1", ttl).Err()
}

func (r *redisPipelineRepository) DeleteTimer(ctx context.Context, contactID, conversationID int64) error {
	return r.rdb.Del(ctx, timerKey(contactID, conversationID)).Err()
}

func (r *redisPipelineRepository) TimerExists(ctx context.Context, contactID, conversationID int64) (bool, error) {
	n, err := r.rdb.Exists(ctx, timerKey(contactID, conversationID)).Result()
	return n > 0, err
}

func (r *redisPipelineRepository) AcquireLock(ctx context.Context, contactID, conversationID int64) (Mutex, error) {
	// WithTries(1): fail immediately if lock is already held — no retry spinning.
	// This enforces exactly-one-pipeline semantics per contact+conversation pair.
	mutex := r.rs.NewMutex(lockKey(contactID, conversationID), redsync.WithTries(1))
	if err := mutex.LockContext(ctx); err != nil {
		return nil, brtErrors.ErrLockFailed
	}
	return mutex, nil
}

func (r *redisPipelineRepository) ScanStates(ctx context.Context, maxPairs int) ([]model.PairID, error) {
	pairs := make([]model.PairID, 0, min(maxPairs, 256))
	var cursor uint64
	for {
		keys, next, err := r.rdb.Scan(ctx, cursor, "bot_runtime:state:*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf("pipeline.repository.scan_states: %w", err)
		}
		for _, key := range keys {
			if len(pairs) >= maxPairs {
				slog.Warn("pipeline.repository.scan_states.limit_reached",
					"max", maxPairs,
				)
				return pairs, nil
			}
			// Format: bot_runtime:state:{contactID}:{conversationID}
			parts := strings.Split(key, ":")
			if len(parts) != 4 {
				continue
			}
			contactID, err1 := strconv.ParseInt(parts[2], 10, 64)
			convID, err2 := strconv.ParseInt(parts[3], 10, 64)
			if err1 != nil || err2 != nil {
				continue
			}
			pairs = append(pairs, model.PairID{ContactID: contactID, ConversationID: convID})
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return pairs, nil
}

func (r *redisPipelineRepository) Ping(ctx context.Context) error {
	return r.rdb.Ping(ctx).Err()
}

// Key helper functions — immutable schema: bot_runtime:{type}:{contact_id}:{conversation_id}

func stateKey(contactID, conversationID int64) string {
	return fmt.Sprintf("bot_runtime:state:%d:%d", contactID, conversationID)
}

func bufferKey(contactID, conversationID int64) string {
	return fmt.Sprintf("bot_runtime:buffer:%d:%d", contactID, conversationID)
}

func lockKey(contactID, conversationID int64) string {
	return fmt.Sprintf("bot_runtime:lock:%d:%d", contactID, conversationID)
}

func timerKey(contactID, conversationID int64) string {
	return fmt.Sprintf("bot_runtime:timer:%d:%d", contactID, conversationID)
}
