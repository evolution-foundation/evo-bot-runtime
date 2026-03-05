package repository_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	goredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/go-redsync/redsync/v4"
	"github.com/redis/go-redis/v9"

	brtErrors "github.com/EvolutionAPI/evo-bot-runtime/internal/errors"
	"github.com/EvolutionAPI/evo-bot-runtime/internal/testhelpers"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/repository"
)

func TestMain(m *testing.M) {
	rdb := redis.NewClient(testhelpers.RedisOptions())
	rdb.FlushDB(context.Background())
	rdb.Close()
	os.Exit(m.Run())
}

func setupTestRepo(t *testing.T) (repository.PipelineRepository, func()) {
	t.Helper()
	rdb := redis.NewClient(testhelpers.RedisOptions())

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	pool := goredis.NewPool(rdb)
	rs := redsync.New(pool)
	repo := repository.NewPipelineRepository(rdb, rs)

	cleanup := func() {
		// FlushDB only when TEST_REDIS_FLUSH=1 — unsafe on shared Redis instances.
		// Tests use unique pair IDs per function so cross-test pollution is negligible.
		if os.Getenv("TEST_REDIS_FLUSH") == "1" {
			rdb.FlushDB(context.Background())
		}
		rdb.Close()
	}
	return repo, cleanup
}

func TestSetState_GetState_RoundTrip(t *testing.T) {
	repo, cleanup := setupTestRepo(t)
	defer cleanup()

	ctx := context.Background()
	var contactID, convID int64 = 100, 200

	state := &model.PipelineState{
		Stage:     model.StageDebounce,
		CreatedAt: time.Now().UTC().Truncate(time.Second),
	}

	if err := repo.SetState(ctx, contactID, convID, state); err != nil {
		t.Fatalf("SetState: %v", err)
	}

	got, err := repo.GetState(ctx, contactID, convID)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if got == nil {
		t.Fatal("GetState: expected state, got nil")
	}
	if got.Stage != state.Stage {
		t.Errorf("Stage: got %q, want %q", got.Stage, state.Stage)
	}
	if !got.CreatedAt.Equal(state.CreatedAt) {
		t.Errorf("CreatedAt: got %v, want %v", got.CreatedAt, state.CreatedAt)
	}
}

func TestGetState_MissingKey_ReturnsNil(t *testing.T) {
	repo, cleanup := setupTestRepo(t)
	defer cleanup()

	ctx := context.Background()

	got, err := repo.GetState(ctx, 9999, 9999)
	if err != nil {
		t.Fatalf("GetState on missing key: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for missing key, got %+v", got)
	}
}

func TestAppendToBuffer_PreservesOrder(t *testing.T) {
	repo, cleanup := setupTestRepo(t)
	defer cleanup()

	ctx := context.Background()
	var contactID, convID int64 = 101, 201

	messages := []string{"first", "second", "third"}
	for _, msg := range messages {
		if err := repo.AppendToBuffer(ctx, contactID, convID, msg); err != nil {
			t.Fatalf("AppendToBuffer(%q): %v", msg, err)
		}
	}

	got, err := repo.GetBuffer(ctx, contactID, convID)
	if err != nil {
		t.Fatalf("GetBuffer: %v", err)
	}
	if len(got) != len(messages) {
		t.Fatalf("GetBuffer: got %d entries, want %d", len(got), len(messages))
	}
	for i, want := range messages {
		if got[i] != want {
			t.Errorf("buffer[%d]: got %q, want %q", i, got[i], want)
		}
	}
}

func TestGetBuffer_EmptyKey_ReturnsEmpty(t *testing.T) {
	repo, cleanup := setupTestRepo(t)
	defer cleanup()

	ctx := context.Background()

	got, err := repo.GetBuffer(ctx, 8888, 8888)
	if err != nil {
		t.Fatalf("GetBuffer on empty key: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty slice, got %v", got)
	}
}

func TestSetTimer_DeleteTimer_TimerExists(t *testing.T) {
	repo, cleanup := setupTestRepo(t)
	defer cleanup()

	ctx := context.Background()
	var contactID, convID int64 = 102, 202

	// Timer should not exist yet
	exists, err := repo.TimerExists(ctx, contactID, convID)
	if err != nil {
		t.Fatalf("TimerExists: %v", err)
	}
	if exists {
		t.Fatal("expected timer to not exist")
	}

	// Set timer with 5s TTL
	if err := repo.SetTimer(ctx, contactID, convID, 5*time.Second); err != nil {
		t.Fatalf("SetTimer: %v", err)
	}

	// Timer should exist
	exists, err = repo.TimerExists(ctx, contactID, convID)
	if err != nil {
		t.Fatalf("TimerExists after set: %v", err)
	}
	if !exists {
		t.Fatal("expected timer to exist after SetTimer")
	}

	// Delete timer
	if err := repo.DeleteTimer(ctx, contactID, convID); err != nil {
		t.Fatalf("DeleteTimer: %v", err)
	}

	// Timer should not exist anymore
	exists, err = repo.TimerExists(ctx, contactID, convID)
	if err != nil {
		t.Fatalf("TimerExists after delete: %v", err)
	}
	if exists {
		t.Fatal("expected timer to not exist after DeleteTimer")
	}
}

func TestClearState_DeletesAllKeys(t *testing.T) {
	repo, cleanup := setupTestRepo(t)
	defer cleanup()

	ctx := context.Background()
	var contactID, convID int64 = 103, 203

	// Set state, buffer, timer
	_ = repo.SetState(ctx, contactID, convID, &model.PipelineState{Stage: model.StageAI})
	_ = repo.AppendToBuffer(ctx, contactID, convID, "msg")
	_ = repo.SetTimer(ctx, contactID, convID, 30*time.Second)

	if err := repo.ClearState(ctx, contactID, convID); err != nil {
		t.Fatalf("ClearState: %v", err)
	}

	state, err := repo.GetState(ctx, contactID, convID)
	if err != nil || state != nil {
		t.Errorf("expected nil state after ClearState, got %v (err: %v)", state, err)
	}

	buf, err := repo.GetBuffer(ctx, contactID, convID)
	if err != nil || len(buf) != 0 {
		t.Errorf("expected empty buffer after ClearState, got %v (err: %v)", buf, err)
	}

	exists, err := repo.TimerExists(ctx, contactID, convID)
	if err != nil || exists {
		t.Errorf("expected timer gone after ClearState, exists=%v (err: %v)", exists, err)
	}
}

func TestAcquireLock_Contention(t *testing.T) {
	repo, cleanup := setupTestRepo(t)
	defer cleanup()

	ctx := context.Background()
	var contactID, convID int64 = 104, 204
	var wg sync.WaitGroup
	results := make([]error, 2)
	mutexes := make([]repository.Mutex, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			mu, err := repo.AcquireLock(ctx, contactID, convID)
			if err != nil {
				results[idx] = err
				return
			}
			mutexes[idx] = mu
			// Hold lock briefly
			time.Sleep(50 * time.Millisecond)
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	// Exactly one must have failed with ErrLockFailed
	failures := 0
	for _, err := range results {
		if errors.Is(err, brtErrors.ErrLockFailed) {
			failures++
		}
	}
	if failures != 1 {
		t.Errorf("expected exactly 1 goroutine to fail with ErrLockFailed, got %d failures (results: %v)", failures, results)
	}
}
