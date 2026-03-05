package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-redsync/redsync/v4"
	goredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"

	brtErrors "github.com/EvolutionAPI/evo-bot-runtime/internal/errors"
	"github.com/EvolutionAPI/evo-bot-runtime/internal/testhelpers"
	aiIface "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/service"
	aiModel "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
	debounceService "github.com/EvolutionAPI/evo-bot-runtime/pkg/debounce/service"
	dispatchIface "github.com/EvolutionAPI/evo-bot-runtime/pkg/dispatch/service"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/repository"
)

// --- mock helpers ---

type mockMutex struct{}

func (m *mockMutex) Unlock() (bool, error) { return true, nil }

// mockAIAdapter implements aiIface.AIAdapter for testing.
type mockAIAdapter struct {
	callFn func(ctx context.Context, req *aiModel.A2ARequest) (*aiModel.NormalizedResponse, error)
}

func (m *mockAIAdapter) Call(ctx context.Context, req *aiModel.A2ARequest) (*aiModel.NormalizedResponse, error) {
	return m.callFn(ctx, req)
}

// compile-time interface check
var _ aiIface.AIAdapter = (*mockAIAdapter)(nil)

// mockDispatchEngine implements dispatchIface.DispatchEngine for testing.
type mockDispatchEngine struct {
	dispatchFn func(ctx context.Context, contactID, conversationID int64, content string, cfg model.BotConfig, postbackURL string) error
}

func (m *mockDispatchEngine) Dispatch(ctx context.Context, contactID, conversationID int64, content string, cfg model.BotConfig, postbackURL string) error {
	return m.dispatchFn(ctx, contactID, conversationID, content, cfg, postbackURL)
}

var _ dispatchIface.DispatchEngine = (*mockDispatchEngine)(nil) // compile-time check

type mockDebounce struct {
	startCalled bool
	resetCalled bool
	startErr    error
}

func (m *mockDebounce) Start(_ context.Context, _, _ int64, _ string, _ model.BotConfig) error {
	m.startCalled = true
	return m.startErr
}
func (m *mockDebounce) Reset(_ context.Context, _, _ int64, _ string, _ model.BotConfig) error {
	m.resetCalled = true
	return nil
}
func (m *mockDebounce) GetBuffer(_ context.Context, _, _ int64) (string, error) { return "", nil }
func (m *mockDebounce) TimerExists(_ context.Context, _, _ int64) (bool, error) { return false, nil }

// mockFailLockRepo delegates all calls to a real repo but always fails AcquireLock.
type mockFailLockRepo struct {
	repository.PipelineRepository
}

func (m *mockFailLockRepo) AcquireLock(_ context.Context, _, _ int64) (repository.Mutex, error) {
	return nil, errors.New("lock unavailable")
}


// --- setup ---

func TestMain(m *testing.M) {
	rdb := redis.NewClient(testhelpers.RedisOptions())
	rdb.FlushDB(context.Background())
	rdb.Close()
	os.Exit(m.Run())
}

func newTestRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	return redis.NewClient(testhelpers.RedisOptions())
}

func setupRealRepo(t *testing.T) repository.PipelineRepository {
	t.Helper()
	rdb := newTestRedisClient(t)
	pool := goredis.NewPool(rdb)
	rs := redsync.New(pool)
	repo := repository.NewPipelineRepository(rdb, rs)
	t.Cleanup(func() {
		if os.Getenv("TEST_REDIS_FLUSH") == "1" {
			rdb.FlushDB(context.Background())
		}
		rdb.Close()
	})
	return repo
}

// setupSvcWithAIAndDispatch is the base helper: wires real Redis repo + real DebounceEngine
// + provided AI and dispatch adapters.
func setupSvcWithAIAndDispatch(t *testing.T, ai aiIface.AIAdapter, dispatch dispatchIface.DispatchEngine) (*pipelineService, *redis.Client) {
	t.Helper()
	rdb := newTestRedisClient(t)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	pool := goredis.NewPool(rdb)
	rs := redsync.New(pool)
	repo := repository.NewPipelineRepository(rdb, rs)
	debounceEng := debounceService.NewDebounceEngine(repo)
	svc := &pipelineService{repo: repo, debounce: debounceEng, aiAdapter: ai, dispatchEng: dispatch}
	t.Cleanup(func() {
		if os.Getenv("TEST_REDIS_FLUSH") == "1" {
			rdb.FlushDB(context.Background())
		}
		rdb.Close()
	})
	return svc, rdb
}

// setupSvcWithAI is the helper for Story 2.x/3.x tests: uses a blocking dispatch so
// that existing tests checking for StageDispatch see it before dispatch completes.
func setupSvcWithAI(t *testing.T, ai aiIface.AIAdapter) (*pipelineService, *redis.Client) {
	blockingDispatch := &mockDispatchEngine{
		dispatchFn: func(ctx context.Context, _, _ int64, _ string, _ model.BotConfig, _ string) error {
			<-ctx.Done()
			return brtErrors.ErrDispatchInterrupted
		},
	}
	return setupSvcWithAIAndDispatch(t, ai, blockingDispatch)
}

func setupSvc(t *testing.T) (*pipelineService, *mockDebounce) {
	t.Helper()
	repo := setupRealRepo(t)
	mock := &mockDebounce{}
	svc := &pipelineService{repo: repo, debounce: mock, aiAdapter: nil}
	return svc, mock
}

// --- tests ---

func TestProcess_NewPair_StartsDebounce(t *testing.T) {
	svc, mock := setupSvc(t)
	ctx := context.Background()
	event := &model.MessageEvent{
		ContactID: 1, ConversationID: 1,
		MessageContent: "hi", BotConfig: model.BotConfig{DebounceTime: 3},
	}

	if err := svc.Process(ctx, event); err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	if !mock.startCalled {
		t.Error("DebounceEngine.Start must be called for new pair")
	}

	state, _ := svc.repo.GetState(ctx, 1, 1)
	if state == nil || state.Stage != model.StageDebounce {
		t.Errorf("state.Stage = %v, want StageDebounce", state)
	}

	if _, ok := svc.entries.Load(pairKey(1, 1)); !ok {
		t.Error("pipelineEntry must be stored in entries map")
	}
}

func TestProcess_StageIncoming_StartsDebounce(t *testing.T) {
	// This is the real production path: handler writes StageIncoming before launching
	// the goroutine; Process must treat it identically to state == nil (startDebounce).
	svc, mock := setupSvc(t)
	ctx := context.Background()

	_ = svc.repo.SetState(ctx, 10, 10, &model.PipelineState{Stage: model.StageIncoming, CreatedAt: time.Now()})

	event := &model.MessageEvent{
		ContactID: 10, ConversationID: 10,
		MessageContent: "hi", BotConfig: model.BotConfig{DebounceTime: 3},
	}
	if err := svc.Process(ctx, event); err != nil {
		t.Fatalf("Process returned error: %v", err)
	}
	if !mock.startCalled {
		t.Error("DebounceEngine.Start must be called when state is StageIncoming")
	}
	state, _ := svc.repo.GetState(ctx, 10, 10)
	if state == nil || state.Stage != model.StageDebounce {
		t.Errorf("state.Stage = %v, want StageDebounce after StageIncoming", state)
	}
}

func TestProcess_StageDebounce_Resets(t *testing.T) {
	svc, mock := setupSvc(t)
	ctx := context.Background()
	event := &model.MessageEvent{
		ContactID: 2, ConversationID: 2,
		MessageContent: "first", BotConfig: model.BotConfig{DebounceTime: 3},
	}

	_ = svc.Process(ctx, event) // creates StageDebounce

	event.MessageContent = "second"
	if err := svc.Process(ctx, event); err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	if !mock.resetCalled {
		t.Error("DebounceEngine.Reset must be called for existing StageDebounce pair")
	}
}

func TestProcess_StageAI_CancelsAndRestartsDebounce(t *testing.T) {
	svc, mock := setupSvc(t)
	ctx := context.Background()

	_ = svc.repo.SetState(ctx, 3, 3, &model.PipelineState{Stage: model.StageAI, CreatedAt: time.Now()})

	cancelled := false
	svc.entries.Store(pairKey(3, 3), pipelineEntry{
		ctx:    ctx,
		cancel: func() { cancelled = true },
	})

	event := &model.MessageEvent{
		ContactID: 3, ConversationID: 3,
		MessageContent: "interrupt", BotConfig: model.BotConfig{DebounceTime: 3},
	}
	if err := svc.Process(ctx, event); err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	if !cancelled {
		t.Error("existing cancel func must be invoked when interrupting StageAI")
	}
	if !mock.startCalled {
		t.Error("DebounceEngine.Start must be called after cancelling AI stage")
	}
}

func TestCancel_InvokesCancelAndClearsState(t *testing.T) {
	svc, _ := setupSvc(t)
	ctx := context.Background()

	_ = svc.repo.SetState(ctx, 4, 4, &model.PipelineState{Stage: model.StageDebounce, CreatedAt: time.Now()})

	cancelled := false
	svc.entries.Store(pairKey(4, 4), pipelineEntry{
		ctx:    ctx,
		cancel: func() { cancelled = true },
	})

	if err := svc.Cancel(4, 4); err != nil {
		t.Fatalf("Cancel returned error: %v", err)
	}
	if !cancelled {
		t.Error("cancel func must be invoked")
	}

	state, _ := svc.repo.GetState(ctx, 4, 4)
	if state != nil {
		t.Error("state must be cleared after Cancel")
	}
}

func TestProcess_LockFailed_ReturnsErrLockFailed(t *testing.T) {
	repo := setupRealRepo(t)
	failRepo := &mockFailLockRepo{PipelineRepository: repo}
	mock := &mockDebounce{}
	svc := &pipelineService{repo: failRepo, debounce: mock}

	event := &model.MessageEvent{
		ContactID: 5, ConversationID: 5,
		MessageContent: "hi", BotConfig: model.BotConfig{DebounceTime: 3},
	}

	err := svc.Process(context.Background(), event)
	if !errors.Is(err, brtErrors.ErrLockFailed) {
		t.Errorf("expected ErrLockFailed, got %v", err)
	}
	if mock.startCalled {
		t.Error("DebounceEngine.Start must NOT be called when lock fails")
	}
}

func TestRecoverPipeline_ClearsStateAndEntry(t *testing.T) {
	svc, _ := setupSvc(t)
	ctx := context.Background()

	_ = svc.repo.SetState(ctx, 6, 6, &model.PipelineState{Stage: model.StageAI, CreatedAt: time.Now()})
	svc.entries.Store(pairKey(6, 6), pipelineEntry{ctx: ctx, cancel: func() {}})

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer svc.recoverPipeline(6, 6)
		panic("test panic")
	}()
	<-done

	state, _ := svc.repo.GetState(ctx, 6, 6)
	if state != nil {
		t.Error("ClearState must be called after panic recovery")
	}
	if _, ok := svc.entries.Load(pairKey(6, 6)); ok {
		t.Error("entry must be deleted from entries map after panic recovery")
	}
}

// setupSvcFull returns a pipelineService wired with real repo + real DebounceEngine.
// Uses a blocking mock AI adapter that waits for ctx cancellation and returns
// ErrPipelineCancelled — this keeps state at StageAI so tests that only check the
// StageAI transition (not the AI outcome) remain correct.
func setupSvcFull(t *testing.T) (*pipelineService, *redis.Client) {
	blockingAI := &mockAIAdapter{
		callFn: func(ctx context.Context, _ *aiModel.A2ARequest) (*aiModel.NormalizedResponse, error) {
			<-ctx.Done()
			return nil, brtErrors.ErrPipelineCancelled
		},
	}
	return setupSvcWithAI(t, blockingAI)
}

func TestProcess_ZeroDuration_SkipsDebounce(t *testing.T) {
	// Use real DebounceEngine so we can verify no timer key is created in Redis (AC #3).
	svc, rdb := setupSvcFull(t)
	ctx := context.Background()

	event := &model.MessageEvent{
		ContactID: 50, ConversationID: 50,
		MessageContent: "instant",
		BotConfig:      model.BotConfig{DebounceTime: 0},
	}
	if err := svc.Process(ctx, event); err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	state, err := svc.repo.GetState(ctx, 50, 50)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if state == nil || state.Stage != model.StageAI {
		t.Errorf("state.Stage = %v, want StageAI", state)
	}

	// AC #3: zero-duration must never set a timer key in Redis.
	exists := rdb.Exists(ctx, "bot_runtime:timer:50:50").Val()
	if exists != 0 {
		t.Errorf("timer must not be set for DebounceTime=0, got exists=%d", exists)
	}
}

func TestPolling_TimerExpiry_AdvancesToAI(t *testing.T) {
	svc, _ := setupSvcFull(t)
	ctx := context.Background()

	event := &model.MessageEvent{
		ContactID: 60, ConversationID: 60,
		MessageContent: "hello",
		BotConfig:      model.BotConfig{DebounceTime: 1}, // 1 second TTL
	}
	if err := svc.Process(ctx, event); err != nil {
		t.Fatalf("Process: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for timer to expire and poller to advance state.
	time.Sleep(1500 * time.Millisecond)

	state, err := svc.repo.GetState(ctx, 60, 60)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if state == nil || state.Stage != model.StageAI {
		t.Errorf("state.Stage = %v, want StageAI after timer expiry", state)
	}
}

func TestPolling_MultiPair_Isolation(t *testing.T) {
	svc, _ := setupSvcFull(t)
	ctx := context.Background()

	// Pair A: 1s timer (will expire).
	if err := svc.Process(ctx, &model.MessageEvent{
		ContactID: 70, ConversationID: 70,
		MessageContent: "a",
		BotConfig:      model.BotConfig{DebounceTime: 1},
	}); err != nil {
		t.Fatalf("Process A: %v", err)
	}
	// Pair B: 60s timer (will NOT expire during test).
	if err := svc.Process(ctx, &model.MessageEvent{
		ContactID: 71, ConversationID: 71,
		MessageContent: "b",
		BotConfig:      model.BotConfig{DebounceTime: 60},
	}); err != nil {
		t.Fatalf("Process B: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(1500 * time.Millisecond)

	stateA, err := svc.repo.GetState(ctx, 70, 70)
	if err != nil {
		t.Fatalf("GetState A: %v", err)
	}
	stateB, err := svc.repo.GetState(ctx, 71, 71)
	if err != nil {
		t.Fatalf("GetState B: %v", err)
	}

	if stateA == nil || stateA.Stage != model.StageAI {
		t.Errorf("pair A: state.Stage = %v, want StageAI (timer expired)", stateA)
	}
	if stateB == nil || stateB.Stage != model.StageDebounce {
		t.Errorf("pair B: state.Stage = %v, want StageDebounce (active timer)", stateB)
	}
}

// TestStart_Recovery_AdvancesExpiredPairs simulates a restart where the debounce timer
// expired during downtime. Start() must detect the missing timer and advance to StageAI
// without any incoming message (AC: #4, NFR-01 zero message loss).
func TestStart_Recovery_AdvancesExpiredPairs(t *testing.T) {
	svc, _ := setupSvcFull(t)
	ctx := context.Background()

	// Simulate pre-restart state: StageDebounce persisted in Redis, no timer (expired during downtime).
	_ = svc.repo.SetState(ctx, 80, 80, &model.PipelineState{Stage: model.StageDebounce, CreatedAt: time.Now()})
	// Deliberately do NOT call SetTimer — simulates timer that expired before restart.

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// advanceToAI is called synchronously during recovery (timer already gone) — no sleep needed.
	state, err := svc.repo.GetState(ctx, 80, 80)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if state == nil || state.Stage != model.StageAI {
		t.Errorf("state.Stage = %v, want StageAI (timer expired before restart)", state)
	}
}

// TestStart_Recovery_SkipsActiveTimers verifies that Start() does not prematurely advance
// pairs whose debounce timer is still running at recovery time (AC: #4).
func TestStart_Recovery_SkipsActiveTimers(t *testing.T) {
	svc, _ := setupSvcFull(t)
	ctx := context.Background()

	// Simulate pre-restart state: StageDebounce with an active 60s timer.
	_ = svc.repo.SetState(ctx, 81, 81, &model.PipelineState{Stage: model.StageDebounce, CreatedAt: time.Now()})
	_ = svc.repo.SetTimer(ctx, 81, 81, 60*time.Second)

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	state, err := svc.repo.GetState(ctx, 81, 81)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if state == nil || state.Stage != model.StageDebounce {
		t.Errorf("state.Stage = %v, want StageDebounce (timer still active)", state)
	}
}

// --- AI stage tests (Story 3.2) ---

func TestAIStage_Success_AdvancesToDispatch(t *testing.T) {
	mock := &mockAIAdapter{
		callFn: func(_ context.Context, _ *aiModel.A2ARequest) (*aiModel.NormalizedResponse, error) {
			return &aiModel.NormalizedResponse{Content: "AI says hello"}, nil
		},
	}
	svc, _ := setupSvcWithAI(t, mock)
	ctx := context.Background()

	// DebounceTime=0: skips debounce, goes directly to AI stage.
	event := &model.MessageEvent{
		ContactID: 90, ConversationID: 90,
		MessageContent: "test message",
		BotConfig:      model.BotConfig{DebounceTime: 0},
	}
	if err := svc.Process(ctx, event); err != nil {
		t.Fatalf("Process returned error: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // wait for AI goroutine to complete

	state, err := svc.repo.GetState(ctx, 90, 90)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if state == nil || state.Stage != model.StageDispatch {
		t.Errorf("state.Stage = %v, want StageDispatch after successful AI call", state)
	}
}

func TestAIStage_Cancellation_DoesNotClearNewDebounce(t *testing.T) {
	mock := &mockAIAdapter{
		callFn: func(ctx context.Context, _ *aiModel.A2ARequest) (*aiModel.NormalizedResponse, error) {
			<-ctx.Done() // block until pipeline context is cancelled
			return nil, brtErrors.ErrPipelineCancelled
		},
	}
	svc, _ := setupSvcWithAI(t, mock)
	ctx := context.Background()

	// First message: DebounceTime=0 → skip debounce → enter AI stage immediately.
	event1 := &model.MessageEvent{
		ContactID: 91, ConversationID: 91,
		MessageContent: "first message",
		BotConfig:      model.BotConfig{DebounceTime: 0},
	}
	if err := svc.Process(ctx, event1); err != nil {
		t.Fatalf("Process event1: %v", err)
	}
	time.Sleep(20 * time.Millisecond) // let AI goroutine start and block

	// Second message: cancels the AI call, starts new debounce cycle.
	event2 := &model.MessageEvent{
		ContactID: 91, ConversationID: 91,
		MessageContent: "second message",
		BotConfig:      model.BotConfig{DebounceTime: 5}, // 5s debounce
	}
	if err := svc.Process(ctx, event2); err != nil {
		t.Fatalf("Process event2: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // wait for AI goroutine to handle cancellation

	// State must be StageDebounce (new cycle) — runAIStage must NOT call ClearState on cancellation.
	state, err := svc.repo.GetState(ctx, 91, 91)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if state == nil {
		t.Fatal("state key must exist — new debounce cycle must not be cleared")
	}
	if state.Stage != model.StageDebounce {
		t.Errorf("state.Stage = %v, want StageDebounce from new message", state.Stage)
	}
}

func TestAIStage_Timeout_ClearsState(t *testing.T) {
	mock := &mockAIAdapter{
		callFn: func(_ context.Context, _ *aiModel.A2ARequest) (*aiModel.NormalizedResponse, error) {
			return nil, brtErrors.ErrAITimeout
		},
	}
	svc, rdb := setupSvcWithAI(t, mock)
	ctx := context.Background()

	event := &model.MessageEvent{
		ContactID: 92, ConversationID: 92,
		MessageContent: "test",
		BotConfig:      model.BotConfig{DebounceTime: 0}, // immediate AI
	}
	if err := svc.Process(ctx, event); err != nil {
		t.Fatalf("Process returned error: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // wait for AI goroutine to handle timeout

	exists := rdb.Exists(ctx, "bot_runtime:state:92:92").Val()
	if exists != 0 {
		t.Errorf("state must be cleared after AI timeout, got exists=%d", exists)
	}
}

// --- Dispatch stage end-to-end tests (Story 4.2) ---

func TestPipeline_FullFlow_DispatchCompletes(t *testing.T) {
	mockAI := &mockAIAdapter{
		callFn: func(_ context.Context, _ *aiModel.A2ARequest) (*aiModel.NormalizedResponse, error) {
			return &aiModel.NormalizedResponse{Content: "hello from AI"}, nil
		},
	}
	mockDispatch := &mockDispatchEngine{
		dispatchFn: func(_ context.Context, _, _ int64, _ string, _ model.BotConfig, _ string) error {
			return nil
		},
	}
	svc, rdb := setupSvcWithAIAndDispatch(t, mockAI, mockDispatch)

	event := &model.MessageEvent{
		ContactID:      100,
		ConversationID: 100,
		MessageContent: "test",
		PostbackURL:    "http://postback.example.com",
		BotConfig:      model.BotConfig{DebounceTime: 0},
	}
	if err := svc.Process(context.Background(), event); err != nil {
		t.Fatalf("Process returned error: %v", err)
	}
	time.Sleep(150 * time.Millisecond) // wait for AI + dispatch goroutines

	// State must be cleared — StageDone was set then ClearState called
	exists := rdb.Exists(context.Background(), "bot_runtime:state:100:100").Val()
	if exists != 0 {
		t.Errorf("state key must be deleted after full pipeline completion, got exists=%d", exists)
	}
}

func TestPipeline_DispatchInterrupted_KeepsNewDebounce(t *testing.T) {
	mockAI := &mockAIAdapter{
		callFn: func(_ context.Context, _ *aiModel.A2ARequest) (*aiModel.NormalizedResponse, error) {
			return &aiModel.NormalizedResponse{Content: "response"}, nil
		},
	}
	dispatchDone := make(chan struct{})
	mockDispatch := &mockDispatchEngine{
		dispatchFn: func(ctx context.Context, _, _ int64, _ string, _ model.BotConfig, _ string) error {
			defer close(dispatchDone)
			<-ctx.Done() // block until pipeline context is cancelled
			return brtErrors.ErrDispatchInterrupted
		},
	}
	svc, rdb := setupSvcWithAIAndDispatch(t, mockAI, mockDispatch)

	// First message: skip debounce → AI → dispatch (blocks)
	event1 := &model.MessageEvent{
		ContactID: 101, ConversationID: 101,
		MessageContent: "first",
		PostbackURL:    "http://postback.example.com",
		BotConfig:      model.BotConfig{DebounceTime: 0},
	}
	if err := svc.Process(context.Background(), event1); err != nil {
		t.Fatalf("Process event1 returned error: %v", err)
	}
	time.Sleep(30 * time.Millisecond) // let pipeline reach dispatch stage

	// Second message: cancels dispatch, starts new 5s debounce
	event2 := &model.MessageEvent{
		ContactID: 101, ConversationID: 101,
		MessageContent: "second",
		PostbackURL:    "http://postback.example.com",
		BotConfig:      model.BotConfig{DebounceTime: 5},
	}
	if err := svc.Process(context.Background(), event2); err != nil {
		t.Fatalf("Process event2 returned error: %v", err)
	}

	// L-01: verify dispatch goroutine exits after ctx cancellation (no goroutine leak)
	select {
	case <-dispatchDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("dispatch goroutine did not exit after ctx cancellation — possible goroutine leak")
	}

	// State must be StageDebounce from new message — runDispatchStage must NOT ClearState
	b := rdb.Get(context.Background(), "bot_runtime:state:101:101").Val()
	if !strings.Contains(b, `"debounce"`) {
		t.Errorf("state must be StageDebounce from new message (runDispatchStage must not ClearState on interruption), got: %q", b)
	}
}

func TestPipeline_DispatchError_ClearsState(t *testing.T) {
	mockAI := &mockAIAdapter{
		callFn: func(_ context.Context, _ *aiModel.A2ARequest) (*aiModel.NormalizedResponse, error) {
			return &aiModel.NormalizedResponse{Content: "response"}, nil
		},
	}
	dispatchDone := make(chan struct{})
	mockDispatch := &mockDispatchEngine{
		dispatchFn: func(_ context.Context, _, _ int64, _ string, _ model.BotConfig, _ string) error {
			defer close(dispatchDone)
			return fmt.Errorf("postback server unavailable")
		},
	}
	svc, rdb := setupSvcWithAIAndDispatch(t, mockAI, mockDispatch)

	event := &model.MessageEvent{
		ContactID: 102, ConversationID: 102,
		MessageContent: "test",
		PostbackURL:    "http://postback.example.com",
		BotConfig:      model.BotConfig{DebounceTime: 0},
	}
	if err := svc.Process(context.Background(), event); err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	// L-01: verify dispatch goroutine exits after error (no goroutine leak)
	select {
	case <-dispatchDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("dispatch goroutine did not exit after dispatch error — possible goroutine leak")
	}
	time.Sleep(20 * time.Millisecond) // let runDispatchStage finish ClearState after Dispatch returns

	exists := rdb.Exists(context.Background(), "bot_runtime:state:102:102").Val()
	if exists != 0 {
		t.Errorf("state must be cleared after dispatch error, got exists=%d", exists)
	}
}

