package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redsync/redsync/v4"
	goredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"

	"github.com/EvolutionAPI/evo-bot-runtime/internal/testhelpers"
	aiModel "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
	aiService "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/service"
	debounceService "github.com/EvolutionAPI/evo-bot-runtime/pkg/debounce/service"
	dispatchService "github.com/EvolutionAPI/evo-bot-runtime/pkg/dispatch/service"
	pipelineHandler "github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/handler"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/repository"
	pipelineService "github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/service"
)

// redisOptionsForE2E returns Redis options pointing at DB 14, one step below
// the unit test DB at 15. This prevents e2e FlushDB calls from wiping keys
// owned by concurrently running unit test packages when `go test ./...` is used.
func redisOptionsForE2E() *redis.Options {
	opt := testhelpers.RedisOptions()
	opt.DB = 14
	return opt
}

// TestMain flushes the e2e Redis DB once before the suite runs to evict any
// keys left by a previous (crashed/killed) e2e run.
func TestMain(m *testing.M) {
	rdb := redis.NewClient(redisOptionsForE2E())
	if err := rdb.Ping(context.Background()).Err(); err == nil {
		rdb.FlushDB(context.Background()) //nolint:errcheck
	}
	rdb.Close()
	os.Exit(m.Run())
}

// ── mock servers ──────────────────────────────────────────────────────────────

// mockAIServer stands in for AI Processor.
// Tests call setHandler to control per-test behavior; without a handler set it
// returns {"content":"default response"} immediately.
type mockAIServer struct {
	*httptest.Server
	mu        sync.Mutex
	handlerFn http.HandlerFunc
	calls     atomic.Int32
}

func newMockAIServer() *mockAIServer {
	m := &mockAIServer{}
	m.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.calls.Add(1)
		m.mu.Lock()
		fn := m.handlerFn
		m.mu.Unlock()
		if fn != nil {
			fn(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a2aResponse("default response")) //nolint:errcheck
	}))
	return m
}

// a2aResponse builds a JSON-RPC 2.0 A2A response with the given text content.
func a2aResponse(text string) aiModel.A2AResponse {
	return aiModel.A2AResponse{
		Result: &aiModel.A2AResult{
			Artifacts: []aiModel.A2AArtifact{
				{Parts: []aiModel.A2APart{{Type: "text", Text: text}}},
			},
		},
	}
}

func (m *mockAIServer) setHandler(fn http.HandlerFunc) {
	m.mu.Lock()
	m.handlerFn = fn
	m.mu.Unlock()
}

// mockPostbackServer records every postback call and signals calledCh for each one.
// Tests can call setHandler to override the response (default: 200 OK).
// The body is always recorded and calledCh is always signaled regardless of handler.
type mockPostbackServer struct {
	*httptest.Server
	mu        sync.Mutex
	handlerFn http.HandlerFunc
	calls     [][]byte // raw request body per call
	calledCh  chan struct{}
}

func newMockPostbackServer() *mockPostbackServer {
	m := &mockPostbackServer{calledCh: make(chan struct{}, 64)}
	m.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		m.mu.Lock()
		m.calls = append(m.calls, body)
		fn := m.handlerFn
		m.mu.Unlock()
		select {
		case m.calledCh <- struct{}{}:
		default:
		}
		if fn != nil {
			fn(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	return m
}

func (m *mockPostbackServer) setHandler(fn http.HandlerFunc) {
	m.mu.Lock()
	m.handlerFn = fn
	m.mu.Unlock()
}

// waitForCall blocks until a postback call is received or the timeout elapses.
func (m *mockPostbackServer) waitForCall(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-m.calledCh:
	case <-time.After(timeout):
		t.Fatal("timed out waiting for postback call")
	}
}

// waitForNCalls blocks until n postback calls have been received or the timeout elapses.
// The timeout applies to the whole batch, not per-call.
func (m *mockPostbackServer) waitForNCalls(t *testing.T, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for i := 0; i < n; i++ {
		select {
		case <-m.calledCh:
		case <-deadline:
			t.Fatalf("timed out waiting for postback call %d/%d", i+1, n)
		}
	}
}

// allBodies returns a snapshot of every recorded request body, safe for concurrent use.
func (m *mockPostbackServer) allBodies() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]byte, len(m.calls))
	copy(out, m.calls)
	return out
}

func (m *mockPostbackServer) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *mockPostbackServer) lastBody() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.calls) == 0 {
		return nil
	}
	return m.calls[len(m.calls)-1]
}

// ── harness ───────────────────────────────────────────────────────────────────

const testSecret = "e2e-test-secret"

// pairSeq generates unique contact+conversation IDs so tests don't share Redis keys.
var pairSeq atomic.Int64

func nextPair() (contactID, convID int64) {
	n := pairSeq.Add(1)
	return n * 1000, n*1000 + 1
}

type harness struct {
	botURL   string
	aiServer *mockAIServer
	pbServer *mockPostbackServer
	rdb      *redis.Client
}

// newHarness wires the full production stack (same order as cmd/server/main.go steps 2–9)
// replacing external HTTP dependencies with in-process test servers.
// The test is skipped automatically when Redis is not reachable.
func newHarness(t *testing.T) *harness {
	t.Helper()

	rdb := redis.NewClient(redisOptionsForE2E())
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		rdb.Close()
		t.Skipf("Redis not available: %v", err)
	}
	aiSrv := newMockAIServer()
	pbSrv := newMockPostbackServer()

	pool := goredis.NewPool(rdb)
	rs := redsync.New(pool)
	repo := repository.NewPipelineRepository(rdb, rs)
	debounce := debounceService.NewDebounceEngine(repo)
	ai := aiService.NewAIAdapter(aiSrv.URL, 10)
	dispatch := dispatchService.NewDispatchEngine()
	pipeline := pipelineService.NewPipelineService(repo, debounce, ai, dispatch)
	if err := pipeline.Start(); err != nil {
		t.Fatalf("pipeline.Start: %v", err)
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(gin.Recovery())
	pipelineHandler.NewHandler(repo, pipeline, testSecret).RegisterRoutes(r)
	botSrv := httptest.NewServer(r)

	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		pipeline.Shutdown(shutdownCtx)
		botSrv.Close()
		aiSrv.Server.Close()
		pbSrv.Server.Close()
		rdb.FlushDB(context.Background()) //nolint:errcheck
		rdb.Close()
	})

	return &harness{
		botURL:   botSrv.URL,
		aiServer: aiSrv,
		pbServer: pbSrv,
		rdb:      rdb,
	}
}

// postEvent sends POST /events and returns the response. Fails the test on network error.
func (h *harness) postEvent(t *testing.T, event model.MessageEvent) *http.Response {
	t.Helper()
	body, _ := json.Marshal(event)
	req, _ := http.NewRequest(http.MethodPost, h.botURL+"/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bot-Runtime-Secret", testSecret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /events: %v", err)
	}
	return resp
}

// event builds a minimal valid MessageEvent pointing the postback at h.pbServer.
func (h *harness) event(contactID, convID int64, content string, debounceTime int) model.MessageEvent {
	return model.MessageEvent{
		AgentBotID:     "e2e-bot",
		ContactID:      contactID,
		ConversationID: convID,
		MessageID:      "e2e-msg-1",
		MessageContent: content,
		PostbackURL:    h.pbServer.URL,
		BotConfig:      model.BotConfig{DebounceTime: debounceTime},
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func decodePostbackContent(body []byte) string {
	var pb struct {
		Content string `json:"content"`
	}
	json.Unmarshal(body, &pb) //nolint:errcheck
	return pb.Content
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestE2E_FullHappyPath verifies the complete wiring from HTTP POST through real
// Redis, real AI HTTP adapter, and real dispatch HTTP client to the postback.
// This is the only test that can catch wiring bugs (e.g. PostbackURL not propagated
// to dispatch, BotConfig fields lost between stages) — every unit test mocks at
// least one layer.
func TestE2E_FullHappyPath(t *testing.T) {
	h := newHarness(t)
	contactID, convID := nextPair()

	h.aiServer.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a2aResponse("hello from AI")) //nolint:errcheck
	})

	resp := h.postEvent(t, h.event(contactID, convID, "hello", 0))
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}

	h.pbServer.waitForCall(t, 3*time.Second)

	if n := h.aiServer.calls.Load(); n != 1 {
		t.Errorf("AI called %d times, want 1", n)
	}
	if n := h.pbServer.callCount(); n != 1 {
		t.Errorf("postback called %d times, want 1", n)
	}
	if got := decodePostbackContent(h.pbServer.lastBody()); got != "hello from AI" {
		t.Errorf("postback content = %q, want %q", got, "hello from AI")
	}
}

// TestE2E_DebounceAggregation verifies that two events within the debounce window
// are concatenated with \n\n and reach the AI as a single aggregated message.
// pipeline_service_test.go verifies state transitions but never checks the content
// that reaches the AI — only the full e2e chain can catch buffer formatting bugs.
func TestE2E_DebounceAggregation(t *testing.T) {
	h := newHarness(t)
	contactID, convID := nextPair()

	aiReceived := make(chan string, 1)
	h.aiServer.setHandler(func(w http.ResponseWriter, r *http.Request) {
		var req aiModel.JSONRPCRequest
		json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck
		msg := ""
		if len(req.Params.Message.Parts) > 0 {
			msg = req.Params.Message.Parts[0].Text
		}
		aiReceived <- msg
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a2aResponse("ok")) //nolint:errcheck
	})

	h.postEvent(t, h.event(contactID, convID, "hello", 1)).Body.Close() // starts 1s debounce

	// Wait for the first Process() goroutine to acquire the Redlock and enter
	// StageDebounce before sending the second event. Both goroutines race for the
	// lock (WithTries(1)); the second fails immediately if it tries while the first
	// still holds it. 50ms is well above the Redis round-trip needed to complete
	// startDebounce (acquire → append → setTimer → setState → release).
	time.Sleep(50 * time.Millisecond)

	h.postEvent(t, h.event(contactID, convID, "world", 1)).Body.Close() // resets timer

	select {
	case msg := <-aiReceived:
		if msg != "hello\n\nworld" {
			t.Errorf("AI received message = %q, want %q", msg, "hello\n\nworld")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for AI call after debounce expiry")
	}

	if n := h.aiServer.calls.Load(); n != 1 {
		t.Errorf("AI called %d times, want 1 (two events must produce one aggregated call)", n)
	}
}

// TestE2E_AIInterruption verifies that when a second event arrives while the real
// AI HTTP client is mid-flight, the pipeline context is cancelled (proven by
// "pipeline.ai.cancelled" in production logs) and only the second event's response
// reaches the postback.
// ai_adapter_test.go tests cancellation in isolation; pipeline_service_test.go uses
// a mock adapter. This is the first test to exercise the real HTTP cancellation path
// integrated with the pipeline.
//
// Note: HTTP/1.1 servers do not cancel r.Context() when the client drops the
// connection — the context is only cancelled when the handler returns. We therefore
// verify interruption through observable behavior (postback count + content) rather
// than server-side context propagation. The first call handler is unblocked explicitly
// by the test after assertions complete to prevent deadlock in cleanup.
func TestE2E_AIInterruption(t *testing.T) {
	h := newHarness(t)
	contactID, convID := nextPair()

	firstCallConnected := make(chan struct{}) // closed when mock server has event1's connection
	unblockFirst := make(chan struct{})        // closed by test to release the blocking handler
	firstCallExited := make(chan struct{})     // closed when the blocking handler goroutine exits

	// Always unblock the handler on cleanup to prevent httptest.Server.Close() from
	// waiting indefinitely for the active connection to finish.
	t.Cleanup(func() {
		select {
		case <-unblockFirst:
		default:
			close(unblockFirst)
		}
		select {
		case <-firstCallExited:
		case <-time.After(3 * time.Second):
		}
	})

	var localCalls atomic.Int32
	h.aiServer.setHandler(func(w http.ResponseWriter, r *http.Request) {
		n := localCalls.Add(1)
		if n == 1 {
			// Signal that the server has the connection, then block until the test
			// explicitly releases us. Do not rely on r.Context().Done() — for HTTP/1.1
			// the server context is not cancelled when the client drops the connection.
			close(firstCallConnected)
			<-unblockFirst
			close(firstCallExited)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a2aResponse("event2 response")) //nolint:errcheck
	})

	// event1: debounce=0 → pipeline enters AI stage immediately (AI blocks).
	h.postEvent(t, h.event(contactID, convID, "event1", 0)).Body.Close()

	// Wait until the mock server has actually received event1's connection before
	// sending event2. A fixed sleep is unreliable — goroutine scheduling may delay
	// the AI goroutine's HTTP dial past any fixed sleep window.
	select {
	case <-firstCallConnected:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first AI call to reach mock server")
	}

	// event2: cancels event1's pipeline context and starts a new pipeline.
	h.postEvent(t, h.event(contactID, convID, "event2", 0)).Body.Close()
	h.pbServer.waitForCall(t, 2*time.Second)

	// Unblock the first handler now that we have the postback — prevents deadlock
	// if t.Cleanup fires before we reach this point.
	close(unblockFirst)

	if n := h.pbServer.callCount(); n != 1 {
		t.Errorf("postback called %d times, want 1", n)
	}
	if got := decodePostbackContent(h.pbServer.lastBody()); got != "event2 response" {
		t.Errorf("postback content = %q, want %q", got, "event2 response")
	}
}

// TestE2E_ResponseTime verifies that 202 is returned in under 1s regardless of
// how long the pipeline takes, satisfying NFR-05 / AC#1.
// The structural guarantee (goroutine launch before response) exists in the handler,
// but this test measures the actual wall-clock time under real Redis latency.
func TestE2E_ResponseTime(t *testing.T) {
	h := newHarness(t)
	contactID, convID := nextPair()

	// AI never responds — simulates an arbitrarily slow backend.
	h.aiServer.setHandler(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})

	start := time.Now()
	resp := h.postEvent(t, h.event(contactID, convID, "msg", 0))
	elapsed := time.Since(start)
	resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}
	if elapsed > time.Second {
		t.Errorf("202 took %v, want < 1s (response must not block on pipeline execution)", elapsed)
	}
	if n := h.pbServer.callCount(); n != 0 {
		t.Errorf("postback called %d times, want 0 (AI never responded)", n)
	}
}

// TestE2E_ExactlyOnce_ConcurrentEvents verifies AC#6: when two events for the same
// pair arrive simultaneously, Redlock ensures at most one postback is delivered.
//
// Mechanism: the pipeline uses redsync.WithTries(1) — the second concurrent
// Process() call fails immediately with ErrLockFailed rather than queuing behind
// the first. This is intentional: the second event's handler has already written
// StageIncoming to Redis (durability), so the event is not lost; it is processed
// once the lock is released.
//
// This test exercises the Redlock under real goroutine pressure. Both goroutines
// are released simultaneously via a ready channel. Each fires an HTTP request to
// the live gin server. The fast AI mock returns immediately so at least one
// pipeline completes within the timeout. The invariant is: postback count ≤ 1.
func TestE2E_ExactlyOnce_ConcurrentEvents(t *testing.T) {
	h := newHarness(t)
	contactID, convID := nextPair()

	// Fast AI: always responds immediately so at least one pipeline can complete.
	h.aiServer.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a2aResponse("response")) //nolint:errcheck
	})

	// Release two goroutines simultaneously to maximise the chance of a concurrent
	// lock race inside Process().
	ready := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			<-ready
			h.postEvent(t, h.event(contactID, convID, "msg", 0)).Body.Close()
		}()
	}
	close(ready)
	wg.Wait()

	// The lock winner runs the full pipeline and fires exactly one postback.
	// The lock loser returns immediately with ErrLockFailed and produces no postback.
	// Both handlers write StageIncoming before their goroutines start, so the
	// lock race always resolves before either pipeline can complete — guaranteeing
	// exactly one pipeline runs and exactly one postback is delivered.
	h.pbServer.waitForCall(t, 2*time.Second)

	if n := h.pbServer.callCount(); n != 1 {
		t.Errorf("postback called %d times, want exactly 1 (exactly-once guarantee violated)", n)
	}
}

// TestE2E_PipelineIsolation verifies AC#7: an AI error for one contact+conversation
// pair does not prevent another pair from completing its pipeline.
// The panic recovery path is unit-tested but no test has exercised two concurrent
// live pipelines where one fails and the other must succeed.
func TestE2E_PipelineIsolation(t *testing.T) {
	h := newHarness(t)
	pairAContact, pairAConv := nextPair()
	pairBContact, pairBConv := nextPair()

	// Route by message content — JSON-RPC params.message.parts[0].text carries MessageContent.
	h.aiServer.setHandler(func(w http.ResponseWriter, r *http.Request) {
		var req aiModel.JSONRPCRequest
		json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck
		msg := ""
		if len(req.Params.Message.Parts) > 0 {
			msg = req.Params.Message.Parts[0].Text
		}
		if msg == "pair-a" {
			http.Error(w, "AI error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a2aResponse("pair-b response")) //nolint:errcheck
	})

	h.postEvent(t, h.event(pairAContact, pairAConv, "pair-a", 0)).Body.Close()
	h.postEvent(t, h.event(pairBContact, pairBConv, "pair-b", 0)).Body.Close()

	h.pbServer.waitForCall(t, 3*time.Second)

	if n := h.pbServer.callCount(); n != 1 {
		t.Errorf("postback called %d times, want 1 (only pair B should deliver)", n)
	}
	if got := decodePostbackContent(h.pbServer.lastBody()); got != "pair-b response" {
		t.Errorf("postback content = %q, want %q", got, "pair-b response")
	}

	// Pair A must leave no state in Redis after its AI error.
	stateKey := fmt.Sprintf("bot_runtime:state:%d:%d", pairAContact, pairAConv)
	if n := h.rdb.Exists(context.Background(), stateKey).Val(); n != 0 {
		t.Errorf("pair A state key still exists in Redis after AI error — state leak")
	}
}

// TestE2E_DispatchSegmentation verifies that BotConfig segmentation fields are
// threaded correctly from the event through the pipeline to the dispatch engine,
// and that the AI response is split into the expected parts with the signature
// appended only to the last one.
//
// dispatch_engine_test.go covers segmentation in isolation. This is the first
// test to exercise the full chain: BotConfig in the HTTP event → pipelineEntry →
// runDispatchStage → segmentContent → sequential postback POSTs.
//
// Content: "hello world foo bar", limit=10 chars per segment.
// segmentContent produces ["hello", "world foo", "bar"].
// With signature " [sig]" appended to the last part: "bar [sig]".
func TestE2E_DispatchSegmentation(t *testing.T) {
	h := newHarness(t)
	contactID, convID := nextPair()

	const aiResponse = "hello world foo bar"
	h.aiServer.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a2aResponse(aiResponse)) //nolint:errcheck
	})

	evt := h.event(contactID, convID, "trigger", 0)
	evt.BotConfig.TextSegmentationEnabled = true
	evt.BotConfig.TextSegmentationLimit = 10
	evt.BotConfig.MessageSignature = " [sig]"

	resp := h.postEvent(t, evt)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}

	// Expect 3 sequential postback calls (one per segment).
	h.pbServer.waitForNCalls(t, 3, 4*time.Second)

	if n := h.pbServer.callCount(); n != 3 {
		t.Fatalf("postback called %d times, want 3 (one per segment)", n)
	}

	want := []string{"hello", "world foo", "bar [sig]"}
	for i, body := range h.pbServer.allBodies() {
		got := decodePostbackContent(body)
		if got != want[i] {
			t.Errorf("segment[%d] = %q, want %q", i, got, want[i])
		}
	}
}

// TestE2E_DispatchInterruption verifies that when a new event arrives while
// dispatch is mid-flight, the remaining parts are dropped and only the new
// event's response is eventually delivered.
//
// This tests the ErrDispatchInterrupted path. TestE2E_AIInterruption covers
// cancellation during the AI stage — this is the only test covering the
// dispatch-stage cancel path in an integrated context.
//
// Timing: dispatch sends part 1, then waits DelayPerCharacter*len("hi")=200ms
// before part 2. Event 2 arrives during that window, cancels event 1's context.
// The delay select sees ctx.Done() → ErrDispatchInterrupted → parts 2-3 dropped.
func TestE2E_DispatchInterruption(t *testing.T) {
	h := newHarness(t)
	contactID, convID := nextPair()

	// AI returns a 3-part response (with limit=5): ["hi", "there", "world"].
	// DelayPerCharacter=100ms: "hi" (2 chars) → 200ms between part 1 and part 2.
	h.aiServer.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a2aResponse("hi there world")) //nolint:errcheck
	})

	evt1 := h.event(contactID, convID, "event1", 0)
	evt1.BotConfig.TextSegmentationEnabled = true
	evt1.BotConfig.TextSegmentationLimit = 5
	evt1.BotConfig.DelayPerCharacter = 100.0 // ms per char

	h.postEvent(t, evt1).Body.Close()

	// Wait until part 1 ("hi") has been delivered to the postback server.
	// The 200ms inter-part delay hasn't started yet at this point.
	h.pbServer.waitForCall(t, 3*time.Second)

	// Send event2 while dispatch is in the 200ms inter-part delay.
	// This cancels event1's pipeline context → ErrDispatchInterrupted.
	h.aiServer.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a2aResponse("event2 response")) //nolint:errcheck
	})
	h.postEvent(t, h.event(contactID, convID, "event2", 0)).Body.Close()

	// Wait for event2's postback (the second total call).
	h.pbServer.waitForCall(t, 3*time.Second)

	if n := h.pbServer.callCount(); n != 2 {
		t.Errorf("postback called %d times, want 2 (part 1 from event1 + event2 response)", n)
	}
	bodies := h.pbServer.allBodies()
	if got := decodePostbackContent(bodies[0]); got != "hi" {
		t.Errorf("call[0] = %q, want %q (event1 part 1)", got, "hi")
	}
	if got := decodePostbackContent(bodies[1]); got != "event2 response" {
		t.Errorf("call[1] = %q, want %q (event2 response)", got, "event2 response")
	}
}

// TestE2E_RecoveryAfterRestart verifies NFR-01: a pair in StageDebounce survives
// a service restart and completes its pipeline on the new instance.
//
// This is the critical gap in pipeline_service_test.go: Start() recovery is tested
// with mocks but the real wiring — recovered pipelineEntry having the correct
// BotConfig and PostbackURL — was never tested end-to-end before this.
//
// The test also validates the production bug fix: PipelineState now persists
// BotConfig and PostbackURL at StageDebounce so recovery can reconstruct the
// entry correctly.
func TestE2E_RecoveryAfterRestart(t *testing.T) {
	// Build infrastructure manually (not via newHarness) to control pipeline lifecycle.
	rdb := redis.NewClient(redisOptionsForE2E())
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		rdb.Close()
		t.Skipf("Redis not available: %v", err)
	}
	rdb.FlushDB(context.Background()) //nolint:errcheck

	aiSrv := newMockAIServer()
	pbSrv := newMockPostbackServer()

	t.Cleanup(func() {
		aiSrv.Close()
		pbSrv.Close()
		rdb.FlushDB(context.Background()) //nolint:errcheck
		rdb.Close()
	})

	pool := goredis.NewPool(rdb)
	rs := redsync.New(pool)
	repo := repository.NewPipelineRepository(rdb, rs)
	debounce := debounceService.NewDebounceEngine(repo)
	ai := aiService.NewAIAdapter(aiSrv.URL, 10)
	dispatch := dispatchService.NewDispatchEngine()

	aiSrv.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a2aResponse("recovered response")) //nolint:errcheck
	})

	newPipeline := func() pipelineService.PipelineService {
		p := pipelineService.NewPipelineService(repo, debounce, ai, dispatch)
		if err := p.Start(); err != nil {
			t.Fatalf("pipeline.Start: %v", err)
		}
		return p
	}

	// Spin up pipeline1 + bot server to accept the initial event.
	pipeline1 := newPipeline()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(gin.Recovery())
	pipelineHandler.NewHandler(repo, pipeline1, testSecret).RegisterRoutes(r)
	botSrv := httptest.NewServer(r)
	t.Cleanup(func() { botSrv.Close() })

	h := &harness{botURL: botSrv.URL, aiServer: aiSrv, pbServer: pbSrv, rdb: rdb}
	contactID, convID := nextPair()

	// Send event with 1s debounce. The timer lives in Redis (TTL), not in memory.
	h.postEvent(t, h.event(contactID, convID, "msg", 1)).Body.Close()

	// Wait for startDebounce to write StageDebounce (with BotConfig+PostbackURL) to Redis.
	time.Sleep(100 * time.Millisecond)

	// Shutdown pipeline1 — simulates service restart. The Redis debounce timer
	// continues ticking (~900ms remaining).
	ctx1, cancel1 := context.WithTimeout(context.Background(), 3*time.Second)
	pipeline1.Shutdown(ctx1)
	cancel1()

	// Create pipeline2 with the same dependencies, pointing at the same Redis.
	// Start() scans for StageDebounce pairs and recreates their entries with the
	// BotConfig and PostbackURL stored in PipelineState.
	pipeline2 := newPipeline()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		pipeline2.Shutdown(ctx)
	})

	// Wait for the debounce timer to expire, AI to run, and postback to fire.
	// Budget: ~900ms debounce remaining + AI round-trip + dispatch.
	pbSrv.waitForCall(t, 4*time.Second)

	if got := decodePostbackContent(pbSrv.lastBody()); got != "recovered response" {
		t.Errorf("recovery postback = %q, want %q", got, "recovered response")
	}
}

// TestE2E_PostbackFailure_StateCleared verifies that when the postback endpoint
// returns a non-2xx status, the pipeline clears its Redis state and does not retry.
//
// dispatch_engine_test.go covers the non-2xx error return in isolation.
// This test verifies that runDispatchStage handles the error correctly end-to-end:
// the state key is removed from Redis (no leak), and there is no retry attempt.
func TestE2E_PostbackFailure_StateCleared(t *testing.T) {
	h := newHarness(t)
	contactID, convID := nextPair()

	h.aiServer.setHandler(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a2aResponse("response")) //nolint:errcheck
	})

	// Postback server returns 500 — dispatch should error and clear state.
	h.pbServer.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	})

	resp := h.postEvent(t, h.event(contactID, convID, "msg", 0))
	resp.Body.Close()

	// Wait for dispatch to attempt the postback (records the call, then returns 500).
	h.pbServer.waitForCall(t, 3*time.Second)

	// Allow time for sendPart to read the response, propagate the error through
	// runDispatchStage, and ClearState to complete in Redis.
	time.Sleep(200 * time.Millisecond)

	stateKey := fmt.Sprintf("bot_runtime:state:%d:%d", contactID, convID)
	if n := h.rdb.Exists(context.Background(), stateKey).Val(); n != 0 {
		t.Errorf("state key still exists after postback failure — state leaked in Redis")
	}
	if n := h.pbServer.callCount(); n != 1 {
		t.Errorf("postback called %d times, want 1 (no retry on failure)", n)
	}
}
