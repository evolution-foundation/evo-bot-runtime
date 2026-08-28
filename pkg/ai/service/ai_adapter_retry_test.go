package service_test

// EVO-2167: the AI Processor call must retry transient failures (5xx/429/network)
// so a momentary blip does not leave the customer without a reply, while NOT
// retrying permanent failures (4xx).

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	brtErrors "github.com/EvolutionAPI/evo-bot-runtime/internal/errors"
	aiModel "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
	aiService "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/service"
)

func writeOK(w http.ResponseWriter) {
	_ = json.NewEncoder(w).Encode(aiModel.A2AResponse{
		Result: &aiModel.A2AResult{
			Artifacts: []aiModel.A2AArtifact{
				{Parts: []aiModel.A2APart{{Type: "text", Text: "ok"}}},
			},
		},
	})
}

func retryReq(url string) *aiModel.A2ARequest {
	return &aiModel.A2ARequest{
		OutgoingURL:    url + "/api/v1/a2a/agent-123",
		Message:        "hi",
		ContactID:      42,
		ConversationID: 7,
		ApiKey:         "k",
	}
}

func TestCall_RetriesOn503ThenSucceeds(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable) // 503 -> transient
			return
		}
		writeOK(w)
	}))
	defer server.Close()

	adapter := aiService.NewAIAdapter(30, 2, 1) // 2 retries, 1ms base
	resp, err := adapter.Call(context.Background(), retryReq(server.URL))
	if err != nil {
		t.Fatalf("expected success after retry, got error: %v", err)
	}
	if resp.Content != "ok" {
		t.Errorf("content = %q, want ok", resp.Content)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("server calls = %d, want 2 (503 then 200)", got)
	}
}

func TestCall_ExhaustsRetriesOnPersistent500(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	adapter := aiService.NewAIAdapter(30, 2, 1)
	if _, err := adapter.Call(context.Background(), retryReq(server.URL)); err == nil {
		t.Fatal("expected error after exhausting retries, got nil")
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Errorf("server calls = %d, want 3 (1 + 2 retries)", got)
	}
}

func TestCall_DoesNotRetryOn400(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadRequest) // 400 -> permanent
	}))
	defer server.Close()

	adapter := aiService.NewAIAdapter(30, 3, 1)
	if _, err := adapter.Call(context.Background(), retryReq(server.URL)); err == nil {
		t.Fatal("expected error for 400, got nil")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("server calls = %d, want 1 (400 is permanent, no retry)", got)
	}
}

func TestCall_RetriesOnNetworkErrorThenSucceeds(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			panic(http.ErrAbortHandler) // abruptly close the connection -> client network error
		}
		writeOK(w)
	}))
	defer server.Close()

	adapter := aiService.NewAIAdapter(30, 2, 1)
	resp, err := adapter.Call(context.Background(), retryReq(server.URL))
	if err != nil {
		t.Fatalf("expected success after network retry, got error: %v", err)
	}
	if resp.Content != "ok" {
		t.Errorf("content = %q, want ok", resp.Content)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("server calls = %d, want 2", got)
	}
}

func TestCall_NoRetryWhenMaxRetriesZero(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	adapter := aiService.NewAIAdapter(30, 0, 1) // retries disabled -> single attempt
	if _, err := adapter.Call(context.Background(), retryReq(server.URL)); err == nil {
		t.Fatal("expected error, got nil")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("server calls = %d, want 1 (no retry)", got)
	}
}

// AC #5 ("timeout por tentativa"): a per-attempt timeout must surface as ErrAITimeout
// and must NOT be retried, even with retries enabled. Regression guard for the
// per-attempt timeout scoping in doOnce.
func TestCall_TimeoutIsNotRetried(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(1200 * time.Millisecond) // exceed the 1s per-attempt timeout below
		writeOK(w)
	}))
	defer server.Close()

	adapter := aiService.NewAIAdapter(1, 3, 1) // 1s per-attempt timeout, retries enabled
	_, err := adapter.Call(context.Background(), retryReq(server.URL))
	if !errors.Is(err, brtErrors.ErrAITimeout) {
		t.Fatalf("expected ErrAITimeout, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("server calls = %d, want 1 (per-attempt timeout must NOT be retried)", got)
	}
}
