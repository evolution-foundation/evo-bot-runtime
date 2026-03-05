package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	brtErrors "github.com/EvolutionAPI/evo-bot-runtime/internal/errors"
	aiModel "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
	aiService "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/service"
)

func TestCall_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("Authorization = %q, want %q", got, "Bearer test-key")
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", got)
		}

		var req aiModel.A2ARequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		if req.Message != "hello world" {
			t.Errorf("req.Message = %q, want %q", req.Message, "hello world")
		}
		// ContactID/ConversationID must NOT appear in the serialised JSON payload.
		if req.ContactID != 0 || req.ConversationID != 0 {
			t.Errorf("json:- fields leaked into payload: contact_id=%d conversation_id=%d",
				req.ContactID, req.ConversationID)
		}

		if err := json.NewEncoder(w).Encode(aiModel.A2AResponse{Content: "AI response here"}); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	defer server.Close()

	adapter := aiService.NewAIAdapter(server.URL, "test-key", 30)
	resp, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
		Message:        "hello world",
		ContactID:      42,
		ConversationID: 7,
	})
	if err != nil {
		t.Fatalf("Call returned unexpected error: %v", err)
	}
	if resp.Content != "AI response here" {
		t.Errorf("resp.Content = %q, want %q", resp.Content, "AI response here")
	}
}

func TestCall_ContextCancellation_ReturnsPipelineCancelled(t *testing.T) {
	// unblock is closed by t.Cleanup to guarantee the handler exits even if
	// r.Context().Done() is not immediately triggered by the client disconnect.
	unblock := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-unblock:
		}
	}))
	t.Cleanup(func() {
		close(unblock)
		server.Close()
	})

	adapter := aiService.NewAIAdapter(server.URL, "key", 30)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := adapter.Call(ctx, &aiModel.A2ARequest{Message: "test"})
	if !errors.Is(err, brtErrors.ErrPipelineCancelled) {
		t.Errorf("expected ErrPipelineCancelled, got %v", err)
	}
}

func TestCall_Timeout_ReturnsAITimeout(t *testing.T) {
	// unblock is closed by t.Cleanup so server.Close() does not block after
	// the timeout assertion passes (avoids ~4s delay in test teardown).
	unblock := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-unblock:
		}
	}))
	t.Cleanup(func() {
		close(unblock)
		server.Close()
	})

	adapter := aiService.NewAIAdapter(server.URL, "key", 1) // 1 s timeout
	_, err := adapter.Call(context.Background(), &aiModel.A2ARequest{Message: "test"})
	if !errors.Is(err, brtErrors.ErrAITimeout) {
		t.Errorf("expected ErrAITimeout, got %v", err)
	}
}

func TestCall_NonOKStatus_ReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	adapter := aiService.NewAIAdapter(server.URL, "key", 30)
	_, err := adapter.Call(context.Background(), &aiModel.A2ARequest{Message: "test"})
	if err == nil {
		t.Fatal("expected error for non-200 response, got nil")
	}
}
