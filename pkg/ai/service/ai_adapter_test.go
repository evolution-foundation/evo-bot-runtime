package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	brtErrors "github.com/EvolutionAPI/evo-bot-runtime/internal/errors"
	aiModel "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
	aiService "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/service"
)

func TestCall_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify X-API-Key header (per-event auth)
		if got := r.Header.Get("X-API-Key"); got != "test-key" {
			t.Errorf("X-API-Key = %q, want %q", got, "test-key")
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", got)
		}

		// Verify URL path contains agent_bot_id
		if !strings.HasSuffix(r.URL.Path, "/api/v1/a2a/agent-123") {
			t.Errorf("URL path = %q, want suffix /api/v1/a2a/agent-123", r.URL.Path)
		}

		// Verify JSON-RPC 2.0 envelope
		var req aiModel.JSONRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		if req.JSONRPC != "2.0" {
			t.Errorf("req.JSONRPC = %q, want %q", req.JSONRPC, "2.0")
		}
		if req.Method != "message/send" {
			t.Errorf("req.Method = %q, want %q", req.Method, "message/send")
		}
		if req.Params.ContextID != "7" {
			t.Errorf("req.Params.ContextID = %q, want %q", req.Params.ContextID, "7")
		}
		if req.Params.UserID != "42" {
			t.Errorf("req.Params.UserID = %q, want %q", req.Params.UserID, "42")
		}
		if len(req.Params.Message.Parts) != 1 || req.Params.Message.Parts[0].Text != "hello world" {
			t.Errorf("message parts = %+v, want single part with text 'hello world'", req.Params.Message.Parts)
		}

		// Return JSON-RPC 2.0 response with artifacts format
		resp := aiModel.A2AResponse{
			Result: &aiModel.A2AResult{
				Artifacts: []aiModel.A2AArtifact{
					{Parts: []aiModel.A2APart{{Type: "text", Text: "AI response here"}}},
				},
			},
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	defer server.Close()

	adapter := aiService.NewAIAdapter(server.URL, 30)
	resp, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
		AgentBotID:     "agent-123",
		Message:        "hello world",
		ContactID:      42,
		ConversationID: 7,
		ApiKey:         "test-key",
	})
	if err != nil {
		t.Fatalf("Call returned unexpected error: %v", err)
	}
	if resp.Content != "AI response here" {
		t.Errorf("resp.Content = %q, want %q", resp.Content, "AI response here")
	}
}

func TestCall_Success_MessageFormat(t *testing.T) {
	// Test the fallback response format (result.message instead of result.artifacts)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := aiModel.A2AResponse{
			Result: &aiModel.A2AResult{
				Message: &aiModel.A2AMessage{
					Parts: []aiModel.A2APart{{Type: "text", Text: "message format response"}},
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	adapter := aiService.NewAIAdapter(server.URL, 30)
	resp, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
		AgentBotID: "bot-1",
		Message:    "test",
		ApiKey:     "key",
	})
	if err != nil {
		t.Fatalf("Call returned unexpected error: %v", err)
	}
	if resp.Content != "message format response" {
		t.Errorf("resp.Content = %q, want %q", resp.Content, "message format response")
	}
}

func TestCall_ContextCancellation_ReturnsPipelineCancelled(t *testing.T) {
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

	adapter := aiService.NewAIAdapter(server.URL, 30)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := adapter.Call(ctx, &aiModel.A2ARequest{AgentBotID: "bot-1", Message: "test", ApiKey: "key"})
	if !errors.Is(err, brtErrors.ErrPipelineCancelled) {
		t.Errorf("expected ErrPipelineCancelled, got %v", err)
	}
}

func TestCall_Timeout_ReturnsAITimeout(t *testing.T) {
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

	adapter := aiService.NewAIAdapter(server.URL, 1) // 1 s timeout
	_, err := adapter.Call(context.Background(), &aiModel.A2ARequest{AgentBotID: "bot-1", Message: "test", ApiKey: "key"})
	if !errors.Is(err, brtErrors.ErrAITimeout) {
		t.Errorf("expected ErrAITimeout, got %v", err)
	}
}

func TestCall_NonOKStatus_ReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	adapter := aiService.NewAIAdapter(server.URL, 30)
	_, err := adapter.Call(context.Background(), &aiModel.A2ARequest{AgentBotID: "bot-1", Message: "test", ApiKey: "key"})
	if err == nil {
		t.Fatal("expected error for non-200 response, got nil")
	}
}
