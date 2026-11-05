package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	brtErrors "github.com/EvolutionAPI/evo-bot-runtime/internal/errors"
	aiModel "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
	aiService "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/service"
)

// crmMetadata builds the nested CRM metadata map the Rails AgentBotListener sends,
// carrying the conversation UUID at evoai_crm_data.conversation.id and the contact
// UUID at evoai_crm_data.contact.id.
func crmMetadata(conversationUUID, contactUUID string) map[string]any {
	return map[string]any{
		"evoai_crm_data": map[string]any{
			"conversation": map[string]any{
				"id":         conversationUUID,
				"display_id": 7,
			},
			"contact": map[string]any{
				"id":   contactUUID,
				"name": "Davidson Gomes",
			},
		},
	}
}

func TestCall_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify X-API-Key header (per-event auth)
		if got := r.Header.Get("X-API-Key"); got != "test-key" {
			t.Errorf("X-API-Key = %q, want %q", got, "test-key")
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", got)
		}

		// Verify URL path is the OutgoingURL provided by the CRM (already contains the agent ID)
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
		// contextId must be the conversation UUID from metadata, not the numeric ID.
		if req.Params.ContextID != "conv-uuid-abc" {
			t.Errorf("req.Params.ContextID = %q, want %q", req.Params.ContextID, "conv-uuid-abc")
		}
		// userId must be the contact UUID from metadata, not the numeric ContactID.
		if req.Params.UserID != "contact-uuid-xyz" {
			t.Errorf("req.Params.UserID = %q, want %q", req.Params.UserID, "contact-uuid-xyz")
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

	adapter := aiService.NewAIAdapter(30, 0, 0)
	resp, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
		OutgoingURL:    server.URL + "/api/v1/a2a/agent-123",
		Message:        "hello world",
		ContactID:      42,
		ConversationID: 7,
		ApiKey:         "test-key",
		Metadata:       crmMetadata("conv-uuid-abc", "contact-uuid-xyz"),
	})
	if err != nil {
		t.Fatalf("Call returned unexpected error: %v", err)
	}
	if resp.Content != "AI response here" {
		t.Errorf("resp.Content = %q, want %q", resp.Content, "AI response here")
	}
}

// TestCall_ContextID_FallsBackToNumericID asserts that when the CRM metadata is
// absent the adapter falls back to the numeric conversation ID (legacy behaviour),
// so callers that do not send metadata keep working.
func TestCall_ContextID_FallsBackToNumericID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req aiModel.JSONRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		if req.Params.ContextID != "7" {
			t.Errorf("req.Params.ContextID = %q, want fallback %q", req.Params.ContextID, "7")
		}
		// Without metadata, userId falls back to the numeric ContactID.
		if req.Params.UserID != "42" {
			t.Errorf("req.Params.UserID = %q, want fallback %q", req.Params.UserID, "42")
		}
		json.NewEncoder(w).Encode(aiModel.A2AResponse{
			Result: &aiModel.A2AResult{
				Artifacts: []aiModel.A2AArtifact{
					{Parts: []aiModel.A2APart{{Type: "text", Text: "ok"}}},
				},
			},
		})
	}))
	defer server.Close()

	adapter := aiService.NewAIAdapter(30, 0, 0)
	_, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
		OutgoingURL:    server.URL,
		Message:        "test",
		ContactID:      42,
		ConversationID: 7,
		ApiKey:         "key",
		// No Metadata → must fall back to the numeric ConversationID/ContactID.
	})
	if err != nil {
		t.Fatalf("Call returned unexpected error: %v", err)
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

	adapter := aiService.NewAIAdapter(30, 0, 0)
	resp, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
		OutgoingURL: server.URL,
		Message:     "test",
		ApiKey:      "key",
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

	adapter := aiService.NewAIAdapter(30, 0, 0)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := adapter.Call(ctx, &aiModel.A2ARequest{OutgoingURL: server.URL, Message: "test", ApiKey: "key"})
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

	adapter := aiService.NewAIAdapter(1, 0, 0) // 1 s timeout
	_, err := adapter.Call(context.Background(), &aiModel.A2ARequest{OutgoingURL: server.URL, Message: "test", ApiKey: "key"})
	if !errors.Is(err, brtErrors.ErrAITimeout) {
		t.Errorf("expected ErrAITimeout, got %v", err)
	}
}

func TestCall_NonOKStatus_ReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	adapter := aiService.NewAIAdapter(30, 0, 0)
	_, err := adapter.Call(context.Background(), &aiModel.A2ARequest{OutgoingURL: server.URL, Message: "test", ApiKey: "key"})
	if err == nil {
		t.Fatal("expected error for non-200 response, got nil")
	}
}

// headerCapture serves a minimal A2A reply and records the request headers.
func headerCapture(t *testing.T) (*httptest.Server, func() http.Header) {
	t.Helper()
	got := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Clone()
		json.NewEncoder(w).Encode(aiModel.A2AResponse{
			Result: &aiModel.A2AResult{Artifacts: []aiModel.A2AArtifact{{Parts: []aiModel.A2APart{{Type: "text", Text: "ok"}}}}},
		})
	}))
	t.Cleanup(server.Close)
	return server, func() http.Header {
		select {
		case h := <-got:
			return h
		case <-time.After(3 * time.Second):
			t.Fatal("server was never called")
			return nil
		}
	}
}

func callWithHeaders(t *testing.T, url string, headers map[string]string) {
	t.Helper()
	adapter := aiService.NewAIAdapter(30, 0, 0)
	if _, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
		OutgoingURL: url, Message: "test", ApiKey: "key", ProcessorHeaders: headers,
	}); err != nil {
		t.Fatalf("Call returned unexpected error: %v", err)
	}
}

func TestCall_ProcessorHeaders_SentToTheProcessor(t *testing.T) {
	server, received := headerCapture(t)
	t.Setenv("AI_PROCESSOR_URL", server.URL)

	callWithHeaders(t, server.URL+"/api/v1/a2a/agent-1", map[string]string{"X-Request-Context": "ctx-1"})

	if got := received().Get("X-Request-Context"); got != "ctx-1" {
		t.Errorf("X-Request-Context = %q, want %q", got, "ctx-1")
	}
}

func TestCall_ProcessorHeaders_NotSentElsewhere(t *testing.T) {
	server, received := headerCapture(t)
	u, _ := url.Parse(server.URL)
	cases := map[string]string{
		"other host":   "http://processor.invalid:" + u.Port(),
		"other port":   "http://" + u.Hostname() + ":1",
		"other scheme": "https://" + u.Host,
		"unset":        "",
		"no host":      "processor:8000",
	}
	for name, processor := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("AI_PROCESSOR_URL", processor)
			callWithHeaders(t, server.URL+"/api/v1/a2a/agent-1", map[string]string{"X-Request-Context": "ctx-1"})
			if got := received().Values("X-Request-Context"); len(got) != 0 {
				t.Errorf("X-Request-Context = %v, want no header", got)
			}
		})
	}
}

func TestCall_ProcessorHeaders_CannotOverrideTheAdaptersOwn(t *testing.T) {
	server, received := headerCapture(t)
	t.Setenv("AI_PROCESSOR_URL", server.URL)

	callWithHeaders(t, server.URL, map[string]string{
		"X-API-Key":    "from-event",
		"Content-Type": "text/plain",
		"Bad Name":     "x",
		"X-Bad-Value":  "a\r\nX-Injected: 1",
	})

	h := received()
	if got := h.Get("X-API-Key"); got != "key" {
		t.Errorf("X-API-Key = %q, want the adapter's %q", got, "key")
	}
	if got := h.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	if h.Get("X-Bad-Value") != "" || h.Get("X-Injected") != "" {
		t.Errorf("invalid header value was sent: %v", h)
	}
}
