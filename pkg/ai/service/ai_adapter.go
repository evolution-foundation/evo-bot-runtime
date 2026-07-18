package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"time"

	brtErrors "github.com/EvolutionAPI/evo-bot-runtime/internal/errors"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
)

// maxResponseBytes caps the AI Processor response body to prevent OOM on oversized payloads.
const maxResponseBytes = 1 << 20 // 1 MiB

// maxBackoff caps the exponential backoff between retries so a large
// AI_CALL_RETRY_BASE_MS or retry count cannot balloon the wait.
const maxBackoff = 5 * time.Second

// AIAdapter calls the AI Processor via A2A protocol (JSON-RPC 2.0).
// Swap the backend by providing a different implementation at main.go wiring.
type AIAdapter interface {
	Call(ctx context.Context, req *model.A2ARequest) (*model.NormalizedResponse, error)
}

type aiAdapter struct {
	timeoutSecs    int
	maxRetries     int
	retryBaseDelay time.Duration
	client         *http.Client
}

// NewAIAdapter constructs the adapter. Returns interface (GEAR R03).
// The AI Processor URL comes from each event's outgoing_url field.
//
// EVO-2167: maxRetries is the number of retries AFTER the first attempt; the call
// is retried on transient failures (5xx/429/network errors) with exponential
// backoff + jitter, so a momentary processor blip does not drop the customer's
// reply. Permanent failures (4xx), timeouts and pipeline cancellation are not
// retried. retryBaseMs is the base backoff in milliseconds.
func NewAIAdapter(timeoutSecs, maxRetries, retryBaseMs int) AIAdapter {
	if maxRetries < 0 {
		maxRetries = 0
	}
	if retryBaseMs <= 0 {
		retryBaseMs = 200
	}
	return &aiAdapter{
		timeoutSecs:    timeoutSecs,
		maxRetries:     maxRetries,
		retryBaseDelay: time.Duration(retryBaseMs) * time.Millisecond,
		client:         &http.Client{},
	}
}

func (a *aiAdapter) Call(ctx context.Context, req *model.A2ARequest) (*model.NormalizedResponse, error) {
	start := time.Now()

	// Use the full outgoing_url provided by the CRM (already contains the agent ID)
	url := req.OutgoingURL

	// Build JSON-RPC 2.0 envelope.
	// contextId MUST be the conversation UUID, not the numeric display_id: the AI
	// Processor builds the ADK session key as "{contextId}_{agentID}". Using the
	// display_id ("3") collides across accounts and never matches the session the
	// Processor persisted (which keys on the UUID), so every turn reads 0 history
	// and the agent loses memory. Prefer the UUID from the CRM metadata; fall back
	// to the numeric ID only when the metadata is absent (e.g. legacy callers).
	contextID := conversationContextID(req.Metadata, req.ConversationID)

	// userId MUST be the contact UUID, not the numeric ContactID. The Processor's
	// ADK session is keyed on (app_name, user_id, session_id). The CRM's SessionSync
	// pre-creates/persists that session with the contact UUID as user_id, so when the
	// a2a run looks it up with the numeric ContactID the keys diverge and the runner
	// raises "Session not found" → 500 → the agent never replies (even though the
	// session and its history exist). Prefer the contact UUID from the CRM metadata;
	// fall back to the numeric ID only when the metadata is absent (legacy callers).
	userID := contactUserID(req.Metadata, req.ContactID)

	rpcReq := model.JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      fmt.Sprintf("%d:%d", req.ContactID, req.ConversationID),
		Method:  "message/send",
		Params: model.JSONRPCParams{
			ContextID: contextID,
			UserID:    userID,
			Message: model.JSONRPCMessage{
				Role: "user",
				Parts: []model.JSONRPCPart{
					{Type: "text", Text: req.Message},
				},
			},
			Metadata: nonNilMetadata(req.Metadata),
		},
	}

	body, err := json.Marshal(rpcReq)
	if err != nil {
		return nil, fmt.Errorf("pipeline.ai.marshal: %w", err)
	}

	// EVO-2167: retry the send on transient failures (5xx/429/network) with
	// exponential backoff + jitter. The body is built once and reused per attempt.
	//
	// Idempotency caveat (the card flagged this to validate): retrying 502/504/network
	// can re-run an attempt the AI Processor already processed but whose response was
	// lost in transit → a duplicate agent turn (ADK history, tool calls, LLM cost).
	// The customer still gets exactly one reply: dispatch runs once, only after a
	// successful attempt. The request body is byte-identical on every retry (built
	// once above), so suppressing the duplicate server-side turn is the AI Processor's
	// responsibility — tracked in EVO-2166. Keep this path free of extra side effects.
	attempts := a.maxRetries + 1
	perAttempt := time.Duration(a.timeoutSecs) * time.Second

	// AC "teto de tempo total": bound the whole sequence with an overall backstop so a
	// growing backoff or attempt count can never run unbounded. The ceiling is the
	// natural worst case (attempts × per-attempt timeout + summed max backoff) plus one
	// per-attempt of slack, so a genuine per-attempt timeout still surfaces as a timeout
	// instead of being swallowed by this backstop. Each attempt keeps its own timeout.
	overallCtx := ctx
	if perAttempt > 0 {
		ceiling := time.Duration(attempts+1)*perAttempt + time.Duration(a.maxRetries)*maxBackoff
		var cancelAll context.CancelFunc
		overallCtx, cancelAll = context.WithTimeout(ctx, ceiling)
		defer cancelAll()
	}

	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			delay := a.backoffDelay(attempt)
			slog.Warn("pipeline.ai.retry",
				"contact_id", req.ContactID,
				"conversation_id", req.ConversationID,
				"attempt", attempt,
				"max_retries", a.maxRetries,
				"delay_ms", delay.Milliseconds(),
			)
			select {
			case <-overallCtx.Done():
				return nil, brtErrors.ErrPipelineCancelled
			case <-time.After(delay):
			}
		}

		resp, retryable, err := a.doOnce(overallCtx, url, body, req, start)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !retryable {
			return nil, err
		}
	}
	return nil, lastErr
}

// doOnce performs a single AI Processor call with a per-attempt timeout. The bool
// return reports whether the error is transient (worth retrying): network errors
// and 5xx/429 statuses are retryable; timeouts, pipeline cancellation, 4xx and
// decode errors are not.
func (a *aiAdapter) doOnce(
	ctx context.Context,
	url string,
	body []byte,
	req *model.A2ARequest,
	start time.Time,
) (*model.NormalizedResponse, bool, error) {
	// Per-attempt timeout — inner timeout, outer ctx for pipeline cancellation.
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(a.timeoutSecs)*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(timeoutCtx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, false, fmt.Errorf("pipeline.ai.new_request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-API-Key", req.ApiKey)

	resp, err := a.client.Do(httpReq)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, brtErrors.ErrPipelineCancelled
		}
		if errors.Is(timeoutCtx.Err(), context.DeadlineExceeded) {
			slog.Warn("pipeline.ai.http.timeout",
				"contact_id", req.ContactID,
				"conversation_id", req.ConversationID,
				"timeout_secs", a.timeoutSecs,
			)
			return nil, false, brtErrors.ErrAITimeout
		}
		// Transient network error (connection refused/reset, e.g. during a deploy).
		return nil, true, fmt.Errorf("pipeline.ai.http: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		retryable := isRetryableStatus(resp.StatusCode)
		// Drain a bounded amount so the connection can be reused.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, retryable, fmt.Errorf("pipeline.ai.status: unexpected %d from AI Processor", resp.StatusCode)
	}

	var a2aResp model.A2AResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&a2aResp); err != nil {
		return nil, false, fmt.Errorf("pipeline.ai.decode: %w", err)
	}

	content := extractResponseText(&a2aResp)

	slog.Info("pipeline.ai.http.completed",
		"contact_id", req.ContactID,
		"conversation_id", req.ConversationID,
		"duration_ms", time.Since(start).Milliseconds(),
	)

	return &model.NormalizedResponse{Content: content}, false, nil
}

// isRetryableStatus reports whether an HTTP status from the AI Processor is a
// transient failure worth retrying. 4xx (bad key/request) are permanent — after
// EVO-2166 an infra error surfaces as 503, not 401, so retrying 5xx covers the
// transient auth/infra case without retrying genuine 401s.
func isRetryableStatus(code int) bool {
	switch code {
	case http.StatusTooManyRequests, // 429
		http.StatusInternalServerError, // 500
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503
		http.StatusGatewayTimeout:      // 504
		return true
	}
	return false
}

// backoffDelay returns the exponential backoff for the given attempt (1-based)
// with up to 50% jitter, capped at maxBackoff.
func (a *aiAdapter) backoffDelay(attempt int) time.Duration {
	d := a.retryBaseDelay << (attempt - 1) // base * 2^(attempt-1)
	if d <= 0 || d > maxBackoff {
		d = maxBackoff
	}
	// Keep at least half the delay, add up to half more as jitter.
	jitter := time.Duration(rand.Int63n(int64(d/2) + 1))
	return d/2 + jitter
}

// extractResponseText extracts the text content from the A2A JSON-RPC response.
// Tries result.artifacts[0].parts[0].text first, then result.message.parts[0].text.
func extractResponseText(resp *model.A2AResponse) string {
	if resp.Result == nil {
		return ""
	}
	// Try artifacts first (primary response format)
	if len(resp.Result.Artifacts) > 0 {
		for _, artifact := range resp.Result.Artifacts {
			for _, part := range artifact.Parts {
				if part.Text != "" {
					return part.Text
				}
			}
		}
	}
	// Fallback to message format
	if resp.Result.Message != nil {
		for _, part := range resp.Result.Message.Parts {
			if part.Text != "" {
				return part.Text
			}
		}
	}
	return ""
}

// nonNilMetadata ensures metadata is never nil (avoids "null" in JSON).
func nonNilMetadata(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

// conversationContextID resolves the contextId for the JSON-RPC call. It reads the
// conversation UUID the CRM nests at metadata.evoai_crm_data.conversation.id and
// returns it; if any hop is missing or empty it falls back to the numeric
// conversation ID (legacy behaviour) so callers without metadata still work.
func conversationContextID(metadata map[string]any, conversationID int64) string {
	fallback := fmt.Sprintf("%d", conversationID)
	if uuid := extractConversationUUID(metadata); uuid != "" {
		return uuid
	}
	return fallback
}

// extractConversationUUID digs metadata.evoai_crm_data.conversation.id out of the
// untyped CRM metadata map. Returns "" when the path is absent or not a string.
func extractConversationUUID(metadata map[string]any) string {
	crmData, ok := metadata["evoai_crm_data"].(map[string]any)
	if !ok {
		return ""
	}
	conversation, ok := crmData["conversation"].(map[string]any)
	if !ok {
		return ""
	}
	id, ok := conversation["id"].(string)
	if !ok {
		return ""
	}
	return id
}

// contactUserID resolves the userId for the JSON-RPC call. It reads the contact
// UUID the CRM nests at metadata.evoai_crm_data.contact.id (the same value the CRM's
// SessionSync uses as the ADK session user_id) and returns it; if any hop is missing
// or empty it falls back to the numeric contact ID (legacy behaviour).
func contactUserID(metadata map[string]any, contactID int64) string {
	if uuid := extractContactUUID(metadata); uuid != "" {
		return uuid
	}
	return fmt.Sprintf("%d", contactID)
}

// extractContactUUID digs metadata.evoai_crm_data.contact.id out of the untyped CRM
// metadata map. Returns "" when the path is absent or not a string.
func extractContactUUID(metadata map[string]any) string {
	crmData, ok := metadata["evoai_crm_data"].(map[string]any)
	if !ok {
		return ""
	}
	contact, ok := crmData["contact"].(map[string]any)
	if !ok {
		return ""
	}
	id, ok := contact["id"].(string)
	if !ok {
		return ""
	}
	return id
}
