package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"mime"
	"net/http"
	neturl "net/url"
	"os"
	"path"
	"strings"
	"time"

	brtErrors "github.com/EvolutionAPI/evo-bot-runtime/internal/errors"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
)

// maxResponseBytes caps the AI Processor response body to prevent OOM on oversized payloads.
const maxResponseBytes = 1 << 20 // 1 MiB

// maxAttachmentBytes caps a single downloaded incoming attachment (EVO-2180).
// Images routinely exceed maxResponseBytes (1 MiB); base64 inflates ~33%, so keep
// this conservative relative to the processor's request-body limit.
const maxAttachmentBytes = 15 << 20 // 15 MiB

// maxAttachmentsTotalBytes caps the SUM of every attachment forwarded in one call.
// The debounce window aggregates the media of all messages in it, so a per-file cap
// alone lets a photo burst build a body of len(attachments) x maxAttachmentBytes.
// Base64 inflates that ~33% and the gateway rejects the POST (nginx
// client_max_body_size) — and a 413 is not retryable, so the customer would lose the
// text reply as well. Over budget, the remaining attachments are dropped and the
// call proceeds with what fits.
const maxAttachmentsTotalBytes = 20 << 20 // 20 MiB (~27 MiB once base64-encoded)

// Attachment downloads run before the AI call and outside its retry ceiling, so they
// need a bound of their own: reusing the AI timeout (AI_CALL_TIMEOUT_SECONDS,
// default 30s) meant an unreachable media host stalled every turn for
// timeout x len(attachments) before the processor was even called.
// maxAttachmentDownload bounds one download; the whole set shares
// attachmentsTotalTimeFactor times that.
const (
	maxAttachmentDownload      = 10 * time.Second
	attachmentsTotalTimeFactor = 3
)

// mediaHostAllowlistEnv names the hosts authorized to serve incoming media,
// comma-separated. It is the *only* source: an allowlist read out of the event
// would be chosen by whoever sent the event, which is exactly who it must
// constrain. Unset means no media is fetched.
const mediaHostAllowlistEnv = "MEDIA_HOST_ALLOWLIST"

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

	// Message parts: text first, then one file part per downloaded attachment.
	// EVO-2180: downloads happen ONCE here (before Marshal), so the byte-identical
	// body is reused across retries.
	parts := []model.JSONRPCPart{{Type: "text", Text: req.Message}}
	parts = append(parts, a.buildFileParts(ctx, req)...)

	rpcReq := model.JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      fmt.Sprintf("%d:%d", req.ContactID, req.ConversationID),
		Method:  "message/send",
		Params: model.JSONRPCParams{
			ContextID: contextID,
			UserID:    userID,
			Message: model.JSONRPCMessage{
				Role:  "user",
				Parts: parts,
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

// buildFileParts downloads each incoming attachment and returns it as a base64 A2A
// file part. A failure (unreachable/private URL, non-200, oversize, timeout) is
// logged and skipped — the text-only message must always survive a media failure.
// EVO-2180.
func (a *aiAdapter) buildFileParts(ctx context.Context, req *model.A2ARequest) []model.JSONRPCPart {
	if len(req.Attachments) == 0 {
		return nil
	}
	perDownload := a.attachmentTimeout()
	budgetCtx, cancelBudget := context.WithTimeout(ctx, attachmentsTotalTimeFactor*perDownload)
	defer cancelBudget()

	// Built once per turn: the client closes over the authorized hosts.
	hosts := allowedMediaHosts()
	client := a.mediaClient(hosts)

	parts := make([]model.JSONRPCPart, 0, len(req.Attachments))
	remaining := maxAttachmentsTotalBytes
	for i, att := range req.Attachments {
		if att.URL == "" {
			continue
		}
		if err := checkMediaURL(att.URL, hosts); err != nil {
			slog.Warn("pipeline.ai.attachment.blocked_url",
				"contact_id", req.ContactID,
				"conversation_id", req.ConversationID,
				"file_type", att.FileType,
				"error", err,
				"hint", "add the host to "+mediaHostAllowlistEnv,
			)
			continue
		}
		if budgetCtx.Err() != nil {
			slog.Warn("pipeline.ai.attachment.budget_exhausted",
				"contact_id", req.ContactID,
				"conversation_id", req.ConversationID,
				"limit", "time",
				"forwarded", len(parts),
				"dropped", len(req.Attachments)-i,
			)
			break
		}
		limit := min(maxAttachmentBytes, remaining)
		if limit <= 0 {
			slog.Warn("pipeline.ai.attachment.budget_exhausted",
				"contact_id", req.ContactID,
				"conversation_id", req.ConversationID,
				"limit", "bytes",
				"forwarded", len(parts),
				"dropped", len(req.Attachments)-i,
			)
			break
		}
		data, respContentType, err := downloadAttachment(budgetCtx, client, att.URL, perDownload, limit)
		if err != nil {
			// Status separates the common 404-from-an-expired-signed-link case from
			// an unreachable host; they logged identically before.
			slog.Warn("pipeline.ai.attachment.download_failed",
				"contact_id", req.ContactID,
				"conversation_id", req.ConversationID,
				"file_type", att.FileType,
				"status", statusOf(err),
				"error", err,
			)
			continue
		}
		mimeType, ok := resolveMimeType(att, respContentType)
		if !ok {
			slog.Warn("pipeline.ai.attachment.skipped_not_media",
				"contact_id", req.ContactID,
				"conversation_id", req.ConversationID,
				"file_type", att.FileType,
				"declared_content_type", att.ContentType,
				"response_content_type", respContentType,
			)
			continue
		}
		remaining -= len(data)
		parts = append(parts, model.JSONRPCPart{
			Type: "file",
			File: &model.JSONRPCFile{
				Name:     attachmentName(att.URL),
				MimeType: mimeType,
				Bytes:    base64.StdEncoding.EncodeToString(data),
			},
		})
		slog.Info("pipeline.ai.attachment.forwarded",
			"contact_id", req.ContactID,
			"conversation_id", req.ConversationID,
			"file_type", att.FileType,
			"mime_type", mimeType,
			"bytes", len(data),
		)
	}
	return parts
}

// attachmentTimeout is the per-download timeout: maxAttachmentDownload, shrunk to
// the configured AI timeout when that is smaller (so a deployment tuned for fast
// failure does not wait longer on media than on the AI call itself).
func (a *aiAdapter) attachmentTimeout() time.Duration {
	d := maxAttachmentDownload
	if t := time.Duration(a.timeoutSecs) * time.Second; t > 0 && t < d {
		d = t
	}
	return d
}

// allowedMediaHosts returns the hostnames authorized to serve incoming media.
// An empty set authorizes nothing: failing closed beats fetching a URL the caller
// chose.
func allowedMediaHosts() map[string]struct{} {
	hosts := make(map[string]struct{}, 2)
	for _, h := range strings.Split(os.Getenv(mediaHostAllowlistEnv), ",") {
		if h = strings.ToLower(strings.TrimSpace(h)); h != "" {
			hosts[h] = struct{}{}
		}
	}
	return hosts
}

// checkMediaURL reports why a media URL must not be fetched, or nil when it may be.
func checkMediaURL(rawURL string, hosts map[string]struct{}) error {
	u, err := neturl.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("unparseable media url: %w", err)
	}
	if scheme := strings.ToLower(u.Scheme); scheme != "http" && scheme != "https" {
		return fmt.Errorf("scheme %q is not allowed for media", u.Scheme)
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return errors.New("media url has no host")
	}
	if _, ok := hosts[host]; !ok {
		return fmt.Errorf("host %q is not authorized to serve media for this event", host)
	}
	return nil
}

// mediaClient shares the adapter's transport but re-runs checkMediaURL on every
// redirect hop, so an authorized host cannot 302 the download onto an internal one.
func (a *aiAdapter) mediaClient(hosts map[string]struct{}) *http.Client {
	return &http.Client{
		Transport: a.client.Transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			return checkMediaURL(req.URL.String(), hosts)
		},
	}
}

// httpStatusError carries the status of a non-200 media response.
type httpStatusError struct{ status int }

func (e *httpStatusError) Error() string { return fmt.Sprintf("unexpected status %d", e.status) }

// statusOf returns the HTTP status of a download error, or 0 if there was no response.
func statusOf(err error) int {
	var se *httpStatusError
	if errors.As(err, &se) {
		return se.status
	}
	return 0
}

// downloadAttachment GETs the URL with the given client and timeout, reading at most
// limit bytes. It returns the body and the response Content-Type.
func downloadAttachment(ctx context.Context, client *http.Client, url string, timeout time.Duration, limit int) ([]byte, string, error) {
	dlCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(dlCtx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", fmt.Errorf("new_request: %w", err)
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, "", fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, "", &httpStatusError{status: resp.StatusCode}
	}
	// +1 so an exactly-at-cap read is distinguishable from an oversize one.
	data, err := io.ReadAll(io.LimitReader(resp.Body, int64(limit)+1))
	if err != nil {
		return nil, "", fmt.Errorf("read: %w", err)
	}
	if len(data) > limit {
		return nil, "", fmt.Errorf("attachment exceeds the %d bytes still available in this call", limit)
	}
	return data, resp.Header.Get("Content-Type"), nil
}

// resolveMimeType picks the mime type sent to the AI Processor, which forwards it
// verbatim into the model call (runner_utils: Blob(mime_type=content_type)) — so a
// wrong value here surfaces as a processor-side failure, not a graceful skip.
//
// It prefers the Content-Type of the response actually downloaded (that describes
// the bytes in hand), falls back to what the CRM declared, then to the URL
// extension. It returns false when the payload is a web page: a Rails proxy URL
// answering 200 with an error/login page would otherwise be forwarded as a valid
// image. It also returns false when nothing better than application/octet-stream
// can be determined — an opaque blob is rejected by the model APIs, so dropping it
// keeps the text reply alive instead of failing the whole turn.
func resolveMimeType(att model.Attachment, respContentType string) (string, bool) {
	respType := mediaTypeOf(respContentType)
	declared := mediaTypeOf(att.ContentType)
	if isWebPage(respType) || isWebPage(declared) {
		return "", false
	}
	for _, candidate := range []string{respType, declared, mediaTypeOf(mimeFromURL(att.URL))} {
		if candidate != "" && candidate != octetStream {
			return candidate, true
		}
	}
	return "", false
}

const octetStream = "application/octet-stream"

// mediaTypeOf normalises a Content-Type header to its bare media type
// ("image/jpeg; charset=binary" → "image/jpeg").
func mediaTypeOf(contentType string) string {
	if contentType == "" {
		return ""
	}
	if mt, _, err := mime.ParseMediaType(contentType); err == nil {
		return mt
	}
	return strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
}

// isWebPage reports whether a media type is an HTML document rather than media.
func isWebPage(mediaType string) bool {
	return mediaType == "text/html" || mediaType == "application/xhtml+xml"
}

// mimeFromURL guesses a mime type from the URL's file extension. ActiveStorage
// proxy URLs keep the original filename, so this recovers the type when neither the
// CRM nor the storage backend declares a useful one.
func mimeFromURL(rawURL string) string {
	u, err := neturl.Parse(rawURL)
	if err != nil {
		return ""
	}
	return mime.TypeByExtension(path.Ext(u.Path))
}

// attachmentName derives a filename from the URL path (fallback "file").
func attachmentName(rawURL string) string {
	if u, err := neturl.Parse(rawURL); err == nil {
		if base := path.Base(u.Path); base != "" && base != "." && base != "/" {
			return base
		}
	}
	return "file"
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
