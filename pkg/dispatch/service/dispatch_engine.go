package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	brtErrors "github.com/EvolutionAPI/evo-bot-runtime/internal/errors"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
)

// DispatchEngine segments the AI response, appends the message signature,
// and sends each part sequentially via HTTP postback.
// Swap the dispatch backend by providing a different implementation at main.go wiring.
type DispatchEngine interface {
	Dispatch(
		ctx            context.Context,
		contactID      int64,
		conversationID int64,
		content        string,
		cfg            model.BotConfig,
		postbackURL    string,
	) error
}

// postbackRequest is the JSON body for each HTTP POST to the postback endpoint.
type postbackRequest struct {
	Content     string               `json:"content"`
	MessageType string               `json:"message_type"`
	ContentType string               `json:"content_type"`
	Attachments []postbackAttachment `json:"attachments,omitempty"`
}

// postbackAttachment is a media URL detected in the AI response, sent to the CRM
// so it can download and render it as real media instead of a plain text link.
// FileType matches the CRM Attachment enum: image / audio / video / file.
type postbackAttachment struct {
	URL      string `json:"url"`
	FileType string `json:"file_type"`
}

type dispatchEngineImpl struct {
	client *http.Client
	secret string
}

// postbackClientTimeout is the maximum time allowed for a single HTTP postback call.
// The per-call context (propagated via Dispatch) can still cancel earlier.
const postbackClientTimeout = 30 * time.Second

// NewDispatchEngine constructs the engine. Returns interface (GEAR R03).
// secret is the BOT_RUNTIME_SECRET sent as X-Bot-Runtime-Secret header on postback.
func NewDispatchEngine(secret string) DispatchEngine {
	return &dispatchEngineImpl{
		client: &http.Client{Timeout: postbackClientTimeout},
		secret: secret,
	}
}

func (d *dispatchEngineImpl) Dispatch(
	ctx            context.Context,
	contactID      int64,
	conversationID int64,
	content        string,
	cfg            model.BotConfig,
	postbackURL    string,
) error {
	// Pull media URLs out of the full response BEFORE segmenting, so media is
	// not split across text parts. Media is delivered in a single dedicated
	// postback after the text parts (see below). The CRM also re-detects media
	// from the text as a fallback, so this is an optimization, not the only path.
	residual, atts := extractMediaURLs(content)

	parts := segmentContent(residual, cfg)

	// Prepend signature to the first part (FR-21)
	if cfg.MessageSignature != "" && len(parts) > 0 {
		parts[0] = cfg.MessageSignature + parts[0]
	}

	start := time.Now()

	for i, part := range parts {
		// Skip empty residual (e.g. response was only a media URL): the media
		// postback below still runs.
		if part == "" {
			continue
		}

		// Check cancellation BEFORE sending this part
		select {
		case <-ctx.Done():
			slog.Info("pipeline.dispatch.interrupted",
				"contact_id",      contactID,
				"conversation_id", conversationID,
				"parts_sent",      i,
			)
			return brtErrors.ErrDispatchInterrupted
		default:
		}

		if err := d.sendPart(ctx, postbackURL, part, nil); err != nil {
			return fmt.Errorf("pipeline.dispatch.send[%d]: %w", i, err)
		}

		// Apply inter-part delay — skip for the last part (FR-22)
		if i < len(parts)-1 && cfg.DelayPerCharacter > 0 {
			delayMs := time.Duration(cfg.DelayPerCharacter*float64(utf8.RuneCountInString(part))) * time.Millisecond
			select {
			case <-ctx.Done():
				slog.Info("pipeline.dispatch.interrupted",
					"contact_id",      contactID,
					"conversation_id", conversationID,
					"parts_sent",      i+1,
				)
				return brtErrors.ErrDispatchInterrupted
			case <-time.After(delayMs):
			}
		}
	}

	// Deliver media (if any) in a single dedicated postback after the text.
	if len(atts) > 0 {
		select {
		case <-ctx.Done():
			return brtErrors.ErrDispatchInterrupted
		default:
		}
		if err := d.sendPart(ctx, postbackURL, "", atts); err != nil {
			return fmt.Errorf("pipeline.dispatch.send[media]: %w", err)
		}
	}

	slog.Info("pipeline.dispatch.completed",
		"contact_id",      contactID,
		"conversation_id", conversationID,
		"duration_ms",     time.Since(start).Milliseconds(),
		"parts_total",     len(parts),
		"attachments",     len(atts),
	)
	return nil
}

// sendPart sends a single content part (and optional media attachments) to the
// postback URL.
func (d *dispatchEngineImpl) sendPart(ctx context.Context, postbackURL, content string, atts []postbackAttachment) error {
	body, err := json.Marshal(postbackRequest{
		Content:     content,
		MessageType: "outgoing",
		ContentType: "text",
		Attachments: atts,
	})
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, postbackURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("new_request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if d.secret != "" {
		req.Header.Set("X-Bot-Runtime-Secret", d.secret)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body) // drain to allow connection reuse

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("status: %d", resp.StatusCode)
	}
	return nil
}

// segmentContent splits content into parts of at most TextSegmentationLimit characters,
// respecting word boundaries, then merges any part shorter than TextSegmentationMinSize
// with its predecessor.
func segmentContent(content string, cfg model.BotConfig) []string {
	if !cfg.TextSegmentationEnabled || cfg.TextSegmentationLimit <= 0 {
		return []string{content}
	}

	limit := cfg.TextSegmentationLimit
	words := strings.Fields(content)
	if len(words) == 0 {
		return []string{content} // preserve empty or whitespace-only content
	}

	// Build raw parts greedily by word (rune-aware — fixes non-ASCII content)
	var rawParts []string
	var current strings.Builder
	currentRunes := 0
	for _, word := range words {
		wordRunes := utf8.RuneCountInString(word)
		if currentRunes == 0 {
			current.WriteString(word)
			currentRunes = wordRunes
		} else if currentRunes+1+wordRunes <= limit {
			current.WriteByte(' ')
			current.WriteString(word)
			currentRunes += 1 + wordRunes
		} else {
			rawParts = append(rawParts, current.String())
			current.Reset()
			current.WriteString(word)
			currentRunes = wordRunes
		}
	}
	if currentRunes > 0 {
		rawParts = append(rawParts, current.String())
	}

	// Merge parts shorter than TextSegmentationMinSize into previous part,
	// only when the merged result stays within limit (prevents overflow).
	if cfg.TextSegmentationMinSize <= 0 || len(rawParts) <= 1 {
		return rawParts
	}

	merged := make([]string, 0, len(rawParts))
	lastPartRunes := 0
	for _, p := range rawParts {
		pRunes := utf8.RuneCountInString(p)
		if len(merged) > 0 && pRunes < cfg.TextSegmentationMinSize && lastPartRunes+1+pRunes <= limit {
			merged[len(merged)-1] += " " + p
			lastPartRunes += 1 + pRunes
		} else {
			merged = append(merged, p)
			lastPartRunes = pRunes
		}
	}
	return merged
}

// --- Media URL detection ---------------------------------------------------
//
// extractMediaURLs pulls media URLs (by file extension) out of the response
// text and returns the residual text plus the detected attachments.
//
// DRIFT WARNING: this MUST stay in sync with the Ruby
// AgentBots::MediaTypeDetector (app/services/agent_bots/media_type_detector.rb)
// in evo-ai-crm-community. The CRM re-detects media from text as a fallback, so
// a divergence degrades gracefully rather than losing media.

var urlRegex = regexp.MustCompile(`https?://[^\s<>"']+`)

// extension -> file_type (matches Attachment enum; document maps to "file").
var mediaExtToFileType = func() map[string]string {
	m := map[string]string{}
	for _, e := range []string{"jpg", "jpeg", "png", "gif", "bmp", "webp", "svg", "tiff"} {
		m[e] = "image"
	}
	for _, e := range []string{"mp3", "wav", "ogg", "m4a", "aac", "flac"} {
		m[e] = "audio"
	}
	for _, e := range []string{"mp4", "avi", "mov", "wmv", "flv", "mkv", "webm"} {
		m[e] = "video"
	}
	for _, e := range []string{"pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "rtf", "odt"} {
		m[e] = "file"
	}
	return m
}()

// trailing punctuation often captured when a URL ends a sentence.
const trailingPunct = ")].,!?;:"

func extractMediaURLs(content string) (residual string, atts []postbackAttachment) {
	residual = content
	for _, rawURL := range urlRegex.FindAllString(content, -1) {
		url := strings.TrimRight(rawURL, trailingPunct)
		fileType := mediaFileType(url)
		if fileType == "" {
			continue // non-media URL stays in the text
		}
		atts = append(atts, postbackAttachment{URL: url, FileType: fileType})
		residual = strings.Replace(residual, rawURL, "", 1)
	}
	return strings.TrimSpace(residual), atts
}

// mediaFileType returns the Attachment file_type for a URL, or "" if not media.
// Matches the extension in the PATH, ignoring query string / fragment.
func mediaFileType(url string) string {
	path := url
	if i := strings.IndexAny(path, "?#"); i >= 0 {
		path = path[:i]
	}
	seg := path
	if i := strings.LastIndex(seg, "/"); i >= 0 {
		seg = seg[i+1:]
	}
	dot := strings.LastIndex(seg, ".")
	if dot < 0 {
		return ""
	}
	ext := strings.ToLower(seg[dot+1:])
	return mediaExtToFileType[ext]
}
