package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	brtErrors "github.com/EvolutionAPI/evo-bot-runtime/internal/errors"
	aiModel "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/dispatch/service"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
)

// postbackBody mirrors the JSON body dispatched to the postback endpoint.
type postbackBody struct {
	Content     string `json:"content"`
	MessageType string `json:"message_type"`
	ContentType string `json:"content_type"`
}

func collectParts(t *testing.T) (*httptest.Server, *[]string, *sync.Mutex) {
	t.Helper()
	var parts []string
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b postbackBody
		json.NewDecoder(r.Body).Decode(&b) //nolint:errcheck
		mu.Lock()
		parts = append(parts, b.Content)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	return server, &parts, &mu
}

func TestDispatch_MultiPart_SignatureOnFirstOnly(t *testing.T) {
	server, partsPtr, mu := collectParts(t)

	eng := service.NewDispatchEngine("")
	cfg := model.BotConfig{
		TextSegmentationEnabled: true,
		TextSegmentationLimit:   15, // forces multiple parts
		TextSegmentationMinSize: 0,
		MessageSignature:        "[bot] ",
		DelayPerCharacter:       0, // no delay — fast test
	}

	// "hello world this is test" → segments of ≤15 chars
	resp1 := &aiModel.NormalizedResponse{Content: "hello world this is test", ContentType: "text"}
	if err := eng.Dispatch(context.Background(), 1, 1, resp1, cfg, server.URL); err != nil {
		t.Fatalf("Dispatch returned unexpected error: %v", err)
	}

	mu.Lock()
	parts := make([]string, len(*partsPtr))
	copy(parts, *partsPtr)
	mu.Unlock()

	if len(parts) <= 1 {
		t.Fatalf("segmentation must produce multiple parts, got %d", len(parts))
	}

	// Signature only on first part
	if !strings.HasPrefix(parts[0], "[bot] ") {
		t.Errorf("signature must be prepended to first part: %q", parts[0])
	}
	for i, p := range parts[1:] {
		if strings.Contains(p, "[bot]") {
			t.Errorf("signature must NOT be on part %d: %q", i+1, p)
		}
	}
}

func TestDispatch_NoSegmentation_SinglePart(t *testing.T) {
	server, partsPtr, mu := collectParts(t)

	eng := service.NewDispatchEngine("")
	cfg := model.BotConfig{
		TextSegmentationEnabled: false,
		MessageSignature:        "—signature ",
		DelayPerCharacter:       0,
	}

	resp2 := &aiModel.NormalizedResponse{Content: "full response here", ContentType: "text"}
	if err := eng.Dispatch(context.Background(), 2, 2, resp2, cfg, server.URL); err != nil {
		t.Fatalf("Dispatch returned unexpected error: %v", err)
	}

	mu.Lock()
	parts := make([]string, len(*partsPtr))
	copy(parts, *partsPtr)
	mu.Unlock()

	if len(parts) != 1 {
		t.Fatalf("disabled segmentation must produce exactly one part, got %d", len(parts))
	}
	want := "—signature full response here"
	if parts[0] != want {
		t.Errorf("parts[0] = %q, want %q", parts[0], want)
	}
}

func TestDispatch_Cancellation_ReturnsInterrupted(t *testing.T) {
	sentCount := 0
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		sentCount++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())

	eng := service.NewDispatchEngine("")
	cfg := model.BotConfig{
		TextSegmentationEnabled: true,
		TextSegmentationLimit:   5,  // small limit → many parts
		DelayPerCharacter:       10, // 10ms per char → enough time to cancel
	}

	go func() {
		time.Sleep(20 * time.Millisecond) // cancel after first part + delay window starts
		cancel()
	}()

	resp3 := &aiModel.NormalizedResponse{Content: "alpha beta gamma delta epsilon", ContentType: "text"}
	err := eng.Dispatch(ctx, 3, 3, resp3, cfg, server.URL)
	if !errors.Is(err, brtErrors.ErrDispatchInterrupted) {
		t.Errorf("expected ErrDispatchInterrupted, got %v", err)
	}

	mu.Lock()
	sent := sentCount
	mu.Unlock()
	if sent < 1 {
		t.Errorf("at least 1 part must be sent before cancellation, got sent = %d", sent)
	}
	if sent >= 5 {
		t.Errorf("cancelled dispatch must not send all parts, sent = %d", sent)
	}
}

func TestDispatch_EmptySignature_NoSuffix(t *testing.T) {
	server, partsPtr, mu := collectParts(t)

	eng := service.NewDispatchEngine("")
	cfg := model.BotConfig{
		TextSegmentationEnabled: false,
		MessageSignature:        "", // empty — no suffix
	}

	resp4 := &aiModel.NormalizedResponse{Content: "no signature here", ContentType: "text"}
	if err := eng.Dispatch(context.Background(), 4, 4, resp4, cfg, server.URL); err != nil {
		t.Fatalf("Dispatch returned unexpected error: %v", err)
	}

	mu.Lock()
	parts := make([]string, len(*partsPtr))
	copy(parts, *partsPtr)
	mu.Unlock()

	if len(parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(parts))
	}
	if parts[0] != "no signature here" {
		t.Errorf("parts[0] = %q, want %q (empty signature must not append anything)", parts[0], "no signature here")
	}
}

func TestDispatch_NonOKResponse_ReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	eng := service.NewDispatchEngine("")
	cfg := model.BotConfig{TextSegmentationEnabled: false}

	resp8 := &aiModel.NormalizedResponse{Content: "some content", ContentType: "text"}
	err := eng.Dispatch(context.Background(), 8, 8, resp8, cfg, server.URL)
	if err == nil {
		t.Fatal("expected error for non-2xx response, got nil")
	}
}

func TestSegmentContent_MergeDoesNotExceedLimit(t *testing.T) {
	// "hello world"(11 runes) fits limit=11; "test"(4) < minSize=5 but merging
	// would produce "hello world test"(16 runes) > limit=11 → must NOT merge.
	server, partsPtr, mu := collectParts(t)

	eng := service.NewDispatchEngine("")
	cfg := model.BotConfig{
		TextSegmentationEnabled: true,
		TextSegmentationLimit:   11,
		TextSegmentationMinSize: 5,
		DelayPerCharacter:       0,
	}

	resp6 := &aiModel.NormalizedResponse{Content: "hello world test", ContentType: "text"}
	if err := eng.Dispatch(context.Background(), 6, 6, resp6, cfg, server.URL); err != nil {
		t.Fatalf("Dispatch returned unexpected error: %v", err)
	}

	mu.Lock()
	parts := make([]string, len(*partsPtr))
	copy(parts, *partsPtr)
	mu.Unlock()

	if len(parts) != 2 {
		t.Fatalf("merge must not exceed limit: expected 2 parts, got %d: %v", len(parts), parts)
	}
	if parts[0] != "hello world" {
		t.Errorf("parts[0] = %q, want %q", parts[0], "hello world")
	}
	if parts[1] != "test" {
		t.Errorf("parts[1] = %q, want %q", parts[1], "test")
	}
}

func TestSegmentContent_RuneAwareLimits(t *testing.T) {
	// "olá"=3 runes (4 bytes), "mundo"=5 runes (5 bytes).
	// limit=9 runes: "olá mundo"=9 runes → fits as a single part.
	// A byte-counting bug would compute 10 bytes > 9 and wrongly split into 2 parts.
	server, partsPtr, mu := collectParts(t)

	eng := service.NewDispatchEngine("")
	cfg := model.BotConfig{
		TextSegmentationEnabled: true,
		TextSegmentationLimit:   9, // rune limit — "olá mundo" is exactly 9 runes
		TextSegmentationMinSize: 0,
		DelayPerCharacter:       0,
	}

	resp7 := &aiModel.NormalizedResponse{Content: "olá mundo", ContentType: "text"}
	if err := eng.Dispatch(context.Background(), 7, 7, resp7, cfg, server.URL); err != nil {
		t.Fatalf("Dispatch returned unexpected error: %v", err)
	}

	mu.Lock()
	parts := make([]string, len(*partsPtr))
	copy(parts, *partsPtr)
	mu.Unlock()

	if len(parts) != 1 {
		t.Fatalf("rune-aware limit=9 must keep 9-rune string as single part, got %d parts: %v", len(parts), parts)
	}
	if parts[0] != "olá mundo" {
		t.Errorf("parts[0] = %q, want %q", parts[0], "olá mundo")
	}
}

// postbackBodyWithItems captures items and attachments for select/media tests.
type postbackBodyWithItems struct {
	Content     string               `json:"content"`
	MessageType string               `json:"message_type"`
	ContentType string               `json:"content_type"`
	Items       []aiModel.SelectItem `json:"items,omitempty"`
	Attachments []postbackAttachment `json:"attachments,omitempty"`
}

type postbackAttachment struct {
	URL      string `json:"url"`
	FileType string `json:"file_type"`
}

func TestDispatch_InputSelect_NoSegmentation(t *testing.T) {
	var bodies []postbackBodyWithItems
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b postbackBodyWithItems
		json.NewDecoder(r.Body).Decode(&b) //nolint:errcheck
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	eng := service.NewDispatchEngine("")
	cfg := model.BotConfig{
		TextSegmentationEnabled: true,
		TextSegmentationLimit:   5, // would normally split "choose an option" into multiple parts
		DelayPerCharacter:       0,
	}

	items := []aiModel.SelectItem{
		{Title: "Yes", Value: "yes"},
		{Title: "No", Value: "no"},
	}
	resp := &aiModel.NormalizedResponse{
		Content:     "choose an option",
		ContentType: "input_select",
		Items:       items,
	}
	if err := eng.Dispatch(context.Background(), 10, 10, resp, cfg, server.URL); err != nil {
		t.Fatalf("Dispatch returned unexpected error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(bodies) != 1 {
		t.Fatalf("input_select must not be segmented: expected 1 postback, got %d", len(bodies))
	}
	if bodies[0].ContentType != "input_select" {
		t.Errorf("ContentType = %q, want %q", bodies[0].ContentType, "input_select")
	}
	if bodies[0].Content != "choose an option" {
		t.Errorf("Content = %q, want %q", bodies[0].Content, "choose an option")
	}
	if len(bodies[0].Items) != 2 {
		t.Fatalf("Items length = %d, want 2", len(bodies[0].Items))
	}
	if bodies[0].Items[0].Title != "Yes" || bodies[0].Items[0].Value != "yes" {
		t.Errorf("Items[0] = %+v, want {Yes yes}", bodies[0].Items[0])
	}
	if bodies[0].Items[1].Title != "No" || bodies[0].Items[1].Value != "no" {
		t.Errorf("Items[1] = %+v, want {No no}", bodies[0].Items[1])
	}
}

// TestDispatch_InputSelect_EmptyContent_StillSendsItems is a regression test:
// when the AI processor sends a select block with no accompanying question
// text (Content == ""), the items must still reach the postback endpoint.
// A prior version of the "skip empty residual" guard (meant for the
// text/media path) also swallowed empty input_select parts, silently
// dropping the items.
func TestDispatch_InputSelect_EmptyContent_StillSendsItems(t *testing.T) {
	var bodies []postbackBodyWithItems
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b postbackBodyWithItems
		json.NewDecoder(r.Body).Decode(&b) //nolint:errcheck
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	eng := service.NewDispatchEngine("")
	cfg := model.BotConfig{TextSegmentationEnabled: false}

	items := []aiModel.SelectItem{
		{Title: "Yes", Value: "yes"},
		{Title: "No", Value: "no"},
	}
	resp := &aiModel.NormalizedResponse{
		Content:     "",
		ContentType: "input_select",
		Items:       items,
	}
	if err := eng.Dispatch(context.Background(), 12, 12, resp, cfg, server.URL); err != nil {
		t.Fatalf("Dispatch returned unexpected error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(bodies) != 1 {
		t.Fatalf("expected 1 postback carrying the items even with empty content, got %d", len(bodies))
	}
	if bodies[0].ContentType != "input_select" {
		t.Errorf("ContentType = %q, want %q", bodies[0].ContentType, "input_select")
	}
	if len(bodies[0].Items) != 2 {
		t.Fatalf("Items length = %d, want 2 — items must not be dropped when Content is empty", len(bodies[0].Items))
	}
}

func TestDispatch_TextWithMedia_ExtractsAttachments(t *testing.T) {
	var bodies []postbackBodyWithItems
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b postbackBodyWithItems
		json.NewDecoder(r.Body).Decode(&b) //nolint:errcheck
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	eng := service.NewDispatchEngine("")
	cfg := model.BotConfig{
		TextSegmentationEnabled: false,
		DelayPerCharacter:       0,
	}

	resp := &aiModel.NormalizedResponse{
		Content:     "Check this photo https://example.com/image.png",
		ContentType: "text",
	}
	if err := eng.Dispatch(context.Background(), 11, 11, resp, cfg, server.URL); err != nil {
		t.Fatalf("Dispatch returned unexpected error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	// Expect 2 postbacks: 1 text (media URL stripped) + 1 media-only
	if len(bodies) != 2 {
		t.Fatalf("expected 2 postbacks (text + media), got %d", len(bodies))
	}

	// First postback: text with media URL removed
	if bodies[0].ContentType != "text" {
		t.Errorf("bodies[0].ContentType = %q, want %q", bodies[0].ContentType, "text")
	}
	if strings.Contains(bodies[0].Content, "image.png") {
		t.Errorf("bodies[0].Content must not contain media URL: %q", bodies[0].Content)
	}
	if len(bodies[0].Attachments) != 0 {
		t.Errorf("bodies[0] must have no attachments, got %d", len(bodies[0].Attachments))
	}

	// Second postback: media-only (empty content, 1 attachment)
	if bodies[1].Content != "" {
		t.Errorf("bodies[1].Content = %q, want empty (media-only)", bodies[1].Content)
	}
	if len(bodies[1].Attachments) != 1 {
		t.Fatalf("bodies[1].Attachments length = %d, want 1", len(bodies[1].Attachments))
	}
	if bodies[1].Attachments[0].URL != "https://example.com/image.png" {
		t.Errorf("Attachments[0].URL = %q, want %q", bodies[1].Attachments[0].URL, "https://example.com/image.png")
	}
	if bodies[1].Attachments[0].FileType != "image" {
		t.Errorf("Attachments[0].FileType = %q, want %q", bodies[1].Attachments[0].FileType, "image")
	}
}

func TestDispatch_ValidatesPostBody(t *testing.T) {
	var received postbackBody
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", ct)
		}
		json.NewDecoder(r.Body).Decode(&received) //nolint:errcheck
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	eng := service.NewDispatchEngine("")
	cfg := model.BotConfig{TextSegmentationEnabled: false}

	resp5 := &aiModel.NormalizedResponse{Content: "test content", ContentType: "text"}
	if err := eng.Dispatch(context.Background(), 5, 5, resp5, cfg, server.URL); err != nil {
		t.Fatalf("Dispatch returned unexpected error: %v", err)
	}

	if received.Content != "test content" {
		t.Errorf("received.Content = %q, want %q", received.Content, "test content")
	}
	if received.MessageType != "outgoing" {
		t.Errorf("received.MessageType = %q, want %q", received.MessageType, "outgoing")
	}
	if received.ContentType != "text" {
		t.Errorf("received.ContentType = %q, want %q", received.ContentType, "text")
	}
}
