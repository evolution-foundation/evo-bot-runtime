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

	eng := service.NewDispatchEngine()
	cfg := model.BotConfig{
		TextSegmentationEnabled: true,
		TextSegmentationLimit:   15, // forces multiple parts
		TextSegmentationMinSize: 0,
		MessageSignature:        "[bot] ",
		DelayPerCharacter:       0, // no delay — fast test
	}

	// "hello world this is test" → segments of ≤15 chars
	if err := eng.Dispatch(context.Background(), 1, 1, "hello world this is test", cfg, server.URL); err != nil {
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

	eng := service.NewDispatchEngine()
	cfg := model.BotConfig{
		TextSegmentationEnabled: false,
		MessageSignature:        "—signature ",
		DelayPerCharacter:       0,
	}

	if err := eng.Dispatch(context.Background(), 2, 2, "full response here", cfg, server.URL); err != nil {
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

	eng := service.NewDispatchEngine()
	cfg := model.BotConfig{
		TextSegmentationEnabled: true,
		TextSegmentationLimit:   5,  // small limit → many parts
		DelayPerCharacter:       10, // 10ms per char → enough time to cancel
	}

	go func() {
		time.Sleep(20 * time.Millisecond) // cancel after first part + delay window starts
		cancel()
	}()

	err := eng.Dispatch(ctx, 3, 3, "alpha beta gamma delta epsilon", cfg, server.URL)
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

	eng := service.NewDispatchEngine()
	cfg := model.BotConfig{
		TextSegmentationEnabled: false,
		MessageSignature:        "", // empty — no suffix
	}

	if err := eng.Dispatch(context.Background(), 4, 4, "no signature here", cfg, server.URL); err != nil {
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

	eng := service.NewDispatchEngine()
	cfg := model.BotConfig{TextSegmentationEnabled: false}

	err := eng.Dispatch(context.Background(), 8, 8, "some content", cfg, server.URL)
	if err == nil {
		t.Fatal("expected error for non-2xx response, got nil")
	}
}

func TestSegmentContent_MergeDoesNotExceedLimit(t *testing.T) {
	// "hello world"(11 runes) fits limit=11; "test"(4) < minSize=5 but merging
	// would produce "hello world test"(16 runes) > limit=11 → must NOT merge.
	server, partsPtr, mu := collectParts(t)

	eng := service.NewDispatchEngine()
	cfg := model.BotConfig{
		TextSegmentationEnabled: true,
		TextSegmentationLimit:   11,
		TextSegmentationMinSize: 5,
		DelayPerCharacter:       0,
	}

	if err := eng.Dispatch(context.Background(), 6, 6, "hello world test", cfg, server.URL); err != nil {
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

	eng := service.NewDispatchEngine()
	cfg := model.BotConfig{
		TextSegmentationEnabled: true,
		TextSegmentationLimit:   9, // rune limit — "olá mundo" is exactly 9 runes
		TextSegmentationMinSize: 0,
		DelayPerCharacter:       0,
	}

	if err := eng.Dispatch(context.Background(), 7, 7, "olá mundo", cfg, server.URL); err != nil {
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

	eng := service.NewDispatchEngine()
	cfg := model.BotConfig{TextSegmentationEnabled: false}

	if err := eng.Dispatch(context.Background(), 5, 5, "test content", cfg, server.URL); err != nil {
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
