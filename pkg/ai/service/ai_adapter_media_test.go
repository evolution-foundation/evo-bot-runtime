package service_test

// EVO-2180: incoming media must be downloaded and forwarded to the AI Processor as
// base64 A2A file parts; a download failure must NOT break the text-only message.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	aiModel "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
	aiService "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/service"
)

// procServer stands up a fake AI Processor that captures the JSON-RPC parts it
// receives and returns a minimal successful response.
func procServer(t *testing.T, capture *[]aiModel.JSONRPCPart) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req aiModel.JSONRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		*capture = req.Params.Message.Parts
		_ = json.NewEncoder(w).Encode(aiModel.A2AResponse{
			Result: &aiModel.A2AResult{
				Artifacts: []aiModel.A2AArtifact{
					{Parts: []aiModel.A2APart{{Type: "text", Text: "ok"}}},
				},
			},
		})
	}))
}

func TestCall_ForwardsAttachmentAsFilePart(t *testing.T) {
	t.Setenv("MEDIA_HOST_ALLOWLIST", "127.0.0.1")
	imgBytes := []byte("\x89PNG\r\n-fake-image-bytes")
	fileSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(imgBytes)
	}))
	defer fileSrv.Close()

	var parts []aiModel.JSONRPCPart
	proc := procServer(t, &parts)
	defer proc.Close()

	adapter := aiService.NewAIAdapter(30, 0, 1)
	_, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
		OutgoingURL:    proc.URL + "/api/v1/a2a/agent-1",
		ContactID:      1,
		ConversationID: 2,
		ApiKey:         "k",
		Message:        "look at this",
		Attachments: []aiModel.Attachment{
			{URL: fileSrv.URL + "/photo.png", ContentType: "image/png", FileType: "image"},
		},
	})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}

	if len(parts) != 2 {
		t.Fatalf("parts = %d, want 2 (text + file)", len(parts))
	}
	if parts[0].Type != "text" || parts[0].Text != "look at this" {
		t.Errorf("parts[0] = %+v, want text 'look at this'", parts[0])
	}
	if parts[1].Type != "file" || parts[1].File == nil {
		t.Fatalf("parts[1] = %+v, want a file part", parts[1])
	}
	if parts[1].File.MimeType != "image/png" {
		t.Errorf("file mimeType = %q, want image/png", parts[1].File.MimeType)
	}
	if parts[1].File.Name != "photo.png" {
		t.Errorf("file name = %q, want photo.png", parts[1].File.Name)
	}
	decoded, err := base64.StdEncoding.DecodeString(parts[1].File.Bytes)
	if err != nil {
		t.Fatalf("file bytes not valid base64: %v", err)
	}
	if !bytes.Equal(decoded, imgBytes) {
		t.Errorf("decoded bytes != original image bytes")
	}
}

func TestCall_AttachmentDownloadFailure_SendsTextOnly(t *testing.T) {
	t.Setenv("MEDIA_HOST_ALLOWLIST", "127.0.0.1")
	var parts []aiModel.JSONRPCPart
	proc := procServer(t, &parts)
	defer proc.Close()

	adapter := aiService.NewAIAdapter(30, 0, 1)
	_, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
		OutgoingURL:    proc.URL + "/api/v1/a2a/agent-1",
		ContactID:      1,
		ConversationID: 2,
		ApiKey:         "k",
		Message:        "hi",
		Attachments: []aiModel.Attachment{
			{URL: "http://127.0.0.1:1/unreachable.png", ContentType: "image/png", FileType: "image"},
		},
	})
	if err != nil {
		t.Fatalf("a download failure must not error the call: %v", err)
	}
	if len(parts) != 1 || parts[0].Type != "text" {
		t.Fatalf("parts = %+v, want a single text part when the download fails", parts)
	}
}

// mediaServer serves fixed bytes with a fixed Content-Type, counting the requests
// it answered so a test can assert which attachments were even attempted.
func mediaServer(contentType string, body []byte, hits *int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(hits, 1)
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		_, _ = w.Write(body)
	}))
}

func filePartsOf(parts []aiModel.JSONRPCPart) []aiModel.JSONRPCPart {
	out := make([]aiModel.JSONRPCPart, 0, len(parts))
	for _, p := range parts {
		if p.Type == "file" {
			out = append(out, p)
		}
	}
	return out
}

// The debounce window aggregates the media of every message in it, so the shared
// byte budget — not just the per-file cap — is what keeps the request under the
// gateway's client_max_body_size. Over budget the extra media is dropped and the
// call still goes out.
func TestCall_TotalAttachmentBudget_DropsExcessAndStillSends(t *testing.T) {
	t.Setenv("MEDIA_HOST_ALLOWLIST", "127.0.0.1")
	var hits int32
	chunk := make([]byte, 6<<20) // 6 MiB each: 4 of them exceed the 20 MiB budget
	fileSrv := mediaServer("image/png", chunk, &hits)
	defer fileSrv.Close()

	var parts []aiModel.JSONRPCPart
	proc := procServer(t, &parts)
	defer proc.Close()

	atts := make([]aiModel.Attachment, 0, 4)
	for i := 0; i < 4; i++ {
		atts = append(atts, aiModel.Attachment{
			URL: fmt.Sprintf("%s/%d.png", fileSrv.URL, i), ContentType: "image/png", FileType: "image",
		})
	}

	adapter := aiService.NewAIAdapter(30, 0, 1)
	if _, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
		OutgoingURL: proc.URL, ContactID: 1, ConversationID: 2, Message: "album", Attachments: atts,
	}); err != nil {
		t.Fatalf("a media budget overflow must not error the call: %v", err)
	}

	files := filePartsOf(parts)
	if len(files) != 3 {
		t.Fatalf("forwarded %d file parts, want 3 (4x6 MiB capped at a 20 MiB budget)", len(files))
	}
	if len(parts) != 4 || parts[0].Type != "text" {
		t.Errorf("parts = %d with parts[0]=%q, want the text part plus 3 files", len(parts), parts[0].Type)
	}
}

// An unreachable media host must not hold the turn hostage: the whole set shares a
// time budget, so latency stays bounded no matter how many attachments arrive.
func TestCall_AttachmentTimeBudget_IsBounded(t *testing.T) {
	t.Setenv("MEDIA_HOST_ALLOWLIST", "127.0.0.1")
	hung := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer hung.Close()

	var parts []aiModel.JSONRPCPart
	proc := procServer(t, &parts)
	defer proc.Close()

	atts := make([]aiModel.Attachment, 0, 8)
	for i := 0; i < 8; i++ {
		atts = append(atts, aiModel.Attachment{URL: fmt.Sprintf("%s/%d.png", hung.URL, i), ContentType: "image/png"})
	}

	// timeoutSecs=1 → 1s per download, 3s for the set. Serial per-attachment
	// timeouts would take 8s.
	adapter := aiService.NewAIAdapter(1, 0, 1)
	start := time.Now()
	if _, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
		OutgoingURL: proc.URL, ContactID: 1, ConversationID: 2, Message: "hi", Attachments: atts,
	}); err != nil {
		t.Fatalf("unreachable media must not error the call: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 6*time.Second {
		t.Errorf("attachment phase took %v, want it bounded near the 3s budget", elapsed)
	}
	if len(parts) != 1 || parts[0].Type != "text" {
		t.Errorf("parts = %+v, want text only when every download hangs", parts)
	}
}

// A Rails proxy URL that answers 200 with an error/login page must not be
// forwarded as an image: the processor passes the mime straight into the model call.
func TestCall_HTMLResponse_IsNotForwardedAsMedia(t *testing.T) {
	t.Setenv("MEDIA_HOST_ALLOWLIST", "127.0.0.1")
	var hits int32
	htmlSrv := mediaServer("text/html; charset=utf-8", []byte("<html>login</html>"), &hits)
	defer htmlSrv.Close()

	var parts []aiModel.JSONRPCPart
	proc := procServer(t, &parts)
	defer proc.Close()

	adapter := aiService.NewAIAdapter(30, 0, 1)
	if _, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
		OutgoingURL: proc.URL, ContactID: 1, ConversationID: 2, Message: "hi",
		Attachments: []aiModel.Attachment{{URL: htmlSrv.URL + "/photo.jpg", ContentType: "image/jpeg", FileType: "image"}},
	}); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if len(parts) != 1 || parts[0].Type != "text" {
		t.Fatalf("parts = %+v, want text only — an HTML body is not media", parts)
	}
}

// When the CRM omits content_type the mime must still describe the bytes: the
// response header wins, and the URL extension is the last resort. Anything that
// resolves to an opaque blob is dropped rather than sent as octet-stream, which the
// model APIs reject.
func TestCall_MimeTypeResolution(t *testing.T) {
	t.Setenv("MEDIA_HOST_ALLOWLIST", "127.0.0.1")
	cases := []struct {
		name            string
		respContentType string
		declared        string
		urlPath         string
		wantMime        string // "" = attachment must be skipped
	}{
		{"response header wins over a missing declaration", "image/jpeg", "", "/blobs/proxy/abc123", "image/jpeg"},
		{"declared type wins over an opaque response", "application/octet-stream", "image/png", "/blobs/proxy/abc123", "image/png"},
		{"url extension is the last resort", "application/octet-stream", "", "/photo.png", "image/png"},
		{"nothing identifiable is dropped", "application/octet-stream", "", "/blobs/proxy/abc123", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var hits int32
			srv := mediaServer(tc.respContentType, []byte("bytes"), &hits)
			defer srv.Close()

			var parts []aiModel.JSONRPCPart
			proc := procServer(t, &parts)
			defer proc.Close()

			adapter := aiService.NewAIAdapter(30, 0, 1)
			if _, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
				OutgoingURL: proc.URL, ContactID: 1, ConversationID: 2, Message: "hi",
				Attachments: []aiModel.Attachment{{URL: srv.URL + tc.urlPath, ContentType: tc.declared, FileType: "image"}},
			}); err != nil {
				t.Fatalf("Call: %v", err)
			}
			files := filePartsOf(parts)
			if tc.wantMime == "" {
				if len(files) != 0 {
					t.Fatalf("file parts = %d, want the attachment dropped", len(files))
				}
				return
			}
			if len(files) != 1 {
				t.Fatalf("file parts = %d, want 1", len(files))
			}
			if files[0].File.MimeType != tc.wantMime {
				t.Errorf("mimeType = %q, want %q", files[0].File.MimeType, tc.wantMime)
			}
		})
	}
}

// A single file over the per-attachment cap is skipped, and the text still goes out.
func TestCall_OversizeAttachment_SendsTextOnly(t *testing.T) {
	t.Setenv("MEDIA_HOST_ALLOWLIST", "127.0.0.1")
	var hits int32
	srv := mediaServer("image/png", make([]byte, (15<<20)+1), &hits)
	defer srv.Close()

	var parts []aiModel.JSONRPCPart
	proc := procServer(t, &parts)
	defer proc.Close()

	adapter := aiService.NewAIAdapter(30, 0, 1)
	if _, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
		OutgoingURL: proc.URL, ContactID: 1, ConversationID: 2, Message: "hi",
		Attachments: []aiModel.Attachment{{URL: srv.URL + "/big.png", ContentType: "image/png", FileType: "image"}},
	}); err != nil {
		t.Fatalf("an oversize attachment must not error the call: %v", err)
	}
	if len(parts) != 1 || parts[0].Type != "text" {
		t.Fatalf("parts = %+v, want text only", parts)
	}
}
