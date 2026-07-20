package service_test

// EVO-2180: incoming media must be downloaded and forwarded to the AI Processor as
// base64 A2A file parts; a download failure must NOT break the text-only message.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

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
