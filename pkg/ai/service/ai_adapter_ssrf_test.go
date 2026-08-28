package service_test

// The attachment URL and the outgoing_url arrive in the same /events payload, so an
// unvalidated download is a read primitive aimed by its caller. These pin the guard.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	aiModel "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
	aiService "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/service"
)

// capturingProc records the parts the adapter posts.
func capturingProc(parts *[]aiModel.JSONRPCPart) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req aiModel.JSONRPCRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		*parts = req.Params.Message.Parts
		_ = json.NewEncoder(w).Encode(aiModel.A2AResponse{
			Result: &aiModel.A2AResult{
				Artifacts: []aiModel.A2AArtifact{{Parts: []aiModel.A2APart{{Type: "text", Text: "ok"}}}},
			},
		})
	}))
}

func fileParts(parts []aiModel.JSONRPCPart) []aiModel.JSONRPCPart {
	out := make([]aiModel.JSONRPCPart, 0, len(parts))
	for _, p := range parts {
		if p.Type == "file" {
			out = append(out, p)
		}
	}
	return out
}

// The exfiltration shape: internal attachment URL, attacker-chosen outgoing_url.
func TestCall_ForeignHostAttachment_IsNotFetched(t *testing.T) {
	t.Setenv("MEDIA_HOST_ALLOWLIST", "evo-crm.internal")
	var reached bool
	internal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("internal-credentials"))
	}))
	defer internal.Close()

	var parts []aiModel.JSONRPCPart
	proc := capturingProc(&parts)
	defer proc.Close()

	adapter := aiService.NewAIAdapter(30, 0, 1)
	if _, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
		OutgoingURL: proc.URL, ContactID: 1, ConversationID: 2, Message: "hi",
		Attachments: []aiModel.Attachment{{URL: internal.URL + "/latest/meta-data/", ContentType: "image/png"}},
	}); err != nil {
		t.Fatalf("a blocked attachment must not error the call: %v", err)
	}

	if reached {
		t.Error("the unauthorized host was fetched — the URL guard did not run")
	}
	if got := fileParts(parts); len(got) != 0 {
		t.Fatalf("file parts = %d, want the attachment dropped", len(got))
	}
	if len(parts) != 1 || parts[0].Type != "text" {
		t.Fatalf("parts = %+v, want the text reply to survive the block", parts)
	}
}

// Blob storage does not always live on the CRM host, so the allowlist must work.
func TestCall_AllowlistedHost_IsFetched(t *testing.T) {
	blob := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write([]byte("png-bytes"))
	}))
	defer blob.Close()

	var parts []aiModel.JSONRPCPart
	proc := capturingProc(&parts)
	defer proc.Close()

	t.Setenv("MEDIA_HOST_ALLOWLIST", "127.0.0.1")
	adapter := aiService.NewAIAdapter(30, 0, 1)
	if _, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
		OutgoingURL: proc.URL, ContactID: 1, ConversationID: 2, Message: "hi",
		Attachments: []aiModel.Attachment{{URL: blob.URL + "/photo.png", ContentType: "image/png"}},
	}); err != nil {
		t.Fatalf("Call: %v", err)
	}

	got := fileParts(parts)
	if len(got) != 1 {
		t.Fatalf("file parts = %d, want the allowlisted host forwarded", len(got))
	}
	decoded, _ := base64.StdEncoding.DecodeString(got[0].File.Bytes)
	if string(decoded) != "png-bytes" {
		t.Errorf("decoded = %q, want the blob bytes", decoded)
	}
}

// Checking the declared URL alone is not enough: a 302 could walk it internal.
func TestCall_RedirectOffTheAuthorizedHost_IsRefused(t *testing.T) {
	// Authorizes 127.0.0.1 (the redirector) but not "localhost" (the target).
	t.Setenv("MEDIA_HOST_ALLOWLIST", "127.0.0.1")
	var reached bool
	internal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		_, _ = w.Write([]byte("internal-after-redirect"))
	}))
	defer internal.Close()

	// httptest always binds 127.0.0.1, so the target uses a host string the
	// allowlist does not carry. Without the redirect check this would succeed.
	target := strings.Replace(internal.URL, "127.0.0.1", "localhost", 1)
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target+"/secret", http.StatusFound)
	}))
	defer redirector.Close()

	var parts []aiModel.JSONRPCPart
	proc := capturingProc(&parts)
	defer proc.Close()

	adapter := aiService.NewAIAdapter(30, 0, 1)
	if _, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
		OutgoingURL: proc.URL, ContactID: 1, ConversationID: 2, Message: "hi",
		Attachments: []aiModel.Attachment{{URL: redirector.URL + "/photo.png", ContentType: "image/png"}},
	}); err != nil {
		t.Fatalf("a refused redirect must not error the call: %v", err)
	}

	if reached {
		t.Error("the redirect target was fetched — the redirect is not re-checked")
	}
	if got := fileParts(parts); len(got) != 0 {
		t.Fatalf("file parts = %d, want the redirected download dropped", len(got))
	}
}

// Only http/https may be addressed.
func TestCall_NonHTTPScheme_IsRejected(t *testing.T) {
	t.Setenv("MEDIA_HOST_ALLOWLIST", "127.0.0.1")
	var parts []aiModel.JSONRPCPart
	proc := capturingProc(&parts)
	defer proc.Close()

	adapter := aiService.NewAIAdapter(30, 0, 1)
	if _, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
		OutgoingURL: proc.URL, ContactID: 1, ConversationID: 2, Message: "hi",
		Attachments: []aiModel.Attachment{{URL: "file:///etc/passwd", ContentType: "image/png"}},
	}); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if got := fileParts(parts); len(got) != 0 {
		t.Fatalf("file parts = %d, want the file:// URL dropped", len(got))
	}
}

// Nothing authorized means nothing fetched — failing closed is the point. This is
// also what an unconfigured deployment gets, which is why MEDIA_HOST_ALLOWLIST has
// to be shipped alongside the service.
func TestCall_NoAuthorizedHost_ForwardsNothing(t *testing.T) {
	var reached bool
	blob := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
	}))
	defer blob.Close()

	var parts []aiModel.JSONRPCPart
	proc := capturingProc(&parts)
	defer proc.Close()

	adapter := aiService.NewAIAdapter(30, 0, 1)
	if _, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
		OutgoingURL: proc.URL, ContactID: 1, ConversationID: 2, Message: "hi",
		Attachments: []aiModel.Attachment{{URL: blob.URL + "/photo.png", ContentType: "image/png"}},
	}); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if reached {
		t.Error("an attachment was fetched with no authorized host configured")
	}
	if len(parts) != 1 || parts[0].Type != "text" {
		t.Fatalf("parts = %+v, want text only", parts)
	}
}
