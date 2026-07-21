package service_test

// The attachment URL and the outgoing_url arrive in the same /events payload, so an
// unvalidated download is not "a fetch that might fail" — it is a read primitive
// aimed by the caller whose response is delivered back to the caller. These tests
// pin the guard that keeps the fetch on hosts the CRM is known to serve blobs from.

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

// capturingProc records the parts the adapter posts and answers with a minimal
// successful A2A response.
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

// The exfiltration shape: an attachment URL pointing at an internal service, and an
// outgoing_url pointing at the attacker. Nothing internal may reach the file parts.
func TestCall_ForeignHostAttachment_IsNotFetched(t *testing.T) {
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
		// A CRM on a host that does not serve the attachment below.
		PostbackURL: "http://evo-crm.internal:3000/webhooks/bot_runtime/postback/7",
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

// A host allowlisted through MEDIA_HOST_ALLOWLIST must still be reachable: blob
// storage does not always live on the CRM host (ActiveStorage redirect mode, CDN).
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
		PostbackURL: "http://evo-crm.internal:3000/webhooks/bot_runtime/postback/7",
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

// A host check on the declared URL alone is not enough: an authorized host that
// answers 302 could otherwise walk the download onto an internal address.
func TestCall_RedirectOffTheAuthorizedHost_IsRefused(t *testing.T) {
	var reached bool
	internal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		_, _ = w.Write([]byte("internal-after-redirect"))
	}))
	defer internal.Close()

	// httptest always binds 127.0.0.1, so the redirect target is addressed by a
	// hostname the allowlist does not carry ("localhost") — same machine, different
	// host string. Without the redirect check the fetch would succeed.
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
		// Authorizes 127.0.0.1 (the redirector) but not "localhost" (the target).
		PostbackURL: redirector.URL,
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

// Only http/https may be addressed: the URL must not be able to reach another
// protocol handler.
func TestCall_NonHTTPScheme_IsRejected(t *testing.T) {
	var parts []aiModel.JSONRPCPart
	proc := capturingProc(&parts)
	defer proc.Close()

	adapter := aiService.NewAIAdapter(30, 0, 1)
	if _, err := adapter.Call(context.Background(), &aiModel.A2ARequest{
		OutgoingURL: proc.URL, ContactID: 1, ConversationID: 2, Message: "hi",
		PostbackURL: proc.URL,
		Attachments: []aiModel.Attachment{{URL: "file:///etc/passwd", ContentType: "image/png"}},
	}); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if got := fileParts(parts); len(got) != 0 {
		t.Fatalf("file parts = %d, want the file:// URL dropped", len(got))
	}
}

// With no postback URL and no allowlist nothing is authorized, so nothing is
// fetched. Failing closed is the point: the alternative is fetching whatever the
// payload asks for.
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
