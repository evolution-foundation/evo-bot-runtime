package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/handler"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/repository"
	pipelineService "github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/service"
)

// mockRepo satisfies repository.PipelineRepository for handler tests.
type mockRepo struct {
	setStateCalled bool
	setStateErr    error
	pingErr        error
}

func (m *mockRepo) SetState(_ context.Context, _, _ int64, _ *model.PipelineState) error {
	m.setStateCalled = true
	return m.setStateErr
}
func (m *mockRepo) GetState(_ context.Context, _, _ int64) (*model.PipelineState, error) {
	return nil, nil
}
func (m *mockRepo) ClearState(_ context.Context, _, _ int64) error { return nil }
func (m *mockRepo) AppendToBuffer(_ context.Context, _, _ int64, _ string) error {
	return nil
}
func (m *mockRepo) GetBuffer(_ context.Context, _, _ int64) ([]string, error) {
	return nil, nil
}
func (m *mockRepo) SetTimer(_ context.Context, _, _ int64, _ time.Duration) error { return nil }
func (m *mockRepo) DeleteTimer(_ context.Context, _, _ int64) error               { return nil }
func (m *mockRepo) TimerExists(_ context.Context, _, _ int64) (bool, error)       { return false, nil }
func (m *mockRepo) AcquireLock(_ context.Context, _, _ int64) (repository.Mutex, error) {
	return nil, nil
}
func (m *mockRepo) ScanStates(_ context.Context, _ int) ([]model.PairID, error) {
	return nil, nil
}
func (m *mockRepo) Ping(_ context.Context) error                         { return m.pingErr }

// mockSvc satisfies pipelineService.PipelineService for handler tests.
type mockSvc struct{ processErr error }

func (m *mockSvc) Process(_ context.Context, _ *model.MessageEvent) error { return m.processErr }
func (m *mockSvc) Cancel(_, _ int64) error                                { return nil }
func (m *mockSvc) Start() error                                           { return nil }
func (m *mockSvc) Shutdown(_ context.Context)                             {}

var _ pipelineService.PipelineService = (*mockSvc)(nil)

const testSecret = "test-secret"

func setupRouter(repo repository.PipelineRepository, svc pipelineService.PipelineService) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	hdl := handler.NewHandler(repo, svc, testSecret)
	hdl.RegisterRoutes(r)
	return r
}

func validPayload() []byte {
	event := map[string]any{
		"agent_bot_id":    "bot-1",
		"conversation_id": 42,
		"contact_id":      7,
		"message_id":      "msg-100",
		"message_content": "hello",
		"postback_url":    "http://crm/postback",
		"bot_config": map[string]any{
			"debounce_time":              3,
			"message_signature":          "",
			"text_segmentation_enabled":  false,
			"text_segmentation_limit":    0,
			"text_segmentation_min_size": 0,
			"delay_per_character":        0.0,
		},
	}
	b, _ := json.Marshal(event)
	return b
}

func TestHandleEvent_202_OnValidRequest(t *testing.T) {
	mock := &mockRepo{}
	r := setupRouter(mock, &mockSvc{})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(validPayload()))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bot-Runtime-Secret", testSecret)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusAccepted)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("response body is not valid JSON: %v", err)
	}
	if body["status"] != "accepted" {
		t.Errorf("body.status: got %q, want %q", body["status"], "accepted")
	}
	if !mock.setStateCalled {
		t.Error("StageIncoming state must be persisted before 202 is sent (NFR-01)")
	}
}

func TestHandleEvent_500_OnSetStateError(t *testing.T) {
	mock := &mockRepo{setStateErr: errors.New("redis write failed")}
	r := setupRouter(mock, &mockSvc{})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(validPayload()))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bot-Runtime-Secret", testSecret)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusInternalServerError)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("response body is not valid JSON: %v", err)
	}
	if body["error"] != "internal error" {
		t.Errorf("body.error: got %q, want %q", body["error"], "internal error")
	}
	if body["code"] != "ERR_INTERNAL" {
		t.Errorf("body.code: got %q, want %q", body["code"], "ERR_INTERNAL")
	}
}

func TestHandleEvent_202_EvenWhenProcessFails(t *testing.T) {
	// Process runs in a goroutine — its errors must not affect the HTTP response.
	r := setupRouter(&mockRepo{}, &mockSvc{processErr: errors.New("process error")})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(validPayload()))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bot-Runtime-Secret", testSecret)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusAccepted)
	}
}

func TestHandleEvent_401_OnMissingSecret(t *testing.T) {
	r := setupRouter(&mockRepo{}, &mockSvc{})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(validPayload()))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusUnauthorized)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("response body is not valid JSON: %v", err)
	}
	if body["error"] != "unauthorized" {
		t.Errorf("body.error: got %q, want %q", body["error"], "unauthorized")
	}
	if body["code"] != "ERR_UNAUTHORIZED" {
		t.Errorf("body.code: got %q, want %q", body["code"], "ERR_UNAUTHORIZED")
	}
}

func TestHandleEvent_401_OnWrongSecret(t *testing.T) {
	r := setupRouter(&mockRepo{}, &mockSvc{})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(validPayload()))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bot-Runtime-Secret", "wrong-secret")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestHandleEvent_400_OnMalformedJSON(t *testing.T) {
	r := setupRouter(&mockRepo{}, &mockSvc{})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader([]byte(`{invalid}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bot-Runtime-Secret", testSecret)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusBadRequest)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("response body is not valid JSON: %v", err)
	}
	if body["error"] != "invalid payload" {
		t.Errorf("body.error: got %q, want %q", body["error"], "invalid payload")
	}
	if body["code"] != "ERR_INVALID_EVENT" {
		t.Errorf("body.code: got %q, want %q", body["code"], "ERR_INVALID_EVENT")
	}
}

func TestHandleEvent_400_OnMissingRequiredFields(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		wantErr string
	}{
		{
			name:    "empty body",
			payload: `{}`,
			wantErr: "contact_id must be > 0",
		},
		{
			name:    "missing conversation_id",
			payload: `{"contact_id":1,"postback_url":"http://x"}`,
			wantErr: "conversation_id must be > 0",
		},
		{
			name:    "missing postback_url",
			payload: `{"contact_id":1,"conversation_id":2}`,
			wantErr: "postback_url is required",
		},
		{
			name:    "negative debounce_time",
			payload: `{"contact_id":1,"conversation_id":2,"postback_url":"http://x","bot_config":{"debounce_time":-1}}`,
			wantErr: "debounce_time must be >= 0",
		},
		{
			name:    "segmentation enabled but limit zero",
			payload: `{"contact_id":1,"conversation_id":2,"postback_url":"http://x","bot_config":{"text_segmentation_enabled":true}}`,
			wantErr: "text_segmentation_limit must be > 0 when segmentation is enabled",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := setupRouter(&mockRepo{}, &mockSvc{})
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader([]byte(tc.payload)))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Bot-Runtime-Secret", testSecret)
			r.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("status: got %d, want %d", w.Code, http.StatusBadRequest)
			}
			var body map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("invalid JSON response: %v", err)
			}
			if body["error"] != tc.wantErr {
				t.Errorf("body.error: got %q, want %q", body["error"], tc.wantErr)
			}
			if body["code"] != "ERR_INVALID_EVENT" {
				t.Errorf("body.code: got %q, want %q", body["code"], "ERR_INVALID_EVENT")
			}
		})
	}
}

func TestHealth_200_WhenRedisReachable(t *testing.T) {
	r := setupRouter(&mockRepo{pingErr: nil}, &mockSvc{})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusOK)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("response body is not valid JSON: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("body.status: got %q, want %q", body["status"], "ok")
	}
}

func TestHealth_503_WhenRedisUnreachable(t *testing.T) {
	r := setupRouter(&mockRepo{pingErr: errors.New("connection refused")}, &mockSvc{})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("response body is not valid JSON: %v", err)
	}
	if body["status"] != "error" {
		t.Errorf("body.status: got %q, want %q", body["status"], "error")
	}
	if body["detail"] != "redis unreachable" {
		t.Errorf("body.detail: got %q, want %q", body["detail"], "redis unreachable")
	}
}
