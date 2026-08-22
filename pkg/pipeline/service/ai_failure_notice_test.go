package service

import (
	"context"
	"errors"
	"os"
	"testing"

	brtErrors "github.com/EvolutionAPI/evo-bot-runtime/internal/errors"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
)

// CRM-236: when the LLM provider degrades (429 quota, 503 high demand, or just a
// slow tail) the turn exceeds the ceiling. The pipeline used to only log and
// clear state, so NOTHING reached the chat — indistinguishable, for the
// customer, from a bot that is ignoring them. Worse, the tool's side effect may
// already be applied: the pipeline card moves at ~20s and the timeout fires
// later, so the funnel advances while the conversation stays silent.

func captureDispatch(t *testing.T) (*mockDispatchEngine, *[]string) {
	t.Helper()
	var sent []string
	engine := &mockDispatchEngine{
		dispatchFn: func(_ context.Context, _, _ int64, content string, _ model.BotConfig, _ string) error {
			sent = append(sent, content)
			return nil
		},
	}
	return engine, &sent
}

func TestAIFailureNotice_TimeoutTellsTheCustomer(t *testing.T) {
	engine, sent := captureDispatch(t)
	svc, _ := setupSvcWithAIAndDispatch(t, &mockAIAdapter{}, engine)

	svc.sendAIFailureNotice(1, 2, model.BotConfig{}, "http://crm.test/postback/2", brtErrors.ErrAITimeout)

	if len(*sent) != 1 {
		t.Fatalf("expected the customer to receive one notice, got %d", len(*sent))
	}
	if (*sent)[0] != defaultAIFailureNotice {
		t.Errorf("unexpected notice: %q", (*sent)[0])
	}
}

func TestAIFailureNotice_NeverLeaksTheProviderError(t *testing.T) {
	engine, sent := captureDispatch(t)
	svc, _ := setupSvcWithAIAndDispatch(t, &mockAIAdapter{}, engine)

	// A real provider error: model names, quota ids and URLs must not reach the customer.
	cause := errors.New("litellm.RateLimitError: VertexAIException - 429 RESOURCE_EXHAUSTED " +
		"Quota exceeded for metric generativelanguage.googleapis.com/generate_content_free_tier_requests, " +
		"limit: 20, model: gemini-2.5-flash")

	svc.sendAIFailureNotice(1, 2, model.BotConfig{}, "http://crm.test/postback/2", cause)

	if len(*sent) != 1 {
		t.Fatalf("expected one notice, got %d", len(*sent))
	}
	for _, leak := range []string{"gemini", "Quota", "RateLimitError", "googleapis"} {
		if contains((*sent)[0], leak) {
			t.Errorf("provider detail %q leaked to the customer: %q", leak, (*sent)[0])
		}
	}
}

func TestAIFailureNotice_OperatorCanCustomiseIt(t *testing.T) {
	t.Setenv(aiFailureNoticeEnv, "Nosso atendimento automático está indisponível.")
	engine, sent := captureDispatch(t)
	svc, _ := setupSvcWithAIAndDispatch(t, &mockAIAdapter{}, engine)

	svc.sendAIFailureNotice(1, 2, model.BotConfig{}, "http://crm.test/postback/2", brtErrors.ErrAITimeout)

	if len(*sent) != 1 || (*sent)[0] != "Nosso atendimento automático está indisponível." {
		t.Fatalf("custom notice not used: %v", *sent)
	}
}

// An operator who prefers silence must be able to keep it.
func TestAIFailureNotice_EmptyEnvDisablesIt(t *testing.T) {
	t.Setenv(aiFailureNoticeEnv, "")
	engine, sent := captureDispatch(t)
	svc, _ := setupSvcWithAIAndDispatch(t, &mockAIAdapter{}, engine)

	svc.sendAIFailureNotice(1, 2, model.BotConfig{}, "http://crm.test/postback/2", brtErrors.ErrAITimeout)

	if len(*sent) != 0 {
		t.Fatalf("notice should be disabled, got %v", *sent)
	}
}

func TestAIFailureNotice_NoPostbackUrlIsNotACrash(t *testing.T) {
	engine, sent := captureDispatch(t)
	svc, _ := setupSvcWithAIAndDispatch(t, &mockAIAdapter{}, engine)

	svc.sendAIFailureNotice(1, 2, model.BotConfig{}, "", brtErrors.ErrAITimeout)

	if len(*sent) != 0 {
		t.Fatalf("nothing can be dispatched without a postback url, got %v", *sent)
	}
}

// The default must survive an env var that exists but is unrelated.
func TestAIFailureNotice_DefaultWhenEnvUnset(t *testing.T) {
	os.Unsetenv(aiFailureNoticeEnv)
	engine, sent := captureDispatch(t)
	svc, _ := setupSvcWithAIAndDispatch(t, &mockAIAdapter{}, engine)

	svc.sendAIFailureNotice(1, 2, model.BotConfig{}, "http://crm.test/postback/2", brtErrors.ErrAITimeout)

	if len(*sent) != 1 || (*sent)[0] != defaultAIFailureNotice {
		t.Fatalf("expected the default notice, got %v", *sent)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}
