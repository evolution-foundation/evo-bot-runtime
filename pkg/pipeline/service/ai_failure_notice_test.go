package service

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	brtErrors "github.com/EvolutionAPI/evo-bot-runtime/internal/errors"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
)

// CRM-236: a degraded provider used to end the turn in silence, while the tool's
// side effect (a moved pipeline card) had already been applied.

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
		if strings.Contains((*sent)[0], leak) {
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
	// Low 13: restore whatever the process had, instead of leaving the env mutated
	// for every test that runs after this one.
	if previous, had := os.LookupEnv(aiFailureNoticeEnv); had {
		t.Cleanup(func() { os.Setenv(aiFailureNoticeEnv, previous) })
	}
	os.Unsetenv(aiFailureNoticeEnv)
	engine, sent := captureDispatch(t)
	svc, _ := setupSvcWithAIAndDispatch(t, &mockAIAdapter{}, engine)

	svc.sendAIFailureNotice(1, 2, model.BotConfig{}, "http://crm.test/postback/2", brtErrors.ErrAITimeout)

	if len(*sent) != 1 || (*sent)[0] != defaultAIFailureNotice {
		t.Fatalf("expected the default notice, got %v", *sent)
	}
}

func TestAIFailureNotice_DoesNotWriteTurnState(t *testing.T) {
	engine, _ := captureDispatch(t)
	svc, _ := setupSvcWithAIAndDispatch(t, &mockAIAdapter{}, engine)

	// The callers already cleared the state before asking for the notice; writing
	// StageDone here would resurrect state for a turn that is over — and, in the
	// follow-up race, stamp it over the NEW turn's state.
	svc.sendAIFailureNotice(1, 2, model.BotConfig{}, "http://crm.test/postback/2", brtErrors.ErrAITimeout)

	// A read error must fail the test, not pass it: GetState returns (nil, nil)
	// for a missing key, so nil-with-error proves nothing about what was written.
	state, err := svc.repo.GetState(context.Background(), 1, 2)
	if err != nil {
		t.Fatalf("could not read the state back: %v", err)
	}
	if state != nil {
		t.Fatalf("the notice wrote turn state: stage=%v", state.Stage)
	}
}

// The entry, not just the state: entries.Delete(pairKey) orphaned the follow-up
// turn, so the message after it started a second concurrent pipeline.
func TestAIFailureNotice_DoesNotTouchTheEntryOfTheNextTurn(t *testing.T) {
	engine, sent := captureDispatch(t)
	svc, _ := setupSvcWithAIAndDispatch(t, &mockAIAdapter{}, engine)

	// The follow-up turn, exactly as startDebounce leaves it: an entry in the map
	// and StageDebounce in Redis, both under the pair the notice is about to use.
	key := pairKey(1, 2)
	nextTurn, cancelNextTurn := context.WithCancel(context.Background())
	defer cancelNextTurn()
	svc.entries.Store(key, pipelineEntry{ctx: nextTurn, cancel: cancelNextTurn})
	debounce := &model.PipelineState{Stage: model.StageDebounce, CreatedAt: time.Now()}
	if err := svc.repo.SetState(context.Background(), 1, 2, debounce); err != nil {
		t.Fatalf("could not seed the next turn's state: %v", err)
	}
	t.Cleanup(func() { svc.repo.ClearState(context.Background(), 1, 2) })

	svc.sendAIFailureNotice(1, 2, model.BotConfig{}, "http://crm.test/postback/2", brtErrors.ErrAITimeout)

	// Guard the guard: the bookkeeping only runs after a successful dispatch, so
	// a notice that never went out would satisfy the assertions below for free.
	if len(*sent) != 1 {
		t.Fatalf("the notice never dispatched, so this proves nothing: %v", *sent)
	}

	stored, ok := svc.entries.Load(key)
	if !ok {
		t.Fatal("the notice deleted the next turn's entry: it can no longer be cancelled, so the message after it starts a second concurrent pipeline")
	}
	if entry, _ := stored.(pipelineEntry); entry.ctx != nextTurn {
		t.Error("the next turn's entry was replaced by the notice")
	}

	// SetState(StageDone) followed by ClearState leaves nothing behind, so only a
	// seeded state can witness it: the next turn must still be in StageDebounce.
	state, err := svc.repo.GetState(context.Background(), 1, 2)
	if err != nil {
		t.Fatalf("could not read the next turn's state back: %v", err)
	}
	if state == nil {
		t.Fatal("the notice cleared the next turn's state: its debounce is lost")
	}
	if state.Stage != model.StageDebounce {
		t.Errorf("the notice stamped the next turn's state: stage=%v, want %v", state.Stage, model.StageDebounce)
	}
}

// The notice is a real dispatch (segmented, with per-rune delays), not a cleanup
// call. Bounding it with cleanupCtx's 5s truncated it and then logged
// "New message arrived" when nothing had arrived.
func TestAIFailureNotice_HasRoomForASegmentedDispatch(t *testing.T) {
	ctx, cancel := noticeCtx()
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("the notice dispatch must stay bounded")
	}
	if remaining := time.Until(deadline); remaining <= 10*time.Second {
		t.Fatalf("notice budget is %v; a segmented dispatch with per-rune delays needs more", remaining)
	}
}

// The default reaches customers of installations that never chose Portuguese.
func TestAIFailureNotice_DefaultIsLocaleNeutralEnglish(t *testing.T) {
	for _, ptBR := range []string{"instabilidade", "Já retorno", "não consegui"} {
		if strings.Contains(defaultAIFailureNotice, ptBR) {
			t.Errorf("default notice still hardcodes pt-BR (%q): %q", ptBR, defaultAIFailureNotice)
		}
	}
}
