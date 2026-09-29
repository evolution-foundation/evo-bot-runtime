package model_test

import (
	"testing"

	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
)

func TestPresenceEventValidate_ValidEvent(t *testing.T) {
	e := model.PresenceEvent{ContactID: 1, ConversationID: 1}
	if err := e.Validate(); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestPresenceEventValidate_RejectsNonPositiveContactID(t *testing.T) {
	e := model.PresenceEvent{ContactID: 0, ConversationID: 1}
	if err := e.Validate(); err == nil {
		t.Error("expected error for contact_id <= 0, got nil")
	}
}

func TestPresenceEventValidate_RejectsNonPositiveConversationID(t *testing.T) {
	e := model.PresenceEvent{ContactID: 1, ConversationID: 0}
	if err := e.Validate(); err == nil {
		t.Error("expected error for conversation_id <= 0, got nil")
	}
}
