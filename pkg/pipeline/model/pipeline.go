package model

import (
	"errors"
	"time"
)

// Stage — string type; never use iota or inline strings
type Stage string

const (
	StageIncoming Stage = "incoming" // written by handler before 202; ensures NFR-01 durability
	StageDebounce Stage = "debounce"
	StageAI       Stage = "ai"
	StageDispatch Stage = "dispatch"
	StageDone     Stage = "done"
)

// PairID identifies a contact+conversation pair for recovery and scanning.
type PairID struct {
	ContactID      int64
	ConversationID int64
}

// PipelineState is what is stored in Redis (JSON-serializable).
// The cancel func is NOT stored here — it lives in PipelineService memory (Story 2.2).
// BotConfig and PostbackURL are persisted for StageDebounce so that the service can
// reconstruct the pipelineEntry correctly after a restart (NFR-01 recovery).
type PipelineState struct {
	Stage        Stage     `json:"stage"`
	CreatedAt    time.Time `json:"created_at"`
	BotConfig    BotConfig `json:"bot_config,omitempty"`
	PostbackURL  string    `json:"postback_url,omitempty"`
	AgentBotID   string    `json:"agent_bot_id,omitempty"`
	ApiKey       string    `json:"api_key,omitempty"`
	OutgoingURL  string         `json:"outgoing_url,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
}

// MessageEvent is the inbound payload from evo-ai-crm AgentBotListener.
// All JSON tags are snake_case — matches the wire format exactly.
type MessageEvent struct {
	AgentBotID     string         `json:"agent_bot_id"`
	ConversationID int64          `json:"conversation_id"`
	ContactID      int64          `json:"contact_id"`
	MessageID      string         `json:"message_id"`
	MessageContent string         `json:"message_content"`
	ApiKey         string         `json:"api_key"`
	OutgoingURL    string         `json:"outgoing_url"`
	BotConfig      BotConfig      `json:"bot_config"`
	PostbackURL    string         `json:"postback_url"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

// BotConfig carries per-bot runtime configuration provided by the caller.
// Bot Runtime must not make any outbound call to fetch config (FR-24).
type BotConfig struct {
	DebounceTime            int     `json:"debounce_time"`              // seconds; 0 = pass-through
	MessageSignature        string  `json:"message_signature"`
	TextSegmentationEnabled bool    `json:"text_segmentation_enabled"`
	TextSegmentationLimit   int     `json:"text_segmentation_limit"`    // max chars per segment
	TextSegmentationMinSize int     `json:"text_segmentation_min_size"`
	DelayPerCharacter       float64 `json:"delay_per_character"`        // ms per char between parts
}

// Validate checks semantic constraints on a MessageEvent after JSON binding.
func (e *MessageEvent) Validate() error {
	if e.ContactID <= 0 {
		return errors.New("contact_id must be > 0")
	}
	if e.ConversationID <= 0 {
		return errors.New("conversation_id must be > 0")
	}
	if e.PostbackURL == "" {
		return errors.New("postback_url is required")
	}
	if e.BotConfig.DebounceTime < 0 {
		return errors.New("debounce_time must be >= 0")
	}
	if e.BotConfig.TextSegmentationEnabled && e.BotConfig.TextSegmentationLimit <= 0 {
		return errors.New("text_segmentation_limit must be > 0 when segmentation is enabled")
	}
	if e.BotConfig.DelayPerCharacter < 0 {
		return errors.New("delay_per_character must be >= 0")
	}
	return nil
}
