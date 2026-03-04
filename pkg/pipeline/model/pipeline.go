package model

import "time"

// Stage — string type; never use iota or inline strings
type Stage string

const (
	StageDebounce Stage = "debounce"
	StageAI       Stage = "ai"
	StageDispatch Stage = "dispatch"
	StageDone     Stage = "done"
)

// PipelineState is what is stored in Redis (JSON-serializable).
// The cancel func is NOT stored here — it lives in PipelineService memory (Story 2.2).
type PipelineState struct {
	Stage     Stage     `json:"stage"`
	CreatedAt time.Time `json:"created_at"`
}

// MessageEvent is the inbound payload from evo-ai-crm AgentBotListener.
// All JSON tags are snake_case — matches the wire format exactly.
type MessageEvent struct {
	AgentBotID     string    `json:"agent_bot_id"`
	ConversationID int64     `json:"conversation_id"`
	ContactID      int64     `json:"contact_id"`
	MessageID      int64     `json:"message_id"`
	MessageContent string    `json:"message_content"`
	BotConfig      BotConfig `json:"bot_config"`
	PostbackURL    string    `json:"postback_url"`
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
