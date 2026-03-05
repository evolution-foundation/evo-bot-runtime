package model

// A2ARequest is the envelope sent to AI Processor via HTTP POST.
// ContactID and ConversationID are internal tracking fields (json:"-") used for
// structured logging; they are never serialised into the HTTP payload.
type A2ARequest struct {
	Message        string `json:"message"` // aggregated buffer content (FR-15)
	ContactID      int64  `json:"-"`
	ConversationID int64  `json:"-"`
}

// A2AResponse is the raw JSON response from AI Processor.
type A2AResponse struct {
	Content string `json:"content"`
}

// NormalizedResponse is the internal format after parsing A2AResponse.
// No JSON tags — this type never crosses a service boundary.
type NormalizedResponse struct {
	Content string
}
