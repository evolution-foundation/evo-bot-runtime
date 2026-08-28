package model

// A2ARequest carries the data needed to call AI Processor via JSON-RPC 2.0.
// Fields are NOT serialised directly — the adapter builds the wire format.
type A2ARequest struct {
	OutgoingURL    string         // full A2A endpoint URL from agent_bot.outgoing_url
	ContactID      int64          // used for userId in JSON-RPC params
	ConversationID int64          // used for contextId in JSON-RPC params
	ApiKey         string         // used for X-API-Key header (per-event auth)
	Message        string         // aggregated buffer content (FR-15)
	Metadata       map[string]any // CRM metadata passed through to processor (tools context)
	Attachments    []Attachment   // EVO-2180: incoming media to forward as A2A file parts
}

// Attachment is an incoming media item (image/audio/…) the adapter downloads and
// forwards to the AI Processor as a base64 A2A file part.
type Attachment struct {
	URL         string // downloadable URL (Rails proxy on BACKEND_URL, reachable server-side)
	ContentType string // e.g. "image/jpeg"
	FileType    string // CRM file_type: image/audio/video/file
}

// jsonRPCRequest is the JSON-RPC 2.0 envelope sent to AI Processor.
type JSONRPCRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      string        `json:"id"`
	Method  string        `json:"method"`
	Params  JSONRPCParams `json:"params"`
}

type JSONRPCParams struct {
	ContextID string         `json:"contextId"`
	UserID    string         `json:"userId"`
	Message   JSONRPCMessage `json:"message"`
	Metadata  map[string]any `json:"metadata"`
}

type JSONRPCMessage struct {
	Role  string        `json:"role"`
	Parts []JSONRPCPart `json:"parts"`
}

type JSONRPCPart struct {
	Type string       `json:"type"`
	Text string       `json:"text,omitempty"`
	File *JSONRPCFile `json:"file,omitempty"`
}

// JSONRPCFile is a base64 file part. Field names/tags match what the AI Processor
// reads (extract_files_from_message: name / mimeType / bytes).
type JSONRPCFile struct {
	Name     string `json:"name,omitempty"`
	MimeType string `json:"mimeType"`
	Bytes    string `json:"bytes"` // base64-encoded content
}

// A2AResponse is the JSON-RPC 2.0 response from AI Processor.
type A2AResponse struct {
	Result *A2AResult `json:"result"`
}

type A2AResult struct {
	Artifacts []A2AArtifact `json:"artifacts"`
	Message   *A2AMessage   `json:"message"`
}

type A2AArtifact struct {
	Parts []A2APart `json:"parts"`
}

type A2AMessage struct {
	Parts []A2APart `json:"parts"`
}

type A2APart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// NormalizedResponse is the internal format after parsing A2AResponse.
// No JSON tags — this type never crosses a service boundary.
type NormalizedResponse struct {
	Content string
}
