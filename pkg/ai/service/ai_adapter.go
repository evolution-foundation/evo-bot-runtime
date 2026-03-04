package service

// AIAdapter is a placeholder — implemented in Story 3.1
type AIAdapter interface{}

type aiAdapter struct{}

// NewAIAdapter creates a new AI adapter instance
func NewAIAdapter() AIAdapter {
	return &aiAdapter{}
}
