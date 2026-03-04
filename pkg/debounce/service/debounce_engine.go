package service

// DebounceEngine is a placeholder — implemented in Story 2.1
type DebounceEngine interface{}

type debounceEngine struct{}

// NewDebounceEngine creates a new debounce engine instance
func NewDebounceEngine() DebounceEngine {
	return &debounceEngine{}
}
