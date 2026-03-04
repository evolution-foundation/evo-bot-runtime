package service

// DispatchEngine is a placeholder — implemented in Story 4.1
type DispatchEngine interface{}

type dispatchEngine struct{}

// NewDispatchEngine creates a new dispatch engine instance
func NewDispatchEngine() DispatchEngine {
	return &dispatchEngine{}
}
