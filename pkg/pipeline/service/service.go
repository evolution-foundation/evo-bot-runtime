package service

// PipelineService is a placeholder — implemented in Story 1.2
type PipelineService interface{}

type pipelineService struct{}

// NewPipelineService creates a new pipeline service instance
func NewPipelineService() PipelineService {
	return &pipelineService{}
}
