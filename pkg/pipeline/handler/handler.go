package handler

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/repository"
)

type Handler struct {
	repo   repository.PipelineRepository
	secret string
}

func NewHandler(repo repository.PipelineRepository, secret string) *Handler {
	return &Handler{repo: repo, secret: secret}
}

func (h *Handler) RegisterRoutes(r *gin.Engine) {
	r.POST("/events", SecretMiddleware(h.secret), h.handleEvent)
	r.GET("/health", h.handleHealth)
}

func (h *Handler) handleHealth(c *gin.Context) {
	if err := h.repo.Ping(c.Request.Context()); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "error",
			"detail": "redis unreachable",
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handler) handleEvent(c *gin.Context) {
	var event model.MessageEvent
	if err := c.ShouldBindJSON(&event); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid payload",
			"code":  "ERR_INVALID_EVENT",
		})
		return
	}

	// Persist initial state BEFORE returning 202 (NFR-01)
	initialState := &model.PipelineState{
		Stage:     model.StageDebounce,
		CreatedAt: time.Now(),
	}
	if err := h.repo.SetState(c.Request.Context(), event.ContactID, event.ConversationID, initialState); err != nil {
		slog.Error("pipeline.event.state_persist_failed",
			"contact_id", event.ContactID,
			"conversation_id", event.ConversationID,
			"error", err,
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "internal error",
			"code":  "ERR_INTERNAL",
		})
		return
	}

	// Launch pipeline processing — stub goroutine (replaced in Story 2.2)
	go h.processPipelineStub(context.Background(), event)

	c.JSON(http.StatusAccepted, gin.H{"status": "accepted"})
}

// processPipelineStub is the placeholder pipeline goroutine.
// Story 2.2 replaces this with: go pipelineSvc.Process(ctx, event)
// context.Background() is used here because the goroutine outlives the HTTP request context.
func (h *Handler) processPipelineStub(_ context.Context, event model.MessageEvent) {
	slog.Info("pipeline.event.received",
		"contact_id", event.ContactID,
		"conversation_id", event.ConversationID,
	)
}
