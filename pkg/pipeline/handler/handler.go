package handler

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/repository"
	pipelineService "github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/service"
)

type Handler struct {
	repo   repository.PipelineRepository
	svc    pipelineService.PipelineService
	secret string
}

func NewHandler(repo repository.PipelineRepository, svc pipelineService.PipelineService, secret string) *Handler {
	return &Handler{repo: repo, svc: svc, secret: secret}
}

func (h *Handler) RegisterRoutes(r *gin.Engine) {
	r.POST("/events", SecretMiddleware(h.secret), h.handleEvent)
	r.POST("/events/presence", SecretMiddleware(h.secret), h.handlePresence)
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

// validatable is any inbound event with post-JSON-bind semantic checks.
type validatable interface {
	Validate() error
}

// bindAndValidate binds the request body into event and runs its Validate.
// On failure it writes the 400 response itself and returns false — the
// caller returns immediately without doing any further work.
func bindAndValidate[T validatable](c *gin.Context, event T) bool {
	if err := c.ShouldBindJSON(event); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid payload",
			"code":  "ERR_INVALID_EVENT",
		})
		return false
	}
	if err := event.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
			"code":  "ERR_INVALID_EVENT",
		})
		return false
	}
	return true
}

func (h *Handler) handleEvent(c *gin.Context) {
	var event model.MessageEvent
	if !bindAndValidate(c, &event) {
		return
	}

	// Persist StageIncoming BEFORE returning 202 (NFR-01).
	// Guarantees event durability if the process goroutine never runs.
	incomingState := &model.PipelineState{Stage: model.StageIncoming, CreatedAt: time.Now()}
	if err := h.repo.SetState(c.Request.Context(), event.ContactID, event.ConversationID, incomingState); err != nil {
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

	go h.runProcess(event)

	c.JSON(http.StatusAccepted, gin.H{"status": "accepted"})
}

// handlePresence renews an in-flight debounce timer when the customer is
// composing/recording. Unlike handleEvent, it does NOT persist StageIncoming
// before processing — a presence pulse must never overwrite an active
// StageAI/StageDispatch state, only ever renew StageDebounce. It always
// responds 202 on a structurally valid payload: "accepted and renewed" and
// "accepted and no-op" are not distinguished to the caller.
func (h *Handler) handlePresence(c *gin.Context) {
	var event model.PresenceEvent
	if !bindAndValidate(c, &event) {
		return
	}

	if err := h.svc.Presence(c.Request.Context(), event.ContactID, event.ConversationID); err != nil {
		slog.Warn("pipeline.presence.failed",
			"contact_id", event.ContactID,
			"conversation_id", event.ConversationID,
			"error", err,
		)
	}

	c.JSON(http.StatusAccepted, gin.H{"status": "accepted"})
}

func (h *Handler) runProcess(event model.MessageEvent) {
	if err := h.svc.Process(context.Background(), &event); err != nil {
		slog.Error("pipeline.process.failed",
			"contact_id", event.ContactID,
			"conversation_id", event.ConversationID,
			"error", err,
		)
	}
}
