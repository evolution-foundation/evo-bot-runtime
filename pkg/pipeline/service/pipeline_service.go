package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	brtErrors "github.com/EvolutionAPI/evo-bot-runtime/internal/errors"
	aiModel "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
	aiIface "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/service"
	debounceIface "github.com/EvolutionAPI/evo-bot-runtime/pkg/debounce/service"
	dispatchIface "github.com/EvolutionAPI/evo-bot-runtime/pkg/dispatch/service"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/repository"
)

// PipelineService orchestrates the per-pair pipeline state machine.
type PipelineService interface {
	Process(ctx context.Context, event *model.MessageEvent) error
	Cancel(contactID, conversationID int64) error
	Start() error
	// Shutdown stops the polling goroutine and cancels all in-flight pipeline
	// contexts. It blocks until the poller exits or ctx is cancelled.
	Shutdown(ctx context.Context)
}

type pipelineEntry struct {
	ctx         context.Context
	cancel      context.CancelFunc
	cfg         model.BotConfig    // carries BotConfig from MessageEvent to dispatch stage
	postbackURL string             // carries PostbackURL from MessageEvent to dispatch stage
	outgoingURL string             // carries OutgoingURL (full A2A endpoint) from MessageEvent to AI stage
	apiKey      string             // carries ApiKey from MessageEvent to AI stage
	metadata    map[string]any     // carries Metadata from MessageEvent to AI stage (for tools context)
}

type pipelineService struct {
	repo        repository.PipelineRepository
	debounce    debounceIface.DebounceEngine
	aiAdapter   aiIface.AIAdapter
	dispatchEng dispatchIface.DispatchEngine
	entries     sync.Map      // string → pipelineEntry
	stopCh      chan struct{}  // closed by Shutdown to stop pollDebounceExpiry
	stoppedCh   chan struct{}  // closed by pollDebounceExpiry when it exits
	stopOnce    sync.Once     // ensures stopCh is closed exactly once
}

// NewPipelineService constructs the service. Returns interface (GEAR R03).
func NewPipelineService(
	repo        repository.PipelineRepository,
	debounce    debounceIface.DebounceEngine,
	aiAdapter   aiIface.AIAdapter,
	dispatchEng dispatchIface.DispatchEngine,
) PipelineService {
	return &pipelineService{
		repo:        repo,
		debounce:    debounce,
		aiAdapter:   aiAdapter,
		dispatchEng: dispatchEng,
		stopCh:      make(chan struct{}),
		stoppedCh:   make(chan struct{}),
	}
}

// Start recovers in-progress debounce pairs from Redis, then launches the single
// polling goroutine that detects timer expiry (AC: #1, #4).
func (s *pipelineService) Start() error {
	ctx := context.Background()

	// Step 1: Recover state from Redis before starting ticker (NFR-01).
	const maxRecoveryPairs = 10_000
	pairs, err := s.repo.ScanStates(ctx, maxRecoveryPairs)
	if err != nil {
		return fmt.Errorf("pipeline.start.scan_states: %w", err)
	}

	for _, pair := range pairs {
		state, err := s.repo.GetState(ctx, pair.ContactID, pair.ConversationID)
		if err != nil || state == nil || state.Stage != model.StageDebounce {
			continue
		}
		// Recreate entry only if not already in memory (avoids overwriting cancel funcs
		// for pairs that received a Process call concurrently with Start recovery).
		key := pairKey(pair.ContactID, pair.ConversationID)
		if _, alreadyExists := s.entries.Load(key); !alreadyExists {
			pipelineCtx, cancel := context.WithCancel(context.Background())
			s.entries.Store(key, pipelineEntry{
				ctx:         pipelineCtx,
				cancel:      cancel,
				cfg:         state.BotConfig,
				postbackURL: state.PostbackURL,
				outgoingURL: state.OutgoingURL,
				apiKey:      state.ApiKey,
				metadata:    state.Metadata,
			})
		}

		// If timer already expired during the restart window → advance immediately.
		timerExists, err := s.repo.TimerExists(ctx, pair.ContactID, pair.ConversationID)
		if err == nil && !timerExists {
			s.advanceToAI(pair.ContactID, pair.ConversationID)
		}
	}

	// Step 2: Start single polling goroutine (AC: #1 — only timer-detection mechanism).
	go s.pollDebounceExpiry()
	return nil
}

func (s *pipelineService) Process(ctx context.Context, event *model.MessageEvent) error {
	mu, err := s.repo.AcquireLock(ctx, event.ContactID, event.ConversationID)
	if err != nil {
		return brtErrors.ErrLockFailed
	}
	defer mu.Unlock()

	state, err := s.repo.GetState(ctx, event.ContactID, event.ConversationID)
	if err != nil {
		return fmt.Errorf("pipeline.process.get_state: %w", err)
	}

	switch {
	case state == nil || state.Stage == model.StageIncoming:
		// The HTTP handler always writes StageIncoming before launching the Process
		// goroutine (NFR-01 durability). When two events for the same pair arrive
		// rapidly, the second handler call overwrites any active StageAI/StageDispatch
		// with StageIncoming before this goroutine runs — bypassing the interrupt
		// branch below. cancelPair is a no-op when no entry exists.
		s.cancelPair(event.ContactID, event.ConversationID)
		if event.BotConfig.DebounceTime == 0 {
			return s.skipDebounce(ctx, event)
		}
		return s.startDebounce(ctx, event)
	case state.Stage == model.StageDebounce:
		return s.resetDebounce(ctx, event)
	case state.Stage == model.StageAI || state.Stage == model.StageDispatch:
		s.cancelPair(event.ContactID, event.ConversationID)
		if err := s.repo.ClearState(ctx, event.ContactID, event.ConversationID); err != nil {
			return fmt.Errorf("pipeline.process.clear: %w", err)
		}
		slog.Info(fmt.Sprintf("pipeline.%s.interrupt", state.Stage),
			"contact_id", event.ContactID,
			"conversation_id", event.ConversationID,
		)
		if event.BotConfig.DebounceTime == 0 {
			return s.skipDebounce(ctx, event)
		}
		return s.startDebounce(ctx, event)
	}
	return nil
}

func (s *pipelineService) startDebounce(ctx context.Context, event *model.MessageEvent) error {
	pipelineCtx, cancel := context.WithCancel(context.Background())
	key := pairKey(event.ContactID, event.ConversationID)
	s.entries.Store(key, pipelineEntry{
		ctx:         pipelineCtx,
		cancel:      cancel,
		cfg:         event.BotConfig,
		postbackURL: event.PostbackURL,
		outgoingURL: event.OutgoingURL,
		apiKey:      event.ApiKey,
		metadata:    event.Metadata,
	})

	if err := s.debounce.Start(ctx, event.ContactID, event.ConversationID, event.MessageContent, event.BotConfig); err != nil {
		cancel()
		s.entries.Delete(key)
		return fmt.Errorf("pipeline.debounce.start: %w", err)
	}

	// Persist BotConfig, PostbackURL, AgentBotID and ApiKey alongside the stage so that
	// Start() recovery after a restart can reconstruct the pipelineEntry correctly (NFR-01).
	newState := &model.PipelineState{
		Stage:       model.StageDebounce,
		CreatedAt:   time.Now(),
		BotConfig:   event.BotConfig,
		PostbackURL: event.PostbackURL,
		OutgoingURL: event.OutgoingURL,
		ApiKey:      event.ApiKey,
		Metadata:    event.Metadata,
	}
	if err := s.repo.SetState(ctx, event.ContactID, event.ConversationID, newState); err != nil {
		cancel()
		s.entries.Delete(key)
		return fmt.Errorf("pipeline.state.set: %w", err)
	}

	slog.Info("pipeline.debounce.started",
		"contact_id", event.ContactID,
		"conversation_id", event.ConversationID,
		"debounce_ms", event.BotConfig.DebounceTime*1000,
	)
	return nil
}

// skipDebounce handles DebounceTime == 0: appends to buffer, advances directly to StageAI (FR-09).
func (s *pipelineService) skipDebounce(ctx context.Context, event *model.MessageEvent) error {
	pipelineCtx, cancel := context.WithCancel(context.Background())
	key := pairKey(event.ContactID, event.ConversationID)
	s.entries.Store(key, pipelineEntry{
		ctx:         pipelineCtx,
		cancel:      cancel,
		cfg:         event.BotConfig,
		postbackURL: event.PostbackURL,
		outgoingURL: event.OutgoingURL,
		apiKey:      event.ApiKey,
		metadata:    event.Metadata,
	})

	// debounce.Start appends to buffer; DebounceTime=0 means no timer (Story 2.1).
	if err := s.debounce.Start(ctx, event.ContactID, event.ConversationID,
		event.MessageContent, event.BotConfig); err != nil {
		cancel()
		s.entries.Delete(key)
		return fmt.Errorf("pipeline.skip_debounce.start: %w", err)
	}

	buffer, err := s.debounce.GetBuffer(ctx, event.ContactID, event.ConversationID)
	if err != nil {
		cancel()
		s.entries.Delete(key)
		return fmt.Errorf("pipeline.skip_debounce.get_buffer: %w", err)
	}

	newState := &model.PipelineState{Stage: model.StageAI, CreatedAt: time.Now()}
	if err := s.repo.SetState(ctx, event.ContactID, event.ConversationID, newState); err != nil {
		cancel()
		s.entries.Delete(key)
		return fmt.Errorf("pipeline.skip_debounce.set_state: %w", err)
	}

	slog.Info("pipeline.debounce.skipped",
		"contact_id", event.ContactID,
		"conversation_id", event.ConversationID,
	)
	s.launchAIStage(event.ContactID, event.ConversationID, buffer)
	return nil
}

func (s *pipelineService) resetDebounce(ctx context.Context, event *model.MessageEvent) error {
	if err := s.debounce.Reset(ctx, event.ContactID, event.ConversationID, event.MessageContent, event.BotConfig); err != nil {
		return fmt.Errorf("pipeline.debounce.reset: %w", err)
	}
	slog.Info("pipeline.debounce.reset",
		"contact_id", event.ContactID,
		"conversation_id", event.ConversationID,
	)
	return nil
}

// advanceToAI acquires the lock and transitions the pair from StageDebounce to StageAI.
// Double-check under lock prevents races with concurrent Process calls (NFR-02).
func (s *pipelineService) advanceToAI(contactID, conversationID int64) {
	ctx := context.Background()

	mu, err := s.repo.AcquireLock(ctx, contactID, conversationID)
	if err != nil {
		// Another instance beat us — skip (exactly-once guarantee holds).
		return
	}
	defer mu.Unlock()

	// Re-check state under lock — may have changed since we detected expiry.
	state, err := s.repo.GetState(ctx, contactID, conversationID)
	if err != nil || state == nil || state.Stage != model.StageDebounce {
		return
	}

	// Re-check timer — may have been reset by a new message arriving concurrently.
	exists, err := s.repo.TimerExists(ctx, contactID, conversationID)
	if err != nil || exists {
		return // Timer reset — let it expire again naturally.
	}

	buffer, err := s.debounce.GetBuffer(ctx, contactID, conversationID)
	if err != nil {
		slog.Error("pipeline.debounce.get_buffer_failed",
			"contact_id", contactID,
			"conversation_id", conversationID,
			"error", err,
		)
		return
	}

	newState := &model.PipelineState{Stage: model.StageAI, CreatedAt: time.Now()}
	if err := s.repo.SetState(ctx, contactID, conversationID, newState); err != nil {
		slog.Error("pipeline.debounce.set_ai_state_failed",
			"contact_id", contactID,
			"conversation_id", conversationID,
			"error", err,
		)
		return
	}

	slog.Info("pipeline.debounce.expired",
		"contact_id", contactID,
		"conversation_id", conversationID,
		"buffer_len", len(buffer),
	)
	s.launchAIStage(contactID, conversationID, buffer)
}

// launchAIStage launches the AI goroutine with the stored pipeline context.
// Must be called only after pipelineEntry is stored in s.entries (guaranteed by
// startDebounce/skipDebounce/advanceToAI).
func (s *pipelineService) launchAIStage(contactID, conversationID int64, buffer string) {
	key := pairKey(contactID, conversationID)
	v, ok := s.entries.Load(key)
	if !ok {
		// Should not happen — entry stored before this is called.
		slog.Error("pipeline.ai.no_entry",
			"contact_id", contactID,
			"conversation_id", conversationID,
		)
		return
	}
	entry, ok := v.(pipelineEntry)
	if !ok {
		slog.Error("pipeline.ai.entry_type_mismatch",
			"contact_id", contactID,
			"conversation_id", conversationID,
		)
		return
	}
	go s.runAIStage(entry.ctx, contactID, conversationID, buffer, entry.cfg, entry.postbackURL)
}

// runAIStage is the AI stage goroutine body. ctx is pipelineEntry.ctx — cancelled by
// Process when a new message arrives for the same pair.
func (s *pipelineService) runAIStage(ctx context.Context, contactID, conversationID int64, buffer string, cfg model.BotConfig, postbackURL string) {
	defer s.recoverPipeline(contactID, conversationID)

	slog.Info("pipeline.ai.started",
		"contact_id", contactID,
		"conversation_id", conversationID,
	)
	start := time.Now()

	// Retrieve outgoing_url, api_key and metadata from the pipeline entry.
	key := pairKey(contactID, conversationID)
	var outgoingURL, apiKey string
	var metadata map[string]any
	if v, ok := s.entries.Load(key); ok {
		if entry, ok := v.(pipelineEntry); ok {
			outgoingURL = entry.outgoingURL
			apiKey = entry.apiKey
			metadata = entry.metadata
		}
	}

	resp, err := s.aiAdapter.Call(ctx, &aiModel.A2ARequest{
		OutgoingURL:    outgoingURL,
		ContactID:      contactID,
		ConversationID: conversationID,
		ApiKey:         apiKey,
		Message:        buffer,
		Metadata:       metadata,
	})
	if err != nil {
		switch {
		case errors.Is(err, brtErrors.ErrPipelineCancelled):
			// Expected: new message arrived, Process already set StageDebounce.
			// Do NOT call ClearState — would destroy the new active state.
			slog.Info("pipeline.ai.cancelled",
				"contact_id", contactID,
				"conversation_id", conversationID,
			)
		case errors.Is(err, brtErrors.ErrAITimeout):
			// AI backend did not respond in time — clear state for fresh start.
			slog.Warn("pipeline.ai.timeout",
				"contact_id", contactID,
				"conversation_id", conversationID,
			)
			s.clearStateWithLog(contactID, conversationID)
		default:
			slog.Error("pipeline.ai.error",
				"contact_id", contactID,
				"conversation_id", conversationID,
				"error", fmt.Errorf("pipeline.ai: %w", err),
			)
			s.clearStateWithLog(contactID, conversationID)
		}
		return
	}

	// Success path — use cleanupCtx() for Redis calls: pipeline ctx may be cancelled.
	dur := time.Since(start)

	newState := &model.PipelineState{Stage: model.StageDispatch, CreatedAt: time.Now()}
	setCtx, setCancel := cleanupCtx()
	setErr := s.repo.SetState(setCtx, contactID, conversationID, newState)
	setCancel()
	if setErr != nil {
		slog.Error("pipeline.ai.set_dispatch_failed",
			"contact_id", contactID,
			"conversation_id", conversationID,
			"error", setErr,
		)
		s.clearStateWithLog(contactID, conversationID)
		return
	}

	slog.Info("pipeline.ai.completed",
		"contact_id", contactID,
		"conversation_id", conversationID,
		"duration", dur.String(),
	)
	s.launchDispatchStage(ctx, contactID, conversationID, resp, cfg, postbackURL)
}

// launchDispatchStage launches the dispatch goroutine.
func (s *pipelineService) launchDispatchStage(
	ctx            context.Context,
	contactID      int64,
	conversationID int64,
	resp           *aiModel.NormalizedResponse,
	cfg            model.BotConfig,
	postbackURL    string,
) {
	go s.runDispatchStage(ctx, contactID, conversationID, resp.Content, cfg, postbackURL)
}

// runDispatchStage is the dispatch stage goroutine body. ctx is pipelineEntry.ctx — cancelled by
// Process when a new message arrives for the same pair.
func (s *pipelineService) runDispatchStage(
	ctx            context.Context,
	contactID      int64,
	conversationID int64,
	content        string,
	cfg            model.BotConfig,
	postbackURL    string,
) {
	defer s.recoverPipeline(contactID, conversationID)

	slog.Info("pipeline.dispatch.started",
		"contact_id",      contactID,
		"conversation_id", conversationID,
	)
	start := time.Now()

	err := s.dispatchEng.Dispatch(ctx, contactID, conversationID, content, cfg, postbackURL)
	if err != nil {
		switch {
		case errors.Is(err, brtErrors.ErrDispatchInterrupted):
			// New message arrived — Process already set StageDebounce.
			// Do NOT call ClearState: would destroy the new active state.
			// Do NOT call entries.Delete: cancelPair already did LoadAndDelete
			// atomically. A Delete here would race with the new event's Store
			// and could delete the replacement entry.
			slog.Info("pipeline.dispatch.cancelled",
				"contact_id",      contactID,
				"conversation_id", conversationID,
			)
		default:
			slog.Error("pipeline.dispatch.error",
				"contact_id",      contactID,
				"conversation_id", conversationID,
				"error",           err,
			)
			s.clearStateWithLog(contactID, conversationID)
		}
		return
	}

	// Success: SetState(StageDone) → ClearState → entries.Delete → log
	dur := time.Since(start)
	doneState := &model.PipelineState{Stage: model.StageDone, CreatedAt: time.Now()}
	doneCtx, doneCancel := cleanupCtx()
	if err := s.repo.SetState(doneCtx, contactID, conversationID, doneState); err != nil {
		slog.Warn("pipeline.dispatch.set_done_failed",
			"contact_id",      contactID,
			"conversation_id", conversationID,
			"error",           err,
		)
	}
	doneCancel()
	s.clearStateWithLog(contactID, conversationID)
	s.entries.Delete(pairKey(contactID, conversationID))

	slog.Info("pipeline.dispatch.completed",
		"contact_id",      contactID,
		"conversation_id", conversationID,
		"duration",        dur.String(),
	)
}

// pollDebounceExpiry is the single timer-detection mechanism (AC: #1).
// It runs a 100ms ticker and advances expired StageDebounce pairs to StageAI.
// It exits when s.stopCh is closed and signals s.stoppedCh before returning.
func (s *pipelineService) pollDebounceExpiry() {
	defer close(s.stoppedCh)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
		}
		s.entries.Range(func(k, v any) bool {
			entry, ok := v.(pipelineEntry)
			if !ok {
				return true
			}
			key, ok := k.(string)
			if !ok {
				return true
			}

			// Skip cancelled pipelines.
			select {
			case <-entry.ctx.Done():
				return true
			default:
			}

			contactID, conversationID := parsePairKey(key)
			if contactID == 0 {
				return true
			}

			// Only process StageDebounce pairs.
			state, err := s.repo.GetState(entry.ctx, contactID, conversationID)
			if err != nil || state == nil || state.Stage != model.StageDebounce {
				return true
			}

			// Check if timer expired.
			exists, err := s.repo.TimerExists(entry.ctx, contactID, conversationID)
			if err != nil || exists {
				return true // Still running.
			}

			// Timer expired — advance to AI (acquires lock internally).
			s.advanceToAI(contactID, conversationID)
			return true
		})
	}
}

func (s *pipelineService) cancelPair(contactID, conversationID int64) {
	key := pairKey(contactID, conversationID)
	if v, ok := s.entries.LoadAndDelete(key); ok {
		if entry, ok := v.(pipelineEntry); ok {
			entry.cancel()
		}
	}
}

func (s *pipelineService) Cancel(contactID, conversationID int64) error {
	s.cancelPair(contactID, conversationID)
	s.clearStateWithLog(contactID, conversationID)
	return nil
}

// Shutdown stops the polling goroutine and cancels all in-flight pipeline
// contexts. It blocks until the poller exits or ctx is cancelled.
// Safe to call multiple times — subsequent calls are no-ops for the stop signal
// but still wait for the poller to finish if it hasn't already.
func (s *pipelineService) Shutdown(ctx context.Context) {
	// Cancel all in-flight pipeline goroutines.
	s.entries.Range(func(k, v any) bool {
		if entry, ok := v.(pipelineEntry); ok {
			entry.cancel()
		}
		return true
	})

	// Signal poller to stop exactly once — close on a closed channel panics.
	s.stopOnce.Do(func() { close(s.stopCh) })

	select {
	case <-s.stoppedCh:
		slog.Info("pipeline.shutdown.complete")
	case <-ctx.Done():
		slog.Warn("pipeline.shutdown.timeout")
	}
}

// recoverPipeline is deferred in every pipeline goroutine to handle panics safely.
func (s *pipelineService) recoverPipeline(contactID, conversationID int64) {
	if r := recover(); r != nil {
		slog.Error("pipeline.panic.recovered",
			"contact_id", contactID,
			"conversation_id", conversationID,
			"panic", r,
			"stack", string(debug.Stack()),
		)
		s.clearStateWithLog(contactID, conversationID)
		s.entries.Delete(pairKey(contactID, conversationID))
	}
}

// cleanupCtx returns a context with a 5-second timeout for best-effort cleanup
// calls (ClearState, SetState) that run after the pipeline context is cancelled
// or after a failure. Prevents these calls from hanging indefinitely.
func cleanupCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// clearStateWithLog calls ClearState and logs a warning if it fails.
// Used in all goroutine error/cleanup paths where the error is non-actionable
// but should not be silently swallowed.
func (s *pipelineService) clearStateWithLog(contactID, conversationID int64) {
	ctx, cancel := cleanupCtx()
	defer cancel()
	if err := s.repo.ClearState(ctx, contactID, conversationID); err != nil {
		slog.Warn("pipeline.cleanup.clear_state_failed",
			"contact_id", contactID,
			"conversation_id", conversationID,
			"error", err,
		)
	}
}

func pairKey(contactID, conversationID int64) string {
	return fmt.Sprintf("%d:%d", contactID, conversationID)
}

// parsePairKey is the inverse of pairKey.
func parsePairKey(key string) (contactID, conversationID int64) {
	parts := strings.SplitN(key, ":", 2)
	if len(parts) != 2 {
		return 0, 0
	}
	contactID, _ = strconv.ParseInt(parts[0], 10, 64)
	conversationID, _ = strconv.ParseInt(parts[1], 10, 64)
	return
}
