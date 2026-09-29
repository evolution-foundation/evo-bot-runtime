package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/repository"
)

// DebounceEngine manages per-pair debounce timers and message buffers.
type DebounceEngine interface {
	Start(ctx context.Context, contactID, conversationID int64, content string, atts []model.Attachment, cfg model.BotConfig) error
	Reset(ctx context.Context, contactID, conversationID int64, content string, atts []model.Attachment, cfg model.BotConfig) error
	// RenewTimer extends the debounce timer TTL without touching the buffer or
	// attachments — used when a presence ("customer is typing") signal arrives
	// mid-debounce, as opposed to Reset which is for an actual new message.
	RenewTimer(ctx context.Context, contactID, conversationID int64, cfg model.BotConfig) error
	GetBuffer(ctx context.Context, contactID, conversationID int64) (string, error)
	GetAttachments(ctx context.Context, contactID, conversationID int64) ([]model.Attachment, error)
	TimerExists(ctx context.Context, contactID, conversationID int64) (bool, error)
}

type debounceEngine struct {
	repo repository.PipelineRepository
}

// NewDebounceEngine constructs the engine. Returns interface (GEAR R03).
func NewDebounceEngine(repo repository.PipelineRepository) DebounceEngine {
	return &debounceEngine{repo: repo}
}

func (d *debounceEngine) Start(ctx context.Context, contactID, conversationID int64, content string, atts []model.Attachment, cfg model.BotConfig) error {
	if err := d.repo.AppendToBuffer(ctx, contactID, conversationID, content); err != nil {
		return fmt.Errorf("debounce.start.append: %w", err)
	}
	if err := d.repo.AppendAttachments(ctx, contactID, conversationID, atts); err != nil {
		return fmt.Errorf("debounce.start.append_attachments: %w", err)
	}
	return d.setTimerIfConfigured(ctx, contactID, conversationID, cfg, "start")
}

func (d *debounceEngine) Reset(ctx context.Context, contactID, conversationID int64, content string, atts []model.Attachment, cfg model.BotConfig) error {
	if err := d.repo.AppendToBuffer(ctx, contactID, conversationID, content); err != nil {
		return fmt.Errorf("debounce.reset.append: %w", err)
	}
	if err := d.repo.AppendAttachments(ctx, contactID, conversationID, atts); err != nil {
		return fmt.Errorf("debounce.reset.append_attachments: %w", err)
	}
	return d.setTimerIfConfigured(ctx, contactID, conversationID, cfg, "reset")
}

// RenewTimer extends an already-running timer only — unlike Start/Reset, it
// must never create one. A presence pulse landing in the ~100ms window after
// pollDebounceExpiry sees the key gone (but before it advances the pair to
// StageAI) must not resurrect that window and indefinitely postpone the reply
// (Sourcery finding on evolution-foundation/evo-bot-runtime#12).
func (d *debounceEngine) RenewTimer(ctx context.Context, contactID, conversationID int64, cfg model.BotConfig) error {
	ttl, ok := d.timerTTLIfConfigured(cfg)
	if !ok {
		return nil
	}
	if _, err := d.repo.RenewTimerIfExists(ctx, contactID, conversationID, ttl); err != nil {
		return fmt.Errorf("debounce.renew_timer.renew_timer_if_exists: %w", err)
	}
	return nil
}

// setTimerIfConfigured is the timer-setting logic shared by Start, Reset and
// RenewTimer — a no-op when DebounceTime <= 0 (pass-through mode).
func (d *debounceEngine) setTimerIfConfigured(ctx context.Context, contactID, conversationID int64, cfg model.BotConfig, op string) error {
	ttl, ok := d.timerTTLIfConfigured(cfg)
	if !ok {
		return nil
	}
	if err := d.repo.SetTimer(ctx, contactID, conversationID, ttl); err != nil {
		return fmt.Errorf("debounce.%s.set_timer: %w", op, err)
	}
	return nil
}

// timerTTLIfConfigured is the DebounceTime<=0 pass-through check shared by
// every timer-setting path (Start, Reset, RenewTimer) — ok is false when
// there's no timer to set/renew at all.
func (d *debounceEngine) timerTTLIfConfigured(cfg model.BotConfig) (ttl time.Duration, ok bool) {
	if cfg.DebounceTime <= 0 {
		return 0, false
	}
	return time.Duration(cfg.DebounceTime) * time.Second, true
}

func (d *debounceEngine) GetBuffer(ctx context.Context, contactID, conversationID int64) (string, error) {
	entries, err := d.repo.GetBuffer(ctx, contactID, conversationID)
	if err != nil {
		return "", fmt.Errorf("debounce.get_buffer: %w", err)
	}
	return strings.Join(entries, "\n\n"), nil
}

func (d *debounceEngine) GetAttachments(ctx context.Context, contactID, conversationID int64) ([]model.Attachment, error) {
	return d.repo.GetAttachments(ctx, contactID, conversationID)
}

func (d *debounceEngine) TimerExists(ctx context.Context, contactID, conversationID int64) (bool, error) {
	return d.repo.TimerExists(ctx, contactID, conversationID)
}
