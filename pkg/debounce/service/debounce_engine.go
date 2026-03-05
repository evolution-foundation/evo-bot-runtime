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
	Start(ctx context.Context, contactID, conversationID int64, content string, cfg model.BotConfig) error
	Reset(ctx context.Context, contactID, conversationID int64, content string, cfg model.BotConfig) error
	GetBuffer(ctx context.Context, contactID, conversationID int64) (string, error)
	TimerExists(ctx context.Context, contactID, conversationID int64) (bool, error)
}

type debounceEngine struct {
	repo repository.PipelineRepository
}

// NewDebounceEngine constructs the engine. Returns interface (GEAR R03).
func NewDebounceEngine(repo repository.PipelineRepository) DebounceEngine {
	return &debounceEngine{repo: repo}
}

func (d *debounceEngine) Start(ctx context.Context, contactID, conversationID int64, content string, cfg model.BotConfig) error {
	if err := d.repo.AppendToBuffer(ctx, contactID, conversationID, content); err != nil {
		return fmt.Errorf("debounce.start.append: %w", err)
	}
	if cfg.DebounceTime > 0 {
		ttl := time.Duration(cfg.DebounceTime) * time.Second
		if err := d.repo.SetTimer(ctx, contactID, conversationID, ttl); err != nil {
			return fmt.Errorf("debounce.start.set_timer: %w", err)
		}
	}
	return nil
}

func (d *debounceEngine) Reset(ctx context.Context, contactID, conversationID int64, content string, cfg model.BotConfig) error {
	if err := d.repo.AppendToBuffer(ctx, contactID, conversationID, content); err != nil {
		return fmt.Errorf("debounce.reset.append: %w", err)
	}
	if cfg.DebounceTime > 0 {
		ttl := time.Duration(cfg.DebounceTime) * time.Second
		if err := d.repo.SetTimer(ctx, contactID, conversationID, ttl); err != nil {
			return fmt.Errorf("debounce.reset.set_timer: %w", err)
		}
	}
	return nil
}

func (d *debounceEngine) GetBuffer(ctx context.Context, contactID, conversationID int64) (string, error) {
	entries, err := d.repo.GetBuffer(ctx, contactID, conversationID)
	if err != nil {
		return "", fmt.Errorf("debounce.get_buffer: %w", err)
	}
	return strings.Join(entries, "\n\n"), nil
}

func (d *debounceEngine) TimerExists(ctx context.Context, contactID, conversationID int64) (bool, error) {
	return d.repo.TimerExists(ctx, contactID, conversationID)
}
