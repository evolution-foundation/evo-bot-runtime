package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/go-redsync/redsync/v4"
	goredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"

	"github.com/EvolutionAPI/evo-bot-runtime/internal/config"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/handler"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/repository"
)

func main() {
	// Step 1: config
	cfg := config.Load()

	// Step 2: Redis client + connectivity check
	opt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		slog.Error("invalid REDIS_URL", "error", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(opt)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		slog.Error("redis connection failed", "error", err)
		os.Exit(1)
	}

	// Step 3: redsync
	pool := goredis.NewPool(rdb)
	rs := redsync.New(pool)

	// Step 4: repository
	pipelineRepo := repository.NewPipelineRepository(rdb, rs)

	// Step 5: handler + routes
	hdl := handler.NewHandler(pipelineRepo, cfg.BotRuntimeSecret)
	r := gin.New()
	r.Use(gin.Recovery())
	hdl.RegisterRoutes(r)

	// Step 6: start server
	slog.Info("evo-bot-runtime starting", "listen_addr", cfg.ListenAddr)
	if err := r.Run(cfg.ListenAddr); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}
