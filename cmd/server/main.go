package main

import (
	"context"
	"log"

	"github.com/gin-gonic/gin"
	"github.com/go-redsync/redsync/v4"
	goredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"

	"github.com/EvolutionAPI/evo-bot-runtime/internal/config"
	aiService "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/service"
	debounceService "github.com/EvolutionAPI/evo-bot-runtime/pkg/debounce/service"
	dispatchService "github.com/EvolutionAPI/evo-bot-runtime/pkg/dispatch/service"
	pipelineHandler "github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/handler"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/repository"
	pipelineService "github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/service"
)

func main() {
	// Step 1: config
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// Step 2: Redis client + connectivity check
	opt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		log.Fatalf("invalid REDIS_URL: %v", err)
	}
	rdb := redis.NewClient(opt)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis connection failed: %v", err)
	}

	// Step 3: redsync
	pool := goredis.NewPool(rdb)
	rs := redsync.New(pool)

	// Step 4: repository
	pipelineRepo := repository.NewPipelineRepository(rdb, rs)

	// Step 5: debounce engine
	debounce := debounceService.NewDebounceEngine(pipelineRepo)

	// Step 6: AI adapter
	aiAdapter := aiService.NewAIAdapter(cfg.AIProcessorURL, cfg.AIProcessorAPIKey, cfg.AICallTimeoutSeconds)

	// Step 7: dispatch engine
	dispatch := dispatchService.NewDispatchEngine()

	// Step 8: pipeline service
	pipeline := pipelineService.NewPipelineService(pipelineRepo, debounce, aiAdapter, dispatch)
	if err := pipeline.Start(); err != nil {
		log.Fatalf("pipeline service failed to start: %v", err)
	}

	// Step 9: handler + routes
	handler := pipelineHandler.NewHandler(pipelineRepo, pipeline, cfg.BotRuntimeSecret)
	r := gin.New()
	r.Use(gin.Recovery())
	handler.RegisterRoutes(r)

	// Step 10: start server
	log.Printf("evo-bot-runtime starting on %s", cfg.ListenAddr)
	if err := r.Run(cfg.ListenAddr); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
