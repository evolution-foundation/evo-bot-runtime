package service_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-redsync/redsync/v4"
	goredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"

	"github.com/EvolutionAPI/evo-bot-runtime/internal/testhelpers"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/debounce/service"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/repository"
)

func TestMain(m *testing.M) {
	rdb := redis.NewClient(testhelpers.RedisOptions())
	rdb.FlushDB(context.Background())
	rdb.Close()
	os.Exit(m.Run())
}

func newTestRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	return redis.NewClient(testhelpers.RedisOptions())
}

func setupEngine(t *testing.T) (service.DebounceEngine, *redis.Client) {
	t.Helper()
	rdb := newTestRedisClient(t)
	pool := goredis.NewPool(rdb)
	rs := redsync.New(pool)
	repo := repository.NewPipelineRepository(rdb, rs)
	eng := service.NewDebounceEngine(repo)
	t.Cleanup(func() {
		if os.Getenv("TEST_REDIS_FLUSH") == "1" {
			rdb.FlushDB(context.Background())
		}
		rdb.Close()
	})
	return eng, rdb
}

func TestStart_SetsTTLAndBuffer(t *testing.T) {
	eng, rdb := setupEngine(t)
	ctx := context.Background()
	cfg := model.BotConfig{DebounceTime: 5}

	if err := eng.Start(ctx, 1, 1, "first message", cfg); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	ttl := rdb.TTL(ctx, "bot_runtime:timer:1:1").Val()
	if ttl <= 4*time.Second || ttl > 5*time.Second {
		t.Errorf("timer TTL should be ~5s, got %v", ttl)
	}

	entries := rdb.LRange(ctx, "bot_runtime:buffer:1:1", 0, -1).Val()
	if len(entries) != 1 || entries[0] != "first message" {
		t.Errorf("buffer = %v, want [\"first message\"]", entries)
	}
}

func TestStart_ZeroDuration_SetsNoTimer(t *testing.T) {
	eng, rdb := setupEngine(t)
	ctx := context.Background()
	cfg := model.BotConfig{DebounceTime: 0}

	if err := eng.Start(ctx, 2, 2, "immediate", cfg); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	exists := rdb.Exists(ctx, "bot_runtime:timer:2:2").Val()
	if exists != 0 {
		t.Errorf("timer must not be set for DebounceTime=0, got exists=%d", exists)
	}
}

func TestReset_RefreshesTTLAndAppendsBuffer(t *testing.T) {
	eng, rdb := setupEngine(t)
	ctx := context.Background()
	cfg := model.BotConfig{DebounceTime: 10}

	if err := eng.Start(ctx, 3, 3, "msg1", cfg); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	// Simulate time passing by setting a shorter TTL manually
	rdb.Expire(ctx, "bot_runtime:timer:3:3", 2*time.Second)

	if err := eng.Reset(ctx, 3, 3, "msg2", cfg); err != nil {
		t.Fatalf("Reset returned error: %v", err)
	}

	ttl := rdb.TTL(ctx, "bot_runtime:timer:3:3").Val()
	if ttl <= 9*time.Second {
		t.Errorf("Reset must restore full TTL, got %v", ttl)
	}

	entries := rdb.LRange(ctx, "bot_runtime:buffer:3:3", 0, -1).Val()
	if len(entries) != 2 || entries[0] != "msg1" || entries[1] != "msg2" {
		t.Errorf("buffer = %v, want [msg1, msg2]", entries)
	}
}

func TestGetBuffer_ConcatenatesWithDoubleNewline(t *testing.T) {
	eng, _ := setupEngine(t)
	ctx := context.Background()
	cfg := model.BotConfig{DebounceTime: 5}

	_ = eng.Start(ctx, 4, 4, "hello", cfg)
	_ = eng.Reset(ctx, 4, 4, "world", cfg)
	_ = eng.Reset(ctx, 4, 4, "again", cfg)

	result, err := eng.GetBuffer(ctx, 4, 4)
	if err != nil {
		t.Fatalf("GetBuffer returned error: %v", err)
	}
	want := "hello\n\nworld\n\nagain"
	if result != want {
		t.Errorf("GetBuffer = %q, want %q", result, want)
	}
}

func TestTimerExists_ReturnsFalseAfterExpiry(t *testing.T) {
	eng, _ := setupEngine(t)
	ctx := context.Background()
	cfg := model.BotConfig{DebounceTime: 1} // 1 second TTL

	if err := eng.Start(ctx, 5, 5, "msg", cfg); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	exists, err := eng.TimerExists(ctx, 5, 5)
	if err != nil {
		t.Fatalf("TimerExists returned error: %v", err)
	}
	if !exists {
		t.Error("timer should exist immediately after Start")
	}

	time.Sleep(2000 * time.Millisecond)

	exists, err = eng.TimerExists(ctx, 5, 5)
	if err != nil {
		t.Fatalf("TimerExists returned error: %v", err)
	}
	if exists {
		t.Error("timer must be expired after TTL")
	}
}
