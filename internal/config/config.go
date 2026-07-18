package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	ListenAddr           string
	RedisURL             string
	BotRuntimeSecret     string
	AICallTimeoutSeconds int
	// EVO-2167: retry the AI Processor call on transient failures (5xx/429/network)
	// so a momentary blip (deploy, restart, DB hiccup) does not leave the customer
	// without a reply. AICallMaxRetries is retries AFTER the first attempt.
	AICallMaxRetries  int
	AICallRetryBaseMs int
}

func Load() (*Config, error) {
	listenAddr, err := mustGetEnv("LISTEN_ADDR")
	if err != nil {
		return nil, err
	}
	redisURL, err := mustGetEnv("REDIS_URL")
	if err != nil {
		return nil, err
	}
	botRuntimeSecret := os.Getenv("BOT_RUNTIME_SECRET")
	aiCallTimeout, err := getEnvIntOrDefault("AI_CALL_TIMEOUT_SECONDS", 30)
	if err != nil {
		return nil, err
	}
	aiCallMaxRetries, err := getEnvIntOrDefault("AI_CALL_MAX_RETRIES", 2)
	if err != nil {
		return nil, err
	}
	aiCallRetryBaseMs, err := getEnvIntOrDefault("AI_CALL_RETRY_BASE_MS", 200)
	if err != nil {
		return nil, err
	}

	return &Config{
		ListenAddr:           listenAddr,
		RedisURL:             redisURL,
		BotRuntimeSecret:     botRuntimeSecret,
		AICallTimeoutSeconds: aiCallTimeout,
		AICallMaxRetries:     aiCallMaxRetries,
		AICallRetryBaseMs:    aiCallRetryBaseMs,
	}, nil
}

func mustGetEnv(key string) (string, error) {
	v := os.Getenv(key)
	if v == "" {
		return "", fmt.Errorf("missing required environment variable: %s", key)
	}
	return v, nil
}

func getEnvIntOrDefault(key string, defaultVal int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid integer for environment variable %s: %q", key, v)
	}
	return n, nil
}
