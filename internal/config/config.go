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
	AIProcessorBaseURL   string
	AICallTimeoutSeconds int
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
	botRuntimeSecret, err := mustGetEnv("BOT_RUNTIME_SECRET")
	if err != nil {
		return nil, err
	}
	aiProcessorBaseURL, err := mustGetEnv("AI_PROCESSOR_BASE_URL")
	if err != nil {
		return nil, err
	}
	aiCallTimeout, err := getEnvIntOrDefault("AI_CALL_TIMEOUT_SECONDS", 30)
	if err != nil {
		return nil, err
	}

	return &Config{
		ListenAddr:           listenAddr,
		RedisURL:             redisURL,
		BotRuntimeSecret:     botRuntimeSecret,
		AIProcessorBaseURL:   aiProcessorBaseURL,
		AICallTimeoutSeconds: aiCallTimeout,
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
