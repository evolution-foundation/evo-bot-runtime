package config

import (
	"log/slog"
	"os"
	"strconv"
)

type Config struct {
	ListenAddr           string
	RedisURL             string
	BotRuntimeSecret     string
	AIProcessorURL       string
	AIProcessorAPIKey    string
	AICallTimeoutSeconds int
}

func Load() *Config {
	return &Config{
		ListenAddr:           requireEnv("LISTEN_ADDR"),
		RedisURL:             requireEnv("REDIS_URL"),
		BotRuntimeSecret:     requireEnv("BOT_RUNTIME_SECRET"),
		AIProcessorURL:       requireEnv("AI_PROCESSOR_URL"),
		AIProcessorAPIKey:    requireEnv("AI_PROCESSOR_API_KEY"),
		AICallTimeoutSeconds: optionalEnvInt("AI_CALL_TIMEOUT_SECONDS", 30),
	}
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("missing required environment variable", "key", key)
		os.Exit(1)
	}
	return v
}

func optionalEnvInt(key string, defaultVal int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		slog.Error("invalid integer environment variable", "key", key, "value", v)
		os.Exit(1)
	}
	return n
}
