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
	// Required: SecretMiddleware compares the header against this, so an empty value
	// authenticates every caller that omits it.
	botRuntimeSecret, err := mustGetEnv("BOT_RUNTIME_SECRET")
	if err != nil {
		return nil, err
	}
	// CRM-236: 30s was dimensioned for a single, well-behaved model call. Measured
	// against gemini-2.5-flash, the SAME trivial prompt answered in
	// 0.74s / 0.79s / 1.36s / 10.34s / 20.40s — a 27x spread on the provider's
	// tail. A tool-calling turn makes at least TWO of those round trips (decide the
	// tool, then write the reply), so two bad tails alone exceed 30s with nothing
	// wrong in the code — and the customer got silence while the tool's side effect
	// (a moved pipeline card) had already been applied.
	//
	// 90s covers two tail-latency calls plus the CRM round trip, and still bounds a
	// genuinely hung provider. It is not a licence to hang: the fallback message
	// below is what protects the customer when the ceiling IS reached.
	aiCallTimeout, err := getEnvIntOrDefault("AI_CALL_TIMEOUT_SECONDS", 90)
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
