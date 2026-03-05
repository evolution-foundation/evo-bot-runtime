// Package testhelpers provides shared test helpers for evo-bot-runtime packages.
package testhelpers

import (
	"os"

	"github.com/redis/go-redis/v9"
)

// RedisOptions returns Redis client options for tests.
// It reads REDIS_TEST_URL (falls back to REDIS_URL, then redis://localhost:6379).
// The DB is set from the REDIS_TEST_DB environment variable if present;
// otherwise defaults to 15 to avoid collisions with application data.
//
// Packages that run tests in parallel against the same Redis instance should
// override REDIS_TEST_DB per package in CI (e.g. via go test -env flags or
// a test wrapper script).
func RedisOptions() *redis.Options {
	url := os.Getenv("REDIS_TEST_URL")
	if url == "" {
		url = os.Getenv("REDIS_URL")
	}
	if url == "" {
		url = "redis://localhost:6379"
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		panic("invalid Redis URL for tests: " + err.Error())
	}
	opt.DB = 15 // safe default; override via REDIS_TEST_URL with /db path segment
	return opt
}
