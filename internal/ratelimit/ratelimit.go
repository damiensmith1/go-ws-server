// Package ratelimit implements a simple fixed-window rate limiter backed
// by Redis. Each (bucket, userKey) pair has its own counter; on the first
// hit the key is given an EXPIRE equal to the window length, so the
// counter resets exactly once per window.
package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config describes one rate-limit bucket.
type Config struct {
	Window      time.Duration
	MaxRequests int
}

// Default configs match the TS reference defaults.
func DefaultMessageConfig(perSec int) Config {
	if perSec <= 0 {
		perSec = 50
	}
	return Config{Window: time.Second, MaxRequests: perSec}
}

func DefaultJobConfig(perMin int) Config {
	if perMin <= 0 {
		perMin = 30
	}
	return Config{Window: time.Minute, MaxRequests: perMin}
}

// script atomically increments and, on the first hit, applies an EXPIRE.
var script = redis.NewScript(`
local current = redis.call("INCR", KEYS[1])
if current == 1 then
  redis.call("EXPIRE", KEYS[1], ARGV[1])
end
return current
`)

// Allow returns true if the request is within the configured limit.
func Allow(ctx context.Context, c *redis.Client, bucket, userKey string, cfg Config) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	key := fmt.Sprintf("rl:%s:%s", bucket, userKey)
	count, err := script.Run(ctx, c, []string{key}, int(cfg.Window.Seconds())).Int()
	if err != nil {
		return false, fmt.Errorf("eval ratelimit: %w", err)
	}
	return count <= cfg.MaxRequests, nil
}
