// Package ratelimit implements a sliding-window rate limiter backed by
// Redis. Each (bucket, userKey) pair gets a sorted set holding one member
// per request, scored by arrival time in milliseconds.
//
// A fixed window — INCR plus EXPIRE — is cheaper but lets a caller spend
// its whole allowance at the end of one window and again at the start of
// the next, so a 50/sec limit admits up to 100 requests across a window
// boundary. Scoring each request and trimming by score bounds the count
// over any span of `Window`, not just over aligned windows.
//
// Memory per key is one member per request in the window, so it stays
// proportional to the limit itself: 50 members for 50/sec.
package ratelimit

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
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

// nowFn is a seam for tests. Production always reads the wall clock.
//
// The timestamp is taken on the server rather than inside the script:
// redis.call("TIME") is non-deterministic, and passing it in keeps the
// script replayable. The cost is that clock skew between instances
// shifts the window slightly, which is acceptable for rate limiting.
var nowFn = time.Now

// seq disambiguates two requests that land in the same millisecond, so
// neither overwrites the other's member in the sorted set.
var seq atomic.Uint64

// script trims everything older than the window, counts what is left, and
// records the new request only if there is room. Returns 1 when allowed.
var script = redis.NewScript(`
local key    = KEYS[1]
local now    = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit  = tonumber(ARGV[3])
local member = ARGV[4]

redis.call("ZREMRANGEBYSCORE", key, "-inf", now - window)
if redis.call("ZCARD", key) >= limit then
  return 0
end
redis.call("ZADD", key, now, member)
redis.call("PEXPIRE", key, window)
return 1
`)

// Allow returns true if the request is within the configured limit.
func Allow(ctx context.Context, c redis.UniversalClient, bucket, userKey string, cfg Config) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	nowMs := nowFn().UnixMilli()
	member := strconv.FormatInt(nowMs, 10) + "-" + strconv.FormatUint(seq.Add(1), 10)

	key := fmt.Sprintf("rl:%s:%s", bucket, userKey)
	allowed, err := script.Run(ctx, c, []string{key},
		nowMs, cfg.Window.Milliseconds(), cfg.MaxRequests, member,
	).Int()
	if err != nil {
		return false, fmt.Errorf("eval ratelimit: %w", err)
	}
	return allowed == 1, nil
}
