package metrics

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisHook instruments every Redis command centrally, so no call site in
// redisx, bus, ratelimit or scheduler has to time anything itself.
//
// The command name is used as a label: go-redis reports a fixed verb
// ("get", "zadd", "eval"), never an argument, so the label set stays
// bounded no matter what keys clients touch.
type RedisHook struct{ m *Metrics }

// NewRedisHook returns a hook to pass to client.AddHook.
func NewRedisHook(m *Metrics) *RedisHook { return &RedisHook{m: m} }

func (h *RedisHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (h *RedisHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmd)
		h.observe(cmd.Name(), start, err)
		return err
	}
}

func (h *RedisHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmds)
		// Attribute the round trip to every command in the pipeline: the
		// latency is shared, and splitting it would be a guess.
		for _, cmd := range cmds {
			h.observe(cmd.Name(), start, cmd.Err())
		}
		return err
	}
}

func (h *RedisHook) observe(name string, start time.Time, err error) {
	h.m.RedisDuration.WithLabelValues(name).Observe(time.Since(start).Seconds())
	// redis.Nil is a miss, not a failure, and counting it would make every
	// dashboard's error rate meaningless.
	if err != nil && !errors.Is(err, redis.Nil) {
		h.m.RedisErrors.WithLabelValues(name).Inc()
	}
}
