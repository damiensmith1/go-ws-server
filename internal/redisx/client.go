// Package redisx wraps the go-redis client with the operations used across
// the server: connection refcounts, distributed topic locks, scheduler
// queue ops, and atomic job claims. Redis key shapes match the reference
// TS implementation exactly so deployments remain interoperable.
package redisx

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Options configures a Redis client.
type Options struct {
	Addr     string
	Password string
}

// New returns a configured *redis.Client. The client lazily connects on
// first command, mirroring the TS `lazyConnect: true` setting.
func New(opts Options) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:        opts.Addr,
		Password:    opts.Password,
		DialTimeout: 5 * time.Second,
		ReadTimeout: 5 * time.Second,
		MaxRetries:  3,
	})
}

// AddConnection increments the per-userKey active-connection refcount and,
// on the first connection, adds the userKey to the global `activeConnections`
// set.
func AddConnection(ctx context.Context, c *redis.Client, userKey string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	count, err := c.Incr(ctx, "conn:count:"+userKey).Result()
	if err != nil {
		return fmt.Errorf("incr conn count: %w", err)
	}
	if count == 1 {
		if err := c.SAdd(ctx, "activeConnections", userKey).Err(); err != nil {
			return fmt.Errorf("sadd activeConnections: %w", err)
		}
	}
	return nil
}

// RemoveConnection decrements the per-userKey refcount; on reaching zero it
// deletes the count key and removes the userKey from `activeConnections`.
func RemoveConnection(ctx context.Context, c *redis.Client, userKey string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	count, err := c.Decr(ctx, "conn:count:"+userKey).Result()
	if err != nil {
		return fmt.Errorf("decr conn count: %w", err)
	}
	if count <= 0 {
		if err := c.Del(ctx, "conn:count:"+userKey).Err(); err != nil {
			return fmt.Errorf("del conn count: %w", err)
		}
		if err := c.SRem(ctx, "activeConnections", userKey).Err(); err != nil {
			return fmt.Errorf("srem activeConnections: %w", err)
		}
	}
	return nil
}

// AddTopicSubscription records that userKey is subscribed to topic, in both
// the per-topic membership set and the per-userKey index set.
func AddTopicSubscription(ctx context.Context, c *redis.Client, userKey, topic string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := c.SAdd(ctx, "topic:"+topic, userKey).Err(); err != nil {
		return fmt.Errorf("sadd topic members: %w", err)
	}
	if err := c.SAdd(ctx, userKey+":subscribedTopics", topic).Err(); err != nil {
		return fmt.Errorf("sadd subscribedTopics: %w", err)
	}
	return nil
}

// RemoveTopicSubscription is the inverse of AddTopicSubscription.
func RemoveTopicSubscription(ctx context.Context, c *redis.Client, userKey, topic string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := c.SRem(ctx, "topic:"+topic, userKey).Err(); err != nil {
		return fmt.Errorf("srem topic members: %w", err)
	}
	if err := c.SRem(ctx, userKey+":subscribedTopics", topic).Err(); err != nil {
		return fmt.Errorf("srem subscribedTopics: %w", err)
	}
	return nil
}

// SubscribedTopics returns every topic the given userKey is subscribed to.
func SubscribedTopics(ctx context.Context, c *redis.Client, userKey string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return c.SMembers(ctx, userKey+":subscribedTopics").Result()
}
