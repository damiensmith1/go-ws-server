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
//
// Addrs selects the topology, following go-redis's UniversalClient rules:
// a single address is a plain client, several are a Cluster client, and
// any number with MasterName set is a Sentinel failover client. Callers
// that only have a host:port pass it as the sole entry in Addrs.
type Options struct {
	Addrs      []string
	Password   string
	MasterName string
}

// New returns a configured redis.UniversalClient. The client lazily connects on
// first command, mirroring the TS `lazyConnect: true` setting.
func New(opts Options) redis.UniversalClient {
	return redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:       opts.Addrs,
		Password:    opts.Password,
		MasterName:  opts.MasterName,
		DialTimeout: 5 * time.Second,
		ReadTimeout: 5 * time.Second,
		MaxRetries:  3,
	})
}

// AddConnection increments the per-userKey active-connection refcount and,
// on the first connection, adds the userKey to the global `activeConnections`
// set.
func AddConnection(ctx context.Context, c redis.UniversalClient, userKey string) error {
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
func RemoveConnection(ctx context.Context, c redis.UniversalClient, userKey string) error {
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
func AddTopicSubscription(ctx context.Context, c redis.UniversalClient, userKey, topic string) error {
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
func RemoveTopicSubscription(ctx context.Context, c redis.UniversalClient, userKey, topic string) error {
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

// TopicSubscribers returns every userKey currently subscribed to a topic,
// across all instances. The set is maintained by AddTopicSubscription and
// RemoveTopicSubscription; nothing read it until presence existed.
func TopicSubscribers(ctx context.Context, c redis.UniversalClient, topic string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return c.SMembers(ctx, "topic:"+topic).Result()
}

// SubscribedTopics returns every topic the given userKey is subscribed to.
func SubscribedTopics(ctx context.Context, c redis.UniversalClient, userKey string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return c.SMembers(ctx, userKey+":subscribedTopics").Result()
}
