// Package redisx wraps the go-redis client with the operations used across
// the server: connection refcounts, distributed topic locks, scheduler
// queue ops, and atomic job claims. Redis key shapes match the reference
// TS implementation exactly so deployments remain interoperable.
package redisx

import (
	"context"
	"fmt"
	"strconv"
	"strings"
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

// ackCursorKey is per userKey and topic, not per connection: a user's
// reading position should survive the socket that established it, which
// is the entire point of storing it server side.
func ackCursorKey(userKey, topic string) string {
	return "ack:" + userKey + ":" + topic
}

// SetAckCursor records how far a userKey has acknowledged a topic.
//
// Cursors move forward only. Redelivery means a client can legitimately
// ack a stream ID it has already acked, and an out-of-order ack must not
// rewind the cursor and replay everything after it again.
func SetAckCursor(ctx context.Context, c redis.UniversalClient, userKey, topic, streamID string, ttl time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	key := ackCursorKey(userKey, topic)
	current, err := c.Get(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("get ack cursor: %w", err)
	}
	if current != "" && !StreamIDLess(current, streamID) {
		return nil
	}
	if err := c.Set(ctx, key, streamID, ttl).Err(); err != nil {
		return fmt.Errorf("set ack cursor: %w", err)
	}
	return nil
}

// AckCursor returns the last acknowledged stream ID for a userKey and
// topic, or "" if there is none.
func AckCursor(ctx context.Context, c redis.UniversalClient, userKey, topic string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	v, err := c.Get(ctx, ackCursorKey(userKey, topic)).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("get ack cursor: %w", err)
	}
	return v, nil
}

// StreamIDLess compares two Redis stream IDs of the form "<ms>-<seq>".
//
// Lexicographic comparison is wrong here: "10-0" sorts before "9-0" as a
// string, so a naive compare would treat a newer cursor as older and
// replay history the client has already seen.
func StreamIDLess(a, b string) bool {
	amsStr, aseqStr, _ := strings.Cut(a, "-")
	bmsStr, bseqStr, _ := strings.Cut(b, "-")
	ams, _ := strconv.ParseInt(amsStr, 10, 64)
	bms, _ := strconv.ParseInt(bmsStr, 10, 64)
	if ams != bms {
		return ams < bms
	}
	aseq, _ := strconv.ParseInt(aseqStr, 10, 64)
	bseq, _ := strconv.ParseInt(bseqStr, 10, 64)
	return aseq < bseq
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
