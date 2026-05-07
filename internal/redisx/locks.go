package redisx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// LockKind enumerates the two lock kinds clients can take on a topic.
type LockKind string

const (
	LockPublish   LockKind = "publish"
	LockSubscribe LockKind = "subscribe"
)

// DefaultLockTTL is the TTL applied to a freshly acquired or renewed lock.
// Five minutes matches the reference TS implementation; clients are
// expected to renew before expiry.
const DefaultLockTTL = 5 * time.Minute

// ErrLockHeldByOther is returned when an attempt to acquire a topic lock
// fails because another holder owns it.
var ErrLockHeldByOther = errors.New("topic locked by another holder")

// ErrLockNotHeld is returned by RenewLock when the lock is missing or
// owned by someone other than the caller.
var ErrLockNotHeld = errors.New("lock not held by caller")

// unlockScript releases a lock only if the caller currently owns it,
// preventing the classic "I deleted someone else's lock" race.
var unlockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`)

func lockKey(topic string, kind LockKind) string {
	return string(kind) + ":lock:" + topic
}

// LockTopic atomically acquires (or refreshes) the named topic lock for the
// given holder. Returns ErrLockHeldByOther if another holder owns it.
func LockTopic(ctx context.Context, c *redis.Client, topic string, kind LockKind, holder string, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = DefaultLockTTL
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	key := lockKey(topic, kind)
	ok, err := c.SetNX(ctx, key, holder, ttl).Result()
	if err != nil {
		return fmt.Errorf("setnx lock: %w", err)
	}
	if ok {
		if err := c.SAdd(ctx, holder+":lockedTopics", topic).Err(); err != nil {
			return fmt.Errorf("sadd lockedTopics: %w", err)
		}
		return nil
	}
	owner, err := c.Get(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("get lock owner: %w", err)
	}
	if owner == holder {
		return nil
	}
	return ErrLockHeldByOther
}

// UnlockTopic releases the lock if (and only if) holder currently owns it.
// Silent no-op when the lock is missing or owned by someone else.
func UnlockTopic(ctx context.Context, c *redis.Client, topic string, kind LockKind, holder string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	released, err := unlockScript.Run(ctx, c, []string{lockKey(topic, kind)}, holder).Int()
	if err != nil {
		return fmt.Errorf("eval unlock: %w", err)
	}
	if released == 1 {
		if err := c.SRem(ctx, holder+":lockedTopics", topic).Err(); err != nil {
			return fmt.Errorf("srem lockedTopics: %w", err)
		}
	}
	return nil
}

// RenewLock extends the TTL on a lock the caller already owns. Returns
// ErrLockNotHeld if the caller is not the current owner.
func RenewLock(ctx context.Context, c *redis.Client, topic string, kind LockKind, holder string, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = DefaultLockTTL
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	key := lockKey(topic, kind)
	owner, err := c.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ErrLockNotHeld
		}
		return fmt.Errorf("get lock owner: %w", err)
	}
	if owner != holder {
		return ErrLockNotHeld
	}
	if err := c.Set(ctx, key, holder, ttl).Err(); err != nil {
		return fmt.Errorf("set lock: %w", err)
	}
	return nil
}

// IsTopicLockedByOther reports whether the named lock is held by someone
// other than userKey. A lock held by the caller, or no lock at all, returns
// false.
func IsTopicLockedByOther(ctx context.Context, c *redis.Client, topic string, kind LockKind, userKey string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	owner, err := c.Get(ctx, lockKey(topic, kind)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, fmt.Errorf("get lock owner: %w", err)
	}
	return owner != userKey, nil
}

// LockedTopics returns the topics the holder currently owns locks on. Note
// that, in the TS implementation, a single set is shared by both lock
// kinds — we preserve that shape.
func LockedTopics(ctx context.Context, c *redis.Client, holder string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return c.SMembers(ctx, holder+":lockedTopics").Result()
}

// ReleaseUserLocks unlocks every lock owned by holder, across both kinds.
// Used during graceful shutdown and on last-socket disconnect.
func ReleaseUserLocks(ctx context.Context, c *redis.Client, holder string) error {
	topics, err := LockedTopics(ctx, c, holder)
	if err != nil {
		return err
	}
	for _, t := range topics {
		if err := UnlockTopic(ctx, c, t, LockPublish, holder); err != nil {
			return err
		}
		if err := UnlockTopic(ctx, c, t, LockSubscribe, holder); err != nil {
			return err
		}
	}
	return nil
}
