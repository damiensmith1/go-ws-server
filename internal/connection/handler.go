package connection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"

	"github.com/damiensmith1/go-ws-server/internal/bus"
	"github.com/damiensmith1/go-ws-server/internal/protocol"
	"github.com/damiensmith1/go-ws-server/internal/ratelimit"
	"github.com/damiensmith1/go-ws-server/internal/redisx"
)

// Deps is the bundle of dependencies a connection needs to dispatch
// messages. Intentionally explicit: each field shows what handler.go can
// reach for, and missing fields cause compile errors rather than silent
// nil panics.
type Deps struct {
	RDB              redis.UniversalClient
	Bus              *bus.Bus
	Hub              *Hub
	Log              *slog.Logger
	MessageRateLimit ratelimit.Config
	JobRateLimit     ratelimit.Config
	OnSchedulerWake  func()
}

// Dispatch parses one inbound frame and runs its handler. Errors flow back
// to the client as `{type:"error", reqID?, message}` frames.
func Dispatch(ctx context.Context, c *Conn, raw []byte, d Deps) {
	log := d.Log
	if log == nil {
		log = slog.Default()
	}

	var env protocol.Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		c.SendNow(ctx, protocol.EncodeError("", "Invalid JSON"))
		return
	}
	reqID := env.ReqID

	allowed, err := ratelimit.Allow(ctx, d.RDB, "msg", c.UserKey(), d.MessageRateLimit)
	if err != nil {
		log.Error("rate-limit check failed", "err", err.Error())
		c.SendNow(ctx, protocol.EncodeError(reqID, "Internal error"))
		return
	}
	if !allowed {
		c.SendNow(ctx, protocol.EncodeError(reqID, "Rate limit exceeded"))
		return
	}

	if err := protocol.Validate(&env); err != nil {
		c.SendNow(ctx, protocol.EncodeError(reqID, err.Error()))
		return
	}

	log.Debug("message received", "type", env.Type, "reqID", reqID)

	var (
		responseMsg string
		handlerErr  error
	)
	switch env.Type {
	case protocol.TypeSubscribe:
		responseMsg, handlerErr = handleSubscribe(ctx, c, &env, d)
	case protocol.TypeUnsubscribe:
		responseMsg, handlerErr = handleUnsubscribe(ctx, c, &env, d)
	case protocol.TypePublish:
		responseMsg, handlerErr = handlePublish(ctx, c, &env, d)
	case protocol.TypeLockTopic:
		responseMsg, handlerErr = handleLockTopic(ctx, c, &env, d)
	case protocol.TypeUnlockTopic:
		responseMsg, handlerErr = handleUnlockTopic(ctx, c, &env, d)
	case protocol.TypeRenewLock:
		responseMsg, handlerErr = handleRenewLock(ctx, c, &env, d)
	case protocol.TypeScheduleJob:
		responseMsg, handlerErr = handleScheduleJob(ctx, c, &env, d)
	case protocol.TypeRemoveJob:
		responseMsg, handlerErr = handleRemoveJob(ctx, c, &env, d)
	case protocol.TypeBroadcast:
		// Broadcasts are fire-and-forget — the sender doesn't get a reply.
		_, handlerErr = handleBroadcast(ctx, c, &env, d)
	}

	if handlerErr != nil {
		log.Error("message handling failed", "type", env.Type, "err", handlerErr.Error())
		c.SendNow(ctx, protocol.EncodeError(reqID, handlerErr.Error()))
		return
	}
	if responseMsg != "" {
		c.SendNow(ctx, protocol.EncodeSuccess(reqID, responseMsg))
	}
}

func handleSubscribe(ctx context.Context, c *Conn, env *protocol.Envelope, d Deps) (string, error) {
	// Lock check + Redis-side subscription registration.
	locked, err := redisx.IsTopicLockedByOther(ctx, d.RDB, env.Topic, redisx.LockSubscribe, c.UserKey())
	if err != nil {
		return "", fmt.Errorf("check subscribe lock: %w", err)
	}
	if locked {
		return "", fmt.Errorf("Topic %s is locked for subscribing by another instance.", env.Topic)
	}
	if err := redisx.AddTopicSubscription(ctx, d.RDB, c.UserKey(), env.Topic); err != nil {
		return "", err
	}

	if len(env.Since) > 0 {
		since, err := protocol.SinceToString(env.Since)
		if err != nil {
			return "", err
		}
		truncated, oldest, err := d.Bus.SubscribeWithReplay(ctx, c, env.Topic, since)
		if err != nil {
			return "", err
		}
		if truncated {
			c.SendNow(ctx, protocol.EncodeReplayTruncated(env.Topic, oldest))
		}
	} else {
		d.Bus.AddLocalSubscription(env.Topic, c)
	}

	return fmt.Sprintf("Successfully subscribed to topic %s", env.Topic), nil
}

func handleUnsubscribe(ctx context.Context, c *Conn, env *protocol.Envelope, d Deps) (string, error) {
	if err := redisx.RemoveTopicSubscription(ctx, d.RDB, c.UserKey(), env.Topic); err != nil {
		return "", err
	}
	d.Bus.RemoveLocalSubscription(env.Topic, c)
	return fmt.Sprintf("Successfully unsubscribed from topic %s", env.Topic), nil
}

func handlePublish(ctx context.Context, c *Conn, env *protocol.Envelope, d Deps) (string, error) {
	locked, err := redisx.IsTopicLockedByOther(ctx, d.RDB, env.Topic, redisx.LockPublish, c.UserKey())
	if err != nil {
		return "", fmt.Errorf("check publish lock: %w", err)
	}
	if locked {
		return "", fmt.Errorf("Topic %s is locked for publishing by another instance.", env.Topic)
	}
	if _, err := d.Bus.PublishTopic(ctx, env.Topic, env.Data); err != nil {
		return "", err
	}
	return fmt.Sprintf("Successfully published to topic %s", env.Topic), nil
}

func handleLockTopic(ctx context.Context, c *Conn, env *protocol.Envelope, d Deps) (string, error) {
	if err := redisx.LockTopic(ctx, d.RDB, env.Topic, redisx.LockKind(env.LockType), c.UserKey(), redisx.DefaultLockTTL); err != nil {
		if errors.Is(err, redisx.ErrLockHeldByOther) {
			return "", fmt.Errorf("Cannot lock topic %s for %s. It is already locked.", env.Topic, env.LockType)
		}
		return "", err
	}
	return fmt.Sprintf("Successfully locked topic %s for %s. You must renew the lock within 5 minutes.", env.Topic, env.LockType), nil
}

func handleUnlockTopic(ctx context.Context, c *Conn, env *protocol.Envelope, d Deps) (string, error) {
	if err := redisx.UnlockTopic(ctx, d.RDB, env.Topic, redisx.LockKind(env.LockType), c.UserKey()); err != nil {
		return "", err
	}
	return fmt.Sprintf("Successfully unlocked topic %s for %s.", env.Topic, env.LockType), nil
}

func handleRenewLock(ctx context.Context, c *Conn, env *protocol.Envelope, d Deps) (string, error) {
	if err := redisx.RenewLock(ctx, d.RDB, env.Topic, redisx.LockKind(env.LockType), c.UserKey(), redisx.DefaultLockTTL); err != nil {
		if errors.Is(err, redisx.ErrLockNotHeld) {
			return "", fmt.Errorf("Cannot renew lock on topic %s. It is either not locked or locked by another user.", env.Topic)
		}
		return "", err
	}
	return fmt.Sprintf("Successfully renewed lock on topic %s for %s", env.Topic, env.LockType), nil
}

func handleScheduleJob(ctx context.Context, c *Conn, env *protocol.Envelope, d Deps) (string, error) {
	allowed, err := ratelimit.Allow(ctx, d.RDB, "job", c.UserKey(), d.JobRateLimit)
	if err != nil {
		return "", fmt.Errorf("rate-limit check: %w", err)
	}
	if !allowed {
		return "", errors.New("Rate limit exceeded for scheduleJob")
	}
	if err := redisx.AddJob(ctx, d.RDB, env.JobData); err != nil {
		return "", err
	}
	if d.OnSchedulerWake != nil {
		d.OnSchedulerWake()
	}
	return fmt.Sprintf("Successfully scheduled job %s", env.JobData.JobID), nil
}

func handleRemoveJob(ctx context.Context, c *Conn, env *protocol.Envelope, d Deps) (string, error) {
	if err := redisx.RemoveJob(ctx, d.RDB, env.JobID); err != nil {
		return "", err
	}
	return fmt.Sprintf("Successfully removed job %s", env.JobID), nil
}

func handleBroadcast(ctx context.Context, c *Conn, env *protocol.Envelope, d Deps) (string, error) {
	if err := d.Bus.PublishBroadcast(ctx, c.UserKey(), env.Data); err != nil {
		return "", err
	}
	return "", nil
}

// Cleanup is called once per connection on disconnect. Mirrors the TS
// handleDisconnection: drop local subscriptions, decrement the global
// connection count, and on the last socket for this userKey release any
// locks and remove all topic subscriptions.
func Cleanup(ctx context.Context, c *Conn, d Deps, isLast bool) {
	d.Bus.RemoveSubscriberAll(c)

	if isLast {
		topics, err := redisx.LockedTopics(ctx, d.RDB, c.UserKey())
		if err != nil {
			d.Log.Warn("get locked topics on disconnect failed", "err", err.Error())
		}
		for _, t := range topics {
			if err := redisx.UnlockTopic(ctx, d.RDB, t, redisx.LockPublish, c.UserKey()); err != nil {
				d.Log.Warn("unlock publish on disconnect failed", "topic", t, "err", err.Error())
			}
			if err := redisx.UnlockTopic(ctx, d.RDB, t, redisx.LockSubscribe, c.UserKey()); err != nil {
				d.Log.Warn("unlock subscribe on disconnect failed", "topic", t, "err", err.Error())
			}
		}

		subs, err := redisx.SubscribedTopics(ctx, d.RDB, c.UserKey())
		if err != nil {
			d.Log.Warn("get subscribed topics on disconnect failed", "err", err.Error())
		}
		for _, t := range subs {
			if err := redisx.RemoveTopicSubscription(ctx, d.RDB, c.UserKey(), t); err != nil {
				d.Log.Warn("remove subscription on disconnect failed", "topic", t, "err", err.Error())
			}
		}
	}

	if err := redisx.RemoveConnection(ctx, d.RDB, c.UserKey()); err != nil {
		d.Log.Warn("decrement conn count failed", "err", err.Error())
	}
	d.Log.Info("websocket client disconnected", "userKey", c.UserKey(), "isLastSocket", isLast)
}
