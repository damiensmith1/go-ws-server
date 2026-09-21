package connection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/damiensmith1/go-ws-server/authz"
	"github.com/damiensmith1/go-ws-server/bus"
	"github.com/damiensmith1/go-ws-server/internal/ratelimit"
	"github.com/damiensmith1/go-ws-server/internal/redisx"
	"github.com/damiensmith1/go-ws-server/metrics"
	"github.com/damiensmith1/go-ws-server/protocol"
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

	// Authorizer gates per-topic access. A nil value allows everything,
	// which is the behaviour the server had before authorization existed.
	Authorizer authz.Authorizer

	// PresenceTopic receives connect and disconnect events for the first
	// and last socket of each userKey. Empty disables the feed, which is
	// the default: publishing user activity to a topic anyone might
	// subscribe to should be a deliberate choice.
	PresenceTopic string

	// Metrics is optional. A nil value gets a private collector set, so
	// call sites never need a nil check.
	Metrics *metrics.Metrics
}

// logger and metrics resolve the optional Deps fields once, so handlers
// never repeat the nil check.
func (d Deps) logger() *slog.Logger {
	if d.Log != nil {
		return d.Log
	}
	return slog.Default()
}

func (d Deps) metrics() *metrics.Metrics {
	if d.Metrics != nil {
		return d.Metrics
	}
	return metrics.New()
}

// knownFrameTypes bounds the `type` label. env.Type is client-controlled,
// so feeding it to a label verbatim would let anyone mint unlimited time
// series by sending junk type strings.
var knownFrameTypes = map[string]struct{}{
	protocol.TypeSubscribe:   {},
	protocol.TypeUnsubscribe: {},
	protocol.TypePublish:     {},
	protocol.TypeLockTopic:   {},
	protocol.TypeUnlockTopic: {},
	protocol.TypeRenewLock:   {},
	protocol.TypeScheduleJob: {},
	protocol.TypeRemoveJob:   {},
	protocol.TypeBroadcast:   {},
	protocol.TypePresence:    {},
	protocol.TypeListSubs:    {},
}

func frameTypeLabel(t string) string {
	if _, ok := knownFrameTypes[t]; ok {
		return t
	}
	return "unknown"
}

// authorize runs the per-topic policy for one frame.
//
// The three outcomes are kept apart deliberately. Allowed proceeds.
// Denied is the client's own fault and is named plainly in the reply.
// Anything else means the policy could not be evaluated — the caller
// still fails closed, but the client is told "Internal error" rather than
// "not authorized", because reporting an outage as a permission change
// sends operators hunting through policy config for a problem that is not
// there.
func authorize(ctx context.Context, c *Conn, action authz.Action, topic string, d Deps) error {
	m, log := d.metrics(), d.logger()
	if d.Authorizer == nil {
		m.AuthzDecisions.WithLabelValues(string(action), "allowed").Inc()
		return nil
	}
	err := d.Authorizer.Authorize(ctx, authz.Request{
		UserKey: c.UserKey(),
		Action:  action,
		Topic:   topic,
		Claims:  c.Claims(),
	})
	switch {
	case err == nil:
		m.AuthzDecisions.WithLabelValues(string(action), "allowed").Inc()
		return nil
	case authz.Denied(err):
		m.AuthzDecisions.WithLabelValues(string(action), "denied").Inc()
		log.Info("topic access denied", "userKey", c.UserKey(), "action", string(action), "topic", topic)
		return fmt.Errorf("Not authorized to %s topic %s.", action, topic)
	default:
		m.AuthzDecisions.WithLabelValues(string(action), "error").Inc()
		log.Error("authorization check failed", "action", string(action), "topic", topic, "err", err.Error())
		return errors.New("Internal error")
	}
}

func decisionLabel(allowed bool) string {
	if allowed {
		return "allowed"
	}
	return "denied"
}

// Dispatch parses one inbound frame and runs its handler. Errors flow back
// to the client as `{type:"error", reqID?, message}` frames.
func Dispatch(ctx context.Context, c *Conn, raw []byte, d Deps) {
	log := d.Log
	if log == nil {
		log = slog.Default()
	}
	m := d.Metrics
	if m == nil {
		m = metrics.New()
	}

	var env protocol.Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		m.FramesReceived.WithLabelValues("invalid_json").Inc()
		c.SendNow(ctx, protocol.EncodeError("", "Invalid JSON"))
		return
	}
	reqID := env.ReqID
	m.FramesReceived.WithLabelValues(frameTypeLabel(env.Type)).Inc()

	allowed, err := ratelimit.Allow(ctx, d.RDB, "msg", c.UserKey(), d.MessageRateLimit)
	if err != nil {
		log.Error("rate-limit check failed", "err", err.Error())
		c.SendNow(ctx, protocol.EncodeError(reqID, "Internal error"))
		return
	}
	m.RateLimitDecisions.WithLabelValues("msg", decisionLabel(allowed)).Inc()
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
	case protocol.TypePresence:
		_, handlerErr = handlePresence(ctx, c, &env, d)
	case protocol.TypeListSubs:
		_, handlerErr = handleListSubscriptions(ctx, c, &env, d)
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
	if err := authorize(ctx, c, authz.ActionSubscribe, env.Topic, d); err != nil {
		return "", err
	}
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

// handleUnsubscribe is deliberately not gated. Dropping your own
// subscription removes access; it never grants any, and a policy change
// that revokes subscribe must not also trap a client in a topic it can no
// longer read.
func handleUnsubscribe(ctx context.Context, c *Conn, env *protocol.Envelope, d Deps) (string, error) {
	if err := redisx.RemoveTopicSubscription(ctx, d.RDB, c.UserKey(), env.Topic); err != nil {
		return "", err
	}
	d.Bus.RemoveLocalSubscription(env.Topic, c)
	return fmt.Sprintf("Successfully unsubscribed from topic %s", env.Topic), nil
}

func handlePublish(ctx context.Context, c *Conn, env *protocol.Envelope, d Deps) (string, error) {
	if err := authorize(ctx, c, authz.ActionPublish, env.Topic, d); err != nil {
		return "", err
	}
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
	if err := authorize(ctx, c, authz.ActionLock, env.Topic, d); err != nil {
		return "", err
	}
	if err := redisx.LockTopic(ctx, d.RDB, env.Topic, redisx.LockKind(env.LockType), c.UserKey(), redisx.DefaultLockTTL); err != nil {
		if errors.Is(err, redisx.ErrLockHeldByOther) {
			return "", fmt.Errorf("Cannot lock topic %s for %s. It is already locked.", env.Topic, env.LockType)
		}
		return "", err
	}
	return fmt.Sprintf("Successfully locked topic %s for %s. You must renew the lock within 5 minutes.", env.Topic, env.LockType), nil
}

func handleUnlockTopic(ctx context.Context, c *Conn, env *protocol.Envelope, d Deps) (string, error) {
	if err := authorize(ctx, c, authz.ActionLock, env.Topic, d); err != nil {
		return "", err
	}
	if err := redisx.UnlockTopic(ctx, d.RDB, env.Topic, redisx.LockKind(env.LockType), c.UserKey()); err != nil {
		return "", err
	}
	return fmt.Sprintf("Successfully unlocked topic %s for %s.", env.Topic, env.LockType), nil
}

func handleRenewLock(ctx context.Context, c *Conn, env *protocol.Envelope, d Deps) (string, error) {
	if err := authorize(ctx, c, authz.ActionLock, env.Topic, d); err != nil {
		return "", err
	}
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
	if d.Metrics != nil && err == nil {
		d.Metrics.RateLimitDecisions.WithLabelValues("job", decisionLabel(allowed)).Inc()
	}
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

// handlePresence reports who is subscribed to a topic.
//
// Gated as a read of the topic: the membership list is as sensitive as
// the messages, so anyone who may not subscribe may not enumerate the
// subscribers either.
func handlePresence(ctx context.Context, c *Conn, env *protocol.Envelope, d Deps) (string, error) {
	if err := authorize(ctx, c, authz.ActionSubscribe, env.Topic, d); err != nil {
		return "", err
	}
	subs, err := redisx.TopicSubscribers(ctx, d.RDB, env.Topic)
	if err != nil {
		return "", err
	}
	c.SendNow(ctx, protocol.EncodePresence(env.ReqID, env.Topic, subs))
	return "", nil
}

// handleListSubscriptions reports the topics this client is subscribed
// to. Ungated: it reveals only what the caller already did.
func handleListSubscriptions(ctx context.Context, c *Conn, env *protocol.Envelope, d Deps) (string, error) {
	topics, err := redisx.SubscribedTopics(ctx, d.RDB, c.UserKey())
	if err != nil {
		return "", err
	}
	c.SendNow(ctx, protocol.EncodeSubscriptions(env.ReqID, topics))
	return "", nil
}

func handleBroadcast(ctx context.Context, c *Conn, env *protocol.Envelope, d Deps) (string, error) {
	if err := d.Bus.PublishBroadcast(ctx, c.UserKey(), env.Data); err != nil {
		return "", err
	}
	return "", nil
}

// Presence event names published to Deps.PresenceTopic.
const (
	PresenceConnected    = "connected"
	PresenceDisconnected = "disconnected"
)

type presenceEvent struct {
	Event   string `json:"event"`
	UserKey string `json:"userKey"`
	At      string `json:"at"`
}

// PublishPresence announces that a userKey has become present or absent.
//
// Fired only for the first and last socket of a userKey, not every
// connection: a user with three tabs open is present once. Failures are
// logged and swallowed — a presence feed is an observation of the system,
// and must never be able to fail a connection or a disconnection.
func PublishPresence(ctx context.Context, userKey, event string, d Deps) {
	if d.PresenceTopic == "" || d.Bus == nil {
		return
	}
	payload, err := json.Marshal(presenceEvent{
		Event:   event,
		UserKey: userKey,
		At:      time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return
	}
	if _, err := d.Bus.PublishTopic(ctx, d.PresenceTopic, payload); err != nil {
		d.logger().Warn("publish presence event failed",
			"event", event, "userKey", userKey, "topic", d.PresenceTopic, "err", err.Error())
	}
}

// Cleanup is called once per connection on disconnect. Mirrors the TS
// handleDisconnection: drop local subscriptions, decrement the global
// connection count, and on the last socket for this userKey release any
// locks and remove all topic subscriptions.
func Cleanup(ctx context.Context, c *Conn, d Deps, isLast bool) {
	d.Bus.RemoveSubscriberAll(c)

	if isLast {
		PublishPresence(ctx, c.UserKey(), PresenceDisconnected, d)
	}

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
