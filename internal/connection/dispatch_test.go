package connection

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/damiensmith1/go-ws-server/bus"
	"github.com/damiensmith1/go-ws-server/handler"
	"github.com/damiensmith1/go-ws-server/internal/ratelimit"
	"github.com/damiensmith1/go-ws-server/internal/redisx"
	"github.com/damiensmith1/go-ws-server/metrics"
	"github.com/damiensmith1/go-ws-server/protocol"
)

// harness is a dispatch-level fixture: a real Redis (miniredis), a real
// bus, and a Conn whose frames are captured instead of written to a
// socket. Dispatch is the widest untested surface in the server, and
// testing it against fakes would mostly test the fakes.
type harness struct {
	t    *testing.T
	conn *Conn
	deps Deps
	mr   *miniredis.Miniredis
	m    *metrics.Metrics
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	pub := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	sub := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close(); pub.Close(); sub.Close() })

	m := metrics.New()
	hub := NewHub(quietLogger())
	b := bus.New(pub, sub, hub, bus.Config{StreamMaxLength: 100, Metrics: m}, quietLogger())

	c := NewConn(nil, ConnConfig{
		UserKey:          "alice",
		SendChanCapacity: 256,
		MaxBufferedBytes: 1 << 20,
		Metrics:          m,
	}, quietLogger())
	hub.Add(c)

	return &harness{
		t:    t,
		conn: c,
		mr:   mr,
		m:    m,
		deps: Deps{
			RDB:              rdb,
			Bus:              b,
			Hub:              hub,
			Log:              quietLogger(),
			MessageRateLimit: ratelimit.DefaultMessageConfig(1000),
			JobRateLimit:     ratelimit.DefaultJobConfig(1000),
			Metrics:          m,
		},
	}
}

// send dispatches a frame and returns everything the connection replied.
func (h *harness) send(frame string) []map[string]any {
	h.t.Helper()
	before := len(h.conn.sendCh)
	_ = before
	Dispatch(context.Background(), h.conn, []byte(frame), h.deps)

	var out []map[string]any
	for {
		select {
		case f := <-h.conn.sendCh:
			var m map[string]any
			if err := json.Unmarshal(f.payload, &m); err != nil {
				h.t.Fatalf("reply was not JSON: %s", f.payload)
			}
			out = append(out, m)
		default:
			return out
		}
	}
}

// only asserts exactly one reply and returns it.
func (h *harness) only(frames []map[string]any) map[string]any {
	h.t.Helper()
	if len(frames) != 1 {
		h.t.Fatalf("got %d replies, want 1: %v", len(frames), frames)
	}
	return frames[0]
}

func TestDispatchRejectsMalformedInput(t *testing.T) {
	h := newHarness(t)

	t.Run("invalid JSON", func(t *testing.T) {
		got := h.only(h.send(`{not json`))
		if got["type"] != protocol.OutError {
			t.Fatalf("type = %v, want error", got["type"])
		}
		if !strings.Contains(got["message"].(string), "Invalid JSON") {
			t.Fatalf("message = %v", got["message"])
		}
	})

	t.Run("missing type", func(t *testing.T) {
		got := h.only(h.send(`{"topic":"chat"}`))
		if got["type"] != protocol.OutError {
			t.Fatalf("type = %v, want error", got["type"])
		}
	})

	t.Run("unknown type", func(t *testing.T) {
		got := h.only(h.send(`{"type":"teleport","topic":"chat"}`))
		if got["type"] != protocol.OutError {
			t.Fatalf("type = %v, want error", got["type"])
		}
	})

	// reqID must survive onto the error, or a client with several frames
	// in flight cannot tell which one failed.
	t.Run("reqID is echoed on errors", func(t *testing.T) {
		got := h.only(h.send(`{"type":"publish","reqID":"r9"}`))
		if got["type"] != protocol.OutError {
			t.Fatalf("type = %v, want error", got["type"])
		}
		if got["reqID"] != "r9" {
			t.Fatalf("reqID = %v, want r9", got["reqID"])
		}
	})
}

func TestDispatchSubscribeAndPublish(t *testing.T) {
	h := newHarness(t)

	got := h.only(h.send(`{"type":"subscribe","topic":"chat","reqID":"r1"}`))
	if got["type"] != protocol.OutSuccess {
		t.Fatalf("subscribe failed: %v", got)
	}
	if got["reqID"] != "r1" {
		t.Fatalf("reqID = %v, want r1", got["reqID"])
	}

	// The subscription must be registered in Redis, not just locally, or
	// presence and cross-instance routing are wrong.
	if !h.mr.Exists("topic:chat") {
		t.Fatal("subscribe did not register the topic membership in Redis")
	}

	got = h.only(h.send(`{"type":"publish","topic":"chat","data":{"hello":"world"},"reqID":"r2"}`))
	if got["type"] != protocol.OutSuccess {
		t.Fatalf("publish failed: %v", got)
	}

	// And the message must be in the replay stream.
	if !h.mr.Exists("topic:stream:chat") {
		t.Fatal("publish did not append to the topic stream")
	}
}

func TestDispatchUnsubscribe(t *testing.T) {
	h := newHarness(t)
	h.send(`{"type":"subscribe","topic":"chat"}`)

	got := h.only(h.send(`{"type":"unsubscribe","topic":"chat","reqID":"r3"}`))
	if got["type"] != protocol.OutSuccess {
		t.Fatalf("unsubscribe failed: %v", got)
	}
	members, _ := h.mr.SMembers("topic:chat")
	if len(members) != 0 {
		t.Fatalf("membership = %v, want empty after unsubscribe", members)
	}
}

func TestDispatchLockLifecycle(t *testing.T) {
	h := newHarness(t)

	for _, frame := range []string{
		`{"type":"lockTopic","topic":"chat","lockType":"publish","reqID":"l1"}`,
		`{"type":"renewLock","topic":"chat","lockType":"publish","reqID":"l2"}`,
		`{"type":"unlockTopic","topic":"chat","lockType":"publish","reqID":"l3"}`,
	} {
		got := h.only(h.send(frame))
		if got["type"] != protocol.OutSuccess {
			t.Fatalf("%s -> %v", frame, got)
		}
	}

	// An invalid lockType must be rejected by validation, not reach Redis.
	got := h.only(h.send(`{"type":"lockTopic","topic":"chat","lockType":"sideways"}`))
	if got["type"] != protocol.OutError {
		t.Fatalf("invalid lockType was accepted: %v", got)
	}
}

func TestDispatchLockBlocksAnotherHolder(t *testing.T) {
	h := newHarness(t)
	h.send(`{"type":"lockTopic","topic":"chat","lockType":"publish"}`)

	// A second identity must not be able to publish to a topic the first
	// has locked.
	other := NewConn(nil, ConnConfig{
		UserKey: "mallory", SendChanCapacity: 16, MaxBufferedBytes: 1 << 20, Metrics: h.m,
	}, quietLogger())
	Dispatch(context.Background(), other, []byte(`{"type":"publish","topic":"chat","data":{"x":1}}`), h.deps)

	select {
	case f := <-other.sendCh:
		var m map[string]any
		_ = json.Unmarshal(f.payload, &m)
		if m["type"] != protocol.OutError {
			t.Fatalf("publish to a topic locked by another holder succeeded: %v", m)
		}
		if !strings.Contains(m["message"].(string), "locked") {
			t.Fatalf("message = %v, want it to explain the lock", m["message"])
		}
	default:
		t.Fatal("no reply to a publish that should have been refused")
	}
}

func TestDispatchPresenceAndListSubscriptions(t *testing.T) {
	h := newHarness(t)
	h.send(`{"type":"subscribe","topic":"chat"}`)
	h.send(`{"type":"subscribe","topic":"alerts"}`)

	got := h.only(h.send(`{"type":"presence","topic":"chat","reqID":"p1"}`))
	if got["type"] != protocol.OutPresence {
		t.Fatalf("type = %v, want presence", got["type"])
	}
	if got["count"].(float64) != 1 {
		t.Fatalf("count = %v, want 1", got["count"])
	}

	got = h.only(h.send(`{"type":"listSubscriptions","reqID":"p2"}`))
	if got["type"] != protocol.OutSubscriptions {
		t.Fatalf("type = %v, want subscriptions", got["type"])
	}
	topics := got["topics"].([]any)
	if len(topics) != 2 {
		t.Fatalf("topics = %v, want 2", topics)
	}
}

func TestDispatchRateLimit(t *testing.T) {
	h := newHarness(t)
	h.deps.MessageRateLimit = ratelimit.DefaultMessageConfig(2)

	var limited int
	for i := 0; i < 6; i++ {
		got := h.only(h.send(`{"type":"subscribe","topic":"chat"}`))
		if got["type"] == protocol.OutError && strings.Contains(got["message"].(string), "Rate limit") {
			limited++
		}
	}
	if limited == 0 {
		t.Fatal("no frame was rate limited with a budget of 2 per second")
	}

	// A rate-limited frame must not be dispatched: the limit is only
	// meaningful if it prevents the work, not just the reply.
	if limited != 4 {
		t.Fatalf("limited %d of 6, want 4", limited)
	}
}

func TestDispatchBroadcastIsFireAndForget(t *testing.T) {
	h := newHarness(t)
	// The sender gets no reply — that is the documented contract, and a
	// client waiting on reqID would hang if it changed.
	if got := h.send(`{"type":"broadcast","data":{"x":1},"reqID":"b1"}`); len(got) != 0 {
		t.Fatalf("broadcast replied with %v, want nothing", got)
	}
}

func TestDispatchJobs(t *testing.T) {
	h := newHarness(t)

	executeAt := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	job := `{"type":"scheduleJob","reqID":"j1","jobData":{` +
		`"jobId":"job-1","apiEndpoint":"https://example.com/hook",` +
		`"method":"POST","executeAt":"` + executeAt + `"}}`
	got := h.only(h.send(job))
	if got["type"] != protocol.OutSuccess {
		t.Fatalf("scheduleJob failed: %v", got)
	}

	got = h.only(h.send(`{"type":"removeJob","jobId":"job-1","reqID":"j2"}`))
	if got["type"] != protocol.OutSuccess {
		t.Fatalf("removeJob failed: %v", got)
	}
}

// A job with invalid data must be refused by validation, before anything
// is written to Redis.
func TestDispatchRejectsInvalidJob(t *testing.T) {
	h := newHarness(t)

	for _, bad := range []string{
		`{"type":"scheduleJob","jobData":{"apiEndpoint":"https://x.test","method":"POST","executeAt":"2030-01-01T00:00:00Z"}}`,
		`{"type":"scheduleJob","jobData":{"jobId":"j","method":"POST","executeAt":"2030-01-01T00:00:00Z"}}`,
		`{"type":"scheduleJob","jobData":{"jobId":"j","apiEndpoint":"https://x.test","method":"TELEPORT","executeAt":"2030-01-01T00:00:00Z"}}`,
		`{"type":"scheduleJob","jobData":{"jobId":"j","apiEndpoint":"https://x.test","method":"POST","executeAt":"not-a-time"}}`,
	} {
		got := h.only(h.send(bad))
		if got["type"] != protocol.OutError {
			t.Fatalf("invalid job accepted: %s -> %v", bad, got)
		}
	}
}

func TestDispatchCustomVerb(t *testing.T) {
	h := newHarness(t)
	reg := handler.NewRegistry()
	reg.Register("echo", func(_ context.Context, req *handler.Request, _ handler.Responder) (string, error) {
		return "echoed " + req.Topic(), nil
	})
	h.deps.Registry = reg

	got := h.only(h.send(`{"type":"echo","topic":"anything","reqID":"c1"}`))
	if got["type"] != protocol.OutSuccess {
		t.Fatalf("custom verb -> %v", got)
	}
	if got["message"] != "echoed anything" {
		t.Fatalf("message = %v", got["message"])
	}
	if got["reqID"] != "c1" {
		t.Fatalf("reqID = %v, want c1", got["reqID"])
	}
}

// Built-in validation knows nothing about a custom verb's fields, so it
// must not reject one as unknown — but the version check still applies.
func TestDispatchCustomVerbBypassesFieldValidation(t *testing.T) {
	h := newHarness(t)
	reg := handler.NewRegistry()
	reg.Register("bare", func(context.Context, *handler.Request, handler.Responder) (string, error) {
		return "ok", nil
	})
	h.deps.Registry = reg

	// No topic, no data — fields the built-in verbs would require.
	got := h.only(h.send(`{"type":"bare"}`))
	if got["type"] != protocol.OutSuccess {
		t.Fatalf("custom verb was rejected by built-in validation: %v", got)
	}

	got = h.only(h.send(`{"type":"bare","version":99}`))
	if got["type"] != protocol.OutError {
		t.Fatalf("custom verb skipped the version check: %v", got)
	}
}

// A verb reaches a metric label, so the label set must stay bounded:
// registered verbs are known, everything else is "unknown".
func TestCustomVerbIsBoundedInMetrics(t *testing.T) {
	reg := handler.NewRegistry()
	reg.Register("myVerb", func(context.Context, *handler.Request, handler.Responder) (string, error) {
		return "", nil
	})

	if got := frameTypeLabel("myVerb", reg); got != "myVerb" {
		t.Fatalf("registered verb labelled %q", got)
	}
	if got := frameTypeLabel("../../etc/passwd", reg); got != "unknown" {
		t.Fatalf("junk verb labelled %q, want \"unknown\"", got)
	}
	if got := frameTypeLabel(protocol.TypePublish, reg); got != protocol.TypePublish {
		t.Fatalf("built-in verb labelled %q", got)
	}
}

func TestDispatchMiddleware(t *testing.T) {
	t.Run("wraps built-in verbs too", func(t *testing.T) {
		h := newHarness(t)
		var seen []string
		h.deps.Middleware = []handler.Middleware{
			func(next handler.Handler) handler.Handler {
				return func(ctx context.Context, req *handler.Request, rw handler.Responder) (string, error) {
					seen = append(seen, req.Type())
					return next(ctx, req, rw)
				}
			},
		}

		h.send(`{"type":"subscribe","topic":"chat"}`)
		if len(seen) != 1 || seen[0] != "subscribe" {
			t.Fatalf("middleware saw %v, want [subscribe]", seen)
		}
	})

	t.Run("a rejecting middleware stops the handler", func(t *testing.T) {
		h := newHarness(t)
		h.deps.Middleware = []handler.Middleware{
			func(handler.Handler) handler.Handler {
				return func(context.Context, *handler.Request, handler.Responder) (string, error) {
					return "", errors.New("blocked by policy")
				}
			},
		}

		got := h.only(h.send(`{"type":"subscribe","topic":"chat"}`))
		if got["type"] != protocol.OutError {
			t.Fatalf("rejected frame -> %v", got)
		}
		if !strings.Contains(got["message"].(string), "blocked by policy") {
			t.Fatalf("message = %v", got["message"])
		}
		// The subscription must not have happened.
		if h.mr.Exists("topic:chat") {
			t.Fatal("the handler ran despite the middleware rejecting the frame")
		}
	})

	t.Run("identity reaches the handler", func(t *testing.T) {
		h := newHarness(t)
		var got *handler.Request
		reg := handler.NewRegistry()
		reg.Register("inspect", func(_ context.Context, req *handler.Request, _ handler.Responder) (string, error) {
			got = req
			return "ok", nil
		})
		h.deps.Registry = reg

		h.send(`{"type":"inspect"}`)
		if got == nil {
			t.Fatal("handler did not run")
		}
		if got.UserKey != "alice" {
			t.Fatalf("UserKey = %q, want alice", got.UserKey)
		}
		if got.ConnID != h.conn.SubscriberID() {
			t.Fatalf("ConnID = %q, want the connection's id", got.ConnID)
		}
	})
}

// The point of server-side cursors: a client that reconnects with
// since:"ack" resumes where it left off without having to remember a
// stream ID across restarts.
func TestDispatchAckCursor(t *testing.T) {
	h := newHarness(t)

	h.send(`{"type":"subscribe","topic":"chat"}`)
	for i := 0; i < 3; i++ {
		got := h.only(h.send(`{"type":"publish","topic":"chat","data":{"n":1}}`))
		if got["type"] != protocol.OutSuccess {
			t.Fatalf("publish failed: %v", got)
		}
	}

	// Find a real stream ID to acknowledge.
	ids, err := h.deps.RDB.XRange(context.Background(), "topic:stream:chat", "-", "+").Result()
	if err != nil || len(ids) < 3 {
		t.Fatalf("XRange: %v (%d entries)", err, len(ids))
	}
	first := ids[0].ID

	got := h.only(h.send(`{"type":"ack","topic":"chat","streamId":"` + first + `","reqID":"a1"}`))
	if got["type"] != protocol.OutSuccess {
		t.Fatalf("ack failed: %v", got)
	}

	stored, err := redisx.AckCursor(context.Background(), h.deps.RDB, "alice", "chat")
	if err != nil {
		t.Fatalf("AckCursor: %v", err)
	}
	if stored != first {
		t.Fatalf("cursor = %q, want %q", stored, first)
	}

	// Resubscribing with since:"ack" must replay only what follows the
	// cursor: two of the three messages.
	h.send(`{"type":"unsubscribe","topic":"chat"}`)
	frames := h.send(`{"type":"subscribe","topic":"chat","since":"ack","reqID":"s2"}`)

	var replayed int
	for _, f := range frames {
		if f["type"] == protocol.OutPublishDelivery {
			replayed++
		}
	}
	if replayed != 2 {
		t.Fatalf("replayed %d messages, want 2 (everything after the acked one)", replayed)
	}
}

// With nothing acked there is no cursor, and since:"ack" must behave like
// a plain subscribe rather than replaying the whole stream or erroring.
func TestDispatchAckWithNoCursor(t *testing.T) {
	h := newHarness(t)
	h.send(`{"type":"publish","topic":"chat","data":{"n":1}}`)

	frames := h.send(`{"type":"subscribe","topic":"chat","since":"ack","reqID":"s1"}`)
	for _, f := range frames {
		if f["type"] == protocol.OutPublishDelivery {
			t.Fatalf("an unacked subscriber was sent history: %v", f)
		}
	}
	if len(frames) != 1 || frames[0]["type"] != protocol.OutSuccess {
		t.Fatalf("frames = %v, want a single success", frames)
	}
}

func TestDispatchAckValidation(t *testing.T) {
	h := newHarness(t)
	for _, bad := range []string{
		`{"type":"ack","streamId":"1-0"}`,
		`{"type":"ack","topic":"chat"}`,
		`{"type":"ack"}`,
	} {
		got := h.only(h.send(bad))
		if got["type"] != protocol.OutError {
			t.Fatalf("%s was accepted: %v", bad, got)
		}
	}
}
