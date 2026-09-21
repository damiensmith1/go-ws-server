package connection

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/damiensmith1/go-ws-server/bus"
	"github.com/damiensmith1/go-ws-server/internal/ratelimit"
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
