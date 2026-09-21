package bus

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"

	"github.com/damiensmith1/go-ws-server/metrics"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

type fakeSub struct {
	mu  sync.Mutex
	got [][]byte
}

func (f *fakeSub) Send(msg []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]byte, len(msg))
	copy(cp, msg)
	f.got = append(f.got, cp)
}

func (f *fakeSub) Snapshot() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]byte, len(f.got))
	copy(out, f.got)
	return out
}

type fakeBroadcast struct{}

func (fakeBroadcast) SendToConn(string, []byte) {}

func (fakeBroadcast) SendToUser(userKey string, msg []byte) {}

func newBus(t *testing.T) (*Bus, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mr.Close() })
	pub := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	sub := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		pub.Close()
		sub.Close()
	})
	b := New(pub, sub, fakeBroadcast{}, Config{StreamMaxLength: 100}, quietLogger())
	return b, mr
}

func TestPublishTopic_AddsToStream(t *testing.T) {
	b, _ := newBus(t)
	id, err := b.PublishTopic(context.Background(), "chat", json.RawMessage(`{"x":1}`), "")
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("expected a stream id")
	}
}

func TestSubscribeWithReplay_DrainsHistory(t *testing.T) {
	b, _ := newBus(t)
	ctx := context.Background()

	id1, _ := b.PublishTopic(ctx, "chat", json.RawMessage(`{"n":1}`), "")
	id2, _ := b.PublishTopic(ctx, "chat", json.RawMessage(`{"n":2}`), "")
	_ = id1

	sub := &fakeSub{}
	truncated, _, err := b.SubscribeWithReplay(ctx, sub, "chat", "0-0")
	if err != nil {
		t.Fatal(err)
	}
	if truncated {
		t.Fatal("since=0-0 must never report truncation")
	}

	got := sub.Snapshot()
	if len(got) != 2 {
		t.Fatalf("want 2 frames, got %d (%v)", len(got), framesAsStrings(got))
	}

	// Both should be marked replay=true.
	for i, raw := range got {
		var f map[string]any
		_ = json.Unmarshal(raw, &f)
		if f["type"] != "publish" {
			t.Fatalf("frame %d: type=%v", i, f["type"])
		}
		if f["replay"] != true {
			t.Fatalf("frame %d: replay=%v", i, f["replay"])
		}
	}

	// And the second frame should be the second message.
	var f2 map[string]any
	_ = json.Unmarshal(got[1], &f2)
	if f2["streamId"] != id2 {
		t.Fatalf("frame 2 streamId=%v want %s", f2["streamId"], id2)
	}
}

func TestSubscribeWithReplay_SinceCursorExclusive(t *testing.T) {
	b, _ := newBus(t)
	ctx := context.Background()

	id1, _ := b.PublishTopic(ctx, "chat", json.RawMessage(`{"n":1}`), "")
	id2, _ := b.PublishTopic(ctx, "chat", json.RawMessage(`{"n":2}`), "")

	sub := &fakeSub{}
	if _, _, err := b.SubscribeWithReplay(ctx, sub, "chat", id1); err != nil {
		t.Fatal(err)
	}

	got := sub.Snapshot()
	if len(got) != 1 {
		t.Fatalf("want 1 frame (only id2), got %d", len(got))
	}
	var f map[string]any
	_ = json.Unmarshal(got[0], &f)
	if f["streamId"] != id2 {
		t.Fatalf("expected id2, got %v", f["streamId"])
	}
}

func TestParseSinceCursor(t *testing.T) {
	cases := []struct {
		in  string
		out string
		err bool
	}{
		{"100-0", "100-0", false},
		{"12345", "12345-0", false},
		{"2026-05-01T00:00:00Z", "", false}, // varies — just check no error
		{"not a date", "", true},
		{"", "", true},
	}
	for _, tc := range cases {
		got, err := parseSinceCursor(tc.in)
		if tc.err {
			if err == nil {
				t.Errorf("%q: expected error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: unexpected error: %v", tc.in, err)
			continue
		}
		if tc.out != "" && got != tc.out {
			t.Errorf("%q: got %q want %q", tc.in, got, tc.out)
		}
	}
}

func TestCompareStreamIDs(t *testing.T) {
	if compareStreamIDs("100-2", "100-10") >= 0 {
		t.Error("100-2 should be < 100-10")
	}
	if compareStreamIDs("99-9", "100-0") >= 0 {
		t.Error("99-9 should be < 100-0")
	}
	if compareStreamIDs("100-5", "100-5") != 0 {
		t.Error("equal ids")
	}
}

// reduce flakiness: framesAsStrings prints the captured frames for debug
// output if assertions fail.
func framesAsStrings(b [][]byte) []string {
	out := make([]string, len(b))
	for i, x := range b {
		out[i] = string(x)
	}
	return out
}

// Sanity: bus.Run can be started and cancelled cleanly.
func TestBus_RunAndCancel(t *testing.T) {
	b, _ := newBus(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()

	// Give it a beat to subscribe.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run err: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	_ = b.Close()
}

// handleTopic decides delivery and replay-buffering for every subscriber in
// a single critical section. This covers the mixed case: subscribers mid-
// replay must be buffered while the rest are delivered in the same pass.
func TestHandleTopic_BuffersMidReplayAndDeliversRest(t *testing.T) {
	b, _ := newBus(t)

	live1, live2, replaying := &fakeSub{}, &fakeSub{}, &fakeSub{}
	for _, s := range []Subscriber{live1, live2, replaying} {
		b.AddLocalSubscription("t", s)
	}

	// Put one subscriber mid-replay, as SubscribeWithReplay would.
	b.mu.Lock()
	b.replay[replayKey{replaying, "t"}] = &replayState{
		buffering: true,
		seen:      make(map[string]bool),
	}
	b.mu.Unlock()

	payload, _ := json.Marshal(wireTopicPayload{
		StreamID: "5-0",
		Data:     json.RawMessage(`{"n":1}`),
	})
	b.handleTopic("t", string(payload))

	for name, s := range map[string]*fakeSub{"live1": live1, "live2": live2} {
		if got := s.Snapshot(); len(got) != 1 {
			t.Fatalf("%s: want 1 frame, got %d", name, len(got))
		}
	}
	if got := replaying.Snapshot(); len(got) != 0 {
		t.Fatalf("replaying subscriber: want 0 frames, got %d", len(got))
	}

	b.mu.Lock()
	buffered := b.replay[replayKey{replaying, "t"}].buf
	b.mu.Unlock()
	if len(buffered) != 1 || buffered[0].streamID != "5-0" {
		t.Fatalf("want 1 buffered msg with streamID 5-0, got %+v", buffered)
	}

	// Both live subscribers must receive byte-identical frames: handleTopic
	// encodes once and shares the slice.
	if string(live1.Snapshot()[0]) != string(live2.Snapshot()[0]) {
		t.Fatal("live subscribers received differing frames")
	}
}

func TestHandleTopic_RecordsFanoutMetrics(t *testing.T) {
	m := metrics.New()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	pub := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	sub := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { pub.Close(); sub.Close() })
	b := New(pub, sub, fakeBroadcast{}, Config{StreamMaxLength: 100, Metrics: m}, quietLogger())

	live, replaying := &fakeSub{}, &fakeSub{}
	b.AddLocalSubscription("t", live)
	b.AddLocalSubscription("t", replaying)

	if got := testutil.ToFloat64(m.LocalSubscriptions); got != 2 {
		t.Fatalf("got %v subscriptions, want 2", got)
	}

	b.mu.Lock()
	b.replay[replayKey{replaying, "t"}] = &replayState{buffering: true, seen: map[string]bool{}}
	b.mu.Unlock()

	payload, _ := json.Marshal(wireTopicPayload{StreamID: "1-0", Data: json.RawMessage(`{}`)})
	b.handleTopic("t", string(payload))

	if got := testutil.ToFloat64(m.FanoutBuffered); got != 1 {
		t.Fatalf("got %v buffered, want 1", got)
	}
	if got := testutil.CollectAndCount(m.FanoutDuration); got != 1 {
		t.Fatalf("fan-out duration not observed")
	}

	// Removing a subscription must give the gauge its slot back, and doing
	// it twice must not double-count.
	b.RemoveLocalSubscription("t", live)
	b.RemoveLocalSubscription("t", live)
	if got := testutil.ToFloat64(m.LocalSubscriptions); got != 1 {
		t.Fatalf("got %v subscriptions after removal, want 1", got)
	}
}
