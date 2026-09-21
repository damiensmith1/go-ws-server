package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newMini(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		c.Close()
		mr.Close()
	})
	return c, mr
}

func TestAllow_WithinLimit(t *testing.T) {
	c, _ := newMini(t)
	cfg := Config{Window: time.Second, MaxRequests: 3}
	for i := 0; i < 3; i++ {
		ok, err := Allow(context.Background(), c, "msg", "alice", cfg)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("hit %d should be allowed", i)
		}
	}
}

func TestAllow_Exceeds(t *testing.T) {
	c, _ := newMini(t)
	cfg := Config{Window: time.Second, MaxRequests: 2}
	for i := 0; i < 2; i++ {
		ok, _ := Allow(context.Background(), c, "msg", "alice", cfg)
		if !ok {
			t.Fatalf("first %d should pass", i)
		}
	}
	ok, err := Allow(context.Background(), c, "msg", "alice", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("third hit should be blocked")
	}
}

// fakeClock pins nowFn so window behaviour can be driven exactly. The
// sliding window is scored by the timestamps we pass to Redis, so
// miniredis.FastForward (which only drives key TTLs) cannot move it.
func fakeClock(t *testing.T, mr *miniredis.Miniredis) func(time.Duration) {
	t.Helper()
	cur := time.Unix(1700000000, 0)
	nowFn = func() time.Time { return cur }
	t.Cleanup(func() { nowFn = time.Now })
	return func(d time.Duration) {
		cur = cur.Add(d)
		mr.FastForward(d) // keep key TTLs in step with the scored timestamps
	}
}

func TestAllow_WindowResets(t *testing.T) {
	c, mr := newMini(t)
	advance := fakeClock(t, mr)
	cfg := Config{Window: time.Second, MaxRequests: 1}

	ok, _ := Allow(context.Background(), c, "msg", "alice", cfg)
	if !ok {
		t.Fatal("first should pass")
	}
	ok, _ = Allow(context.Background(), c, "msg", "alice", cfg)
	if ok {
		t.Fatal("second within window should block")
	}
	advance(2 * time.Second)
	ok, _ = Allow(context.Background(), c, "msg", "alice", cfg)
	if !ok {
		t.Fatal("after window expiry should pass again")
	}
}

// The case a fixed window gets wrong: spend the allowance at the end of
// one window and again at the start of the next, and a 2/sec limit admits
// 3 requests inside 30ms. A sliding window counts over any 1s span.
func TestAllow_NoBurstAcrossWindowBoundary(t *testing.T) {
	c, mr := newMini(t)
	advance := fakeClock(t, mr)
	cfg := Config{Window: time.Second, MaxRequests: 2}
	allow := func() bool {
		ok, err := Allow(context.Background(), c, "msg", "alice", cfg)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}

	if !allow() { // t=0
		t.Fatal("t=0 should pass")
	}
	advance(990 * time.Millisecond)
	if !allow() { // t=0.99, second of two
		t.Fatal("t=0.99 should pass")
	}
	advance(20 * time.Millisecond)
	if !allow() { // t=1.01, the t=0 entry has aged out
		t.Fatal("t=1.01 should pass: the t=0 request left the window")
	}
	advance(10 * time.Millisecond)
	if allow() { // t=1.02 — t=0.99 and t=1.01 are both still in window
		t.Fatal("t=1.02 should block: 3 requests inside one second")
	}
}

func TestAllow_EntriesExpireIndividually(t *testing.T) {
	c, mr := newMini(t)
	advance := fakeClock(t, mr)
	cfg := Config{Window: time.Second, MaxRequests: 2}

	_, _ = Allow(context.Background(), c, "msg", "alice", cfg) // t=0
	advance(500 * time.Millisecond)
	_, _ = Allow(context.Background(), c, "msg", "alice", cfg) // t=0.5

	advance(600 * time.Millisecond) // t=1.1: only the t=0 entry has aged out
	ok, _ := Allow(context.Background(), c, "msg", "alice", cfg)
	if !ok {
		t.Fatal("one slot freed at t=1.1, should pass")
	}
	ok, _ = Allow(context.Background(), c, "msg", "alice", cfg)
	if ok {
		t.Fatal("t=0.5 entry still in window, should block")
	}
}

func TestAllow_BucketsIndependent(t *testing.T) {
	c, _ := newMini(t)
	cfg := Config{Window: time.Second, MaxRequests: 1}
	_, _ = Allow(context.Background(), c, "msg", "alice", cfg)
	ok, _ := Allow(context.Background(), c, "job", "alice", cfg)
	if !ok {
		t.Fatal("different bucket should not share limit")
	}
}
