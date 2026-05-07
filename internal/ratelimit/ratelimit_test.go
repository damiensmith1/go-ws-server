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

func TestAllow_WindowResets(t *testing.T) {
	c, mr := newMini(t)
	cfg := Config{Window: time.Second, MaxRequests: 1}

	ok, _ := Allow(context.Background(), c, "msg", "alice", cfg)
	if !ok {
		t.Fatal("first should pass")
	}
	ok, _ = Allow(context.Background(), c, "msg", "alice", cfg)
	if ok {
		t.Fatal("second within window should block")
	}
	mr.FastForward(2 * time.Second)
	ok, _ = Allow(context.Background(), c, "msg", "alice", cfg)
	if !ok {
		t.Fatal("after window expiry should pass again")
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
