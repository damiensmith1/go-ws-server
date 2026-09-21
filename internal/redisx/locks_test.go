package redisx

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newMini(t *testing.T) *redis.Client {
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
	return c
}

func TestLockTopic_Acquire(t *testing.T) {
	c := newMini(t)
	ctx := context.Background()
	if err := LockTopic(ctx, c, "chat", LockPublish, "alice", 0); err != nil {
		t.Fatal(err)
	}
	// Second call by same holder is idempotent
	if err := LockTopic(ctx, c, "chat", LockPublish, "alice", 0); err != nil {
		t.Fatalf("same-holder reacquire: %v", err)
	}
}

func TestLockTopic_HeldByOther(t *testing.T) {
	c := newMini(t)
	ctx := context.Background()
	if err := LockTopic(ctx, c, "chat", LockPublish, "alice", 0); err != nil {
		t.Fatal(err)
	}
	err := LockTopic(ctx, c, "chat", LockPublish, "bob", 0)
	if !errors.Is(err, ErrLockHeldByOther) {
		t.Fatalf("expected ErrLockHeldByOther, got %v", err)
	}
}

func TestUnlock_OnlyByOwner(t *testing.T) {
	c := newMini(t)
	ctx := context.Background()
	_ = LockTopic(ctx, c, "chat", LockPublish, "alice", 0)
	// Bob trying to unlock alice's lock is a no-op (silent), per Lua check.
	if err := UnlockTopic(ctx, c, "chat", LockPublish, "bob"); err != nil {
		t.Fatalf("bob's unlock attempt should not error: %v", err)
	}
	// Lock should still belong to alice.
	err := LockTopic(ctx, c, "chat", LockPublish, "carol", 0)
	if !errors.Is(err, ErrLockHeldByOther) {
		t.Fatalf("expected lock still held by alice, got %v", err)
	}

	// Owner unlock works.
	if err := UnlockTopic(ctx, c, "chat", LockPublish, "alice"); err != nil {
		t.Fatal(err)
	}
	// And now anyone can lock.
	if err := LockTopic(ctx, c, "chat", LockPublish, "carol", 0); err != nil {
		t.Fatalf("after alice unlocked, carol should acquire: %v", err)
	}
}

func TestRenewLock_NotHeld(t *testing.T) {
	c := newMini(t)
	ctx := context.Background()
	err := RenewLock(ctx, c, "ghost", LockPublish, "alice", 0)
	if !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("expected ErrLockNotHeld, got %v", err)
	}
}

func TestRenewLock_OK(t *testing.T) {
	c := newMini(t)
	ctx := context.Background()
	_ = LockTopic(ctx, c, "chat", LockPublish, "alice", 0)
	if err := RenewLock(ctx, c, "chat", LockPublish, "alice", 0); err != nil {
		t.Fatal(err)
	}
}

func TestIsTopicLockedByOther(t *testing.T) {
	c := newMini(t)
	ctx := context.Background()
	other, err := IsTopicLockedByOther(ctx, c, "chat", LockPublish, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if other {
		t.Fatal("nothing locked yet")
	}
	_ = LockTopic(ctx, c, "chat", LockPublish, "alice", 0)
	other, _ = IsTopicLockedByOther(ctx, c, "chat", LockPublish, "alice")
	if other {
		t.Fatal("alice owns the lock")
	}
	other, _ = IsTopicLockedByOther(ctx, c, "chat", LockPublish, "bob")
	if !other {
		t.Fatal("bob should see lock as held by other")
	}
}

func TestAckCursor(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { c.Close() })
	ctx := context.Background()

	t.Run("absent cursor is empty, not an error", func(t *testing.T) {
		got, err := AckCursor(ctx, c, "alice", "chat")
		if err != nil {
			t.Fatalf("AckCursor: %v", err)
		}
		if got != "" {
			t.Fatalf("cursor = %q, want empty", got)
		}
	})

	t.Run("round trips", func(t *testing.T) {
		if err := SetAckCursor(ctx, c, "alice", "chat", "100-0", time.Hour); err != nil {
			t.Fatalf("SetAckCursor: %v", err)
		}
		got, _ := AckCursor(ctx, c, "alice", "chat")
		if got != "100-0" {
			t.Fatalf("cursor = %q, want 100-0", got)
		}
	})

	// Redelivery means a client can legitimately re-ack something it has
	// already acked. Rewinding would replay everything after it again.
	t.Run("does not move backwards", func(t *testing.T) {
		_ = SetAckCursor(ctx, c, "bob", "chat", "500-0", time.Hour)
		_ = SetAckCursor(ctx, c, "bob", "chat", "200-0", time.Hour)
		got, _ := AckCursor(ctx, c, "bob", "chat")
		if got != "500-0" {
			t.Fatalf("cursor = %q, want it to stay at 500-0", got)
		}
	})

	t.Run("cursors are per topic", func(t *testing.T) {
		_ = SetAckCursor(ctx, c, "carol", "a", "1-0", time.Hour)
		_ = SetAckCursor(ctx, c, "carol", "b", "2-0", time.Hour)
		a, _ := AckCursor(ctx, c, "carol", "a")
		b, _ := AckCursor(ctx, c, "carol", "b")
		if a != "1-0" || b != "2-0" {
			t.Fatalf("cursors bled between topics: a=%q b=%q", a, b)
		}
	})
}

// Lexicographic comparison would put "10-0" before "9-0" and treat a
// newer cursor as older, replaying history the client already saw.
func TestStreamIDLess(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"9-0", "10-0", true},
		{"10-0", "9-0", false},
		{"100-0", "100-1", true},
		{"100-1", "100-0", false},
		{"100-0", "100-0", false},
		{"1699999999999-0", "1700000000000-0", true},
	}
	for _, tc := range tests {
		if got := StreamIDLess(tc.a, tc.b); got != tc.want {
			t.Errorf("StreamIDLess(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}
