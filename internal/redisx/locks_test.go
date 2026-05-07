package redisx

import (
	"context"
	"errors"
	"testing"

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
