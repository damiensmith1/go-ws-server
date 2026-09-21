package connection

import (
	"context"
	"sync"
	"testing"

	"github.com/damiensmith1/go-ws-server/handler"
	"github.com/damiensmith1/go-ws-server/metrics"
)

// An embedder keeping per-connection state outside the server — a
// subscriber's routing criteria, say — has no other way to learn that a
// socket closed. Without this, that state accumulates forever and every
// later operation pays for connections that no longer exist.
func TestOnDisconnectFires(t *testing.T) {
	h := newHarness(t)
	h.conn.claims = map[string]any{"role": "admin"}

	var (
		mu     sync.Mutex
		seen   []handler.Conn
		called bool
	)
	h.deps.OnDisconnect = func(_ context.Context, c handler.Conn) {
		mu.Lock()
		defer mu.Unlock()
		called = true
		seen = append(seen, c)
	}

	Cleanup(context.Background(), h.conn, h.deps, true)

	mu.Lock()
	defer mu.Unlock()
	if !called {
		t.Fatal("OnDisconnect was never called")
	}
	if len(seen) != 1 {
		t.Fatalf("called %d times, want 1", len(seen))
	}
	got := seen[0]
	if got.ConnID != h.conn.SubscriberID() {
		t.Fatalf("ConnID = %q, want the connection's id", got.ConnID)
	}
	if got.UserKey != "alice" {
		t.Fatalf("UserKey = %q, want alice", got.UserKey)
	}
	// Identity and claims must still be readable: a callback that cannot
	// tell which tenant's state to drop is useless.
	if got.Claims["role"] != "admin" {
		t.Fatalf("Claims = %#v, want the credential's claims", got.Claims)
	}
}

// The hook is optional; a nil value must not be called.
func TestOnDisconnectNilIsSafe(t *testing.T) {
	h := newHarness(t)
	Cleanup(context.Background(), h.conn, h.deps, true)
}

// It must fire for every socket, not only the last one for a userKey.
// Per-connection state is per connection; the presence feed's
// first/last semantics are the wrong granularity for it.
func TestOnDisconnectFiresForEverySocket(t *testing.T) {
	h := newHarness(t)

	var mu sync.Mutex
	var count int
	h.deps.OnDisconnect = func(context.Context, handler.Conn) {
		mu.Lock()
		count++
		mu.Unlock()
	}

	second := NewConn(nil, ConnConfig{
		UserKey: "alice", SendChanCapacity: 8, MaxBufferedBytes: 1 << 20, Metrics: metrics.New(),
	}, quietLogger())

	Cleanup(context.Background(), second, h.deps, false) // not the last socket
	Cleanup(context.Background(), h.conn, h.deps, true)  // the last socket

	mu.Lock()
	defer mu.Unlock()
	if count != 2 {
		t.Fatalf("called %d times, want 2: every socket must be reported", count)
	}
}
