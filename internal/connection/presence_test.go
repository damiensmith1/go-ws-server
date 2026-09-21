package connection

import (
	"testing"

	"github.com/damiensmith1/go-ws-server/metrics"
)

// A user with several sockets is present once, not once per socket. Add
// and Remove report the transitions so a presence feed can be built from
// them without counting connections itself.
func TestHubReportsFirstAndLast(t *testing.T) {
	h := NewHub(quietLogger())
	m := metrics.New()

	a := newTestConn(t, m, ConnConfig{UserKey: "alice"})
	b := newTestConn(t, m, ConnConfig{UserKey: "alice"})
	a.userKey, b.userKey = "alice", "alice"

	if first := h.Add(a); !first {
		t.Fatal("first socket for a userKey was not reported as first")
	}
	if first := h.Add(b); first {
		t.Fatal("second socket for the same userKey was reported as first")
	}
	if last := h.Remove(b); last {
		t.Fatal("removing one of two sockets was reported as last")
	}
	if last := h.Remove(a); !last {
		t.Fatal("removing the final socket was not reported as last")
	}
}

// A presence feed is an observation of the system. It must never be able
// to fail a connection or a disconnection, and it must stay off unless
// explicitly configured.
func TestPublishPresenceIsSafeWhenDisabled(t *testing.T) {
	t.Run("no topic configured is a no-op", func(t *testing.T) {
		// Bus is nil: if this tried to publish, it would panic.
		PublishPresence(t.Context(), "alice", PresenceConnected, Deps{Log: quietLogger()})
	})

	t.Run("a topic with no bus is a no-op", func(t *testing.T) {
		PublishPresence(t.Context(), "alice", PresenceConnected,
			Deps{Log: quietLogger(), PresenceTopic: "_presence"})
	})
}
