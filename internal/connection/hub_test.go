package connection

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/damiensmith1/go-ws-server/internal/metrics"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// newTestConn builds a Conn with no underlying socket. Send, Close and the
// buffer accounting never touch c.ws — only the writer goroutine does, and
// these tests never start it.
func newTestConn(t *testing.T, m *metrics.Metrics, cfg ConnConfig) *Conn {
	t.Helper()
	cfg.UserKey = "alice"
	cfg.Metrics = m
	return NewConn(nil, cfg, quietLogger())
}

func TestNewConn_TracksActiveConnections(t *testing.T) {
	m := metrics.New()
	newTestConn(t, m, ConnConfig{})
	if got := testutil.ToFloat64(m.ConnectionsActive); got != 1 {
		t.Fatalf("got %v active, want 1", got)
	}
}

func TestSend_CountsDelivered(t *testing.T) {
	m := metrics.New()
	c := newTestConn(t, m, ConnConfig{SendChanCapacity: 4, MaxBufferedBytes: 1 << 20})

	c.Send([]byte("hello"))
	c.Send([]byte("world"))

	if got := testutil.ToFloat64(m.FramesSent); got != 2 {
		t.Fatalf("got %v sent, want 2", got)
	}
	if got := testutil.CollectAndCount(m.FramesDropped); got != 0 {
		t.Fatalf("got %v drop series, want none", got)
	}
}

func TestSend_DropsOverBufferThreshold(t *testing.T) {
	m := metrics.New()
	c := newTestConn(t, m, ConnConfig{SendChanCapacity: 8, MaxBufferedBytes: 10})

	c.Send([]byte("12345678"))  // 8 bytes, fits
	c.Send([]byte("123456789")) // would exceed 10, dropped

	if got := testutil.ToFloat64(m.FramesDropped.WithLabelValues(metrics.DropBufferThreshold)); got != 1 {
		t.Fatalf("got %v buffer-threshold drops, want 1", got)
	}
	if got := testutil.ToFloat64(m.FramesSent); got != 1 {
		t.Fatalf("got %v sent, want 1", got)
	}
}

func TestSend_DropsOnFullChannel(t *testing.T) {
	m := metrics.New()
	// Capacity 1 with no writer draining it: the second Send finds the
	// channel full. The buffer threshold is high so it cannot be the cause.
	c := newTestConn(t, m, ConnConfig{SendChanCapacity: 1, MaxBufferedBytes: 1 << 20})

	c.Send([]byte("a"))
	c.Send([]byte("b"))

	if got := testutil.ToFloat64(m.FramesDropped.WithLabelValues(metrics.DropChannelFull)); got != 1 {
		t.Fatalf("got %v channel-full drops, want 1", got)
	}
}

func TestClose_RecordsCategory(t *testing.T) {
	m := metrics.New()
	c := newTestConn(t, m, ConnConfig{SendChanCapacity: 4, MaxBufferedBytes: 1 << 20})

	c.closeWith(1000, "idle timeout", metrics.CloseIdleTimeout)
	// Close is idempotent; the first caller's category must win.
	c.Close(1000, "later")

	if c.closeReason != metrics.CloseIdleTimeout {
		t.Fatalf("got %q, want %q", c.closeReason, metrics.CloseIdleTimeout)
	}
}

func TestExpiryWatcher(t *testing.T) {
	// The watcher must close the socket, and must label the close as an
	// expiry rather than folding it into the generic "server" bucket —
	// operators need to tell credential churn apart from restarts.
	t.Run("closes once the deadline passes", func(t *testing.T) {
		m := metrics.New()
		c := newTestConn(t, m, ConnConfig{
			SendChanCapacity: 4,
			MaxBufferedBytes: 1 << 20,
			ExpiresAt:        time.Now().Add(20 * time.Millisecond),
		})
		go c.runExpiryWatcher(context.Background())

		select {
		case <-c.closed:
		case <-time.After(2 * time.Second):
			t.Fatal("connection still open well past the credential deadline")
		}
		if c.closeReason != metrics.CloseTokenExpired {
			t.Fatalf("closeReason = %q, want %q", c.closeReason, metrics.CloseTokenExpired)
		}
	})

	t.Run("a deadline already in the past closes immediately", func(t *testing.T) {
		m := metrics.New()
		c := newTestConn(t, m, ConnConfig{
			SendChanCapacity: 4,
			MaxBufferedBytes: 1 << 20,
			ExpiresAt:        time.Now().Add(-time.Minute),
		})
		c.runExpiryWatcher(context.Background())

		select {
		case <-c.closed:
		default:
			t.Fatal("want an already-expired credential to be refused, not honoured")
		}
	})

	// Zero means "never expires": starting a timer would close every
	// connection authorized by a non-expiring credential.
	t.Run("no deadline means the watcher returns without closing", func(t *testing.T) {
		m := metrics.New()
		c := newTestConn(t, m, ConnConfig{SendChanCapacity: 4, MaxBufferedBytes: 1 << 20})
		c.runExpiryWatcher(context.Background())

		select {
		case <-c.closed:
			t.Fatal("connection closed despite having no credential deadline")
		default:
		}
	})

	t.Run("context cancellation stops the watcher", func(t *testing.T) {
		m := metrics.New()
		c := newTestConn(t, m, ConnConfig{
			SendChanCapacity: 4,
			MaxBufferedBytes: 1 << 20,
			ExpiresAt:        time.Now().Add(time.Hour),
		})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { c.runExpiryWatcher(ctx); close(done) }()
		cancel()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("watcher ignored context cancellation")
		}
		select {
		case <-c.closed:
			t.Fatal("shutdown must not be reported as a token expiry")
		default:
		}
	})
}

func TestWaitDrained(t *testing.T) {
	t.Run("reports failure when the writer never runs", func(t *testing.T) {
		c := newTestConn(t, metrics.New(), ConnConfig{SendChanCapacity: 4, MaxBufferedBytes: 1 << 20})
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		if c.WaitDrained(ctx) {
			t.Fatal("reported drained although no writer ever consumed the queue")
		}
	})

	t.Run("reports success once the writer returns", func(t *testing.T) {
		c := newTestConn(t, metrics.New(), ConnConfig{SendChanCapacity: 4, MaxBufferedBytes: 1 << 20})
		close(c.drained) // stand in for runWriter returning

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if !c.WaitDrained(ctx) {
			t.Fatal("drained connection reported as stuck")
		}
	})

	// Shutdown must be able to say how much it failed to flush rather than
	// claim a clean stop it did not achieve.
	t.Run("hub counts the connections that did not drain", func(t *testing.T) {
		h := NewHub(quietLogger())
		drained := newTestConn(t, metrics.New(), ConnConfig{SendChanCapacity: 4, MaxBufferedBytes: 1 << 20})
		close(drained.drained)
		stuck := newTestConn(t, metrics.New(), ConnConfig{SendChanCapacity: 4, MaxBufferedBytes: 1 << 20})

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		if got := h.WaitDrained(ctx, []*Conn{drained, stuck}); got != 1 {
			t.Fatalf("stuck count = %d, want 1", got)
		}
	})
}

func TestSlowConsumerEviction(t *testing.T) {
	// A wedged peer must not keep its socket forever. The channel is size
	// 1 and nothing consumes it, so every send after the first drops.
	t.Run("evicts after the consecutive-drop budget", func(t *testing.T) {
		m := metrics.New()
		c := newTestConn(t, m, ConnConfig{
			SendChanCapacity:    1,
			MaxBufferedBytes:    1 << 20,
			MaxConsecutiveDrops: 3,
		})
		for i := 0; i < 10; i++ {
			c.Send([]byte(`{"x":1}`))
		}

		select {
		case <-c.closed:
		default:
			t.Fatal("slow consumer kept its connection past the drop budget")
		}
		if c.closeReason != metrics.CloseSlowConsumer {
			t.Fatalf("closeReason = %q, want %q", c.closeReason, metrics.CloseSlowConsumer)
		}
	})

	// The budget is consecutive, not cumulative: an otherwise healthy
	// connection that drops the odd frame under a burst must survive, or
	// every long-lived connection is eventually evicted.
	t.Run("a successful send resets the run", func(t *testing.T) {
		m := metrics.New()
		c := newTestConn(t, m, ConnConfig{
			SendChanCapacity:    1,
			MaxBufferedBytes:    1 << 20,
			MaxConsecutiveDrops: 3,
		})

		for i := 0; i < 20; i++ {
			c.Send([]byte(`{"x":1}`)) // first fills the channel, rest drop
			<-c.sendCh                // peer catches up
			if i%2 == 0 {
				c.Send([]byte(`{"x":2}`)) // succeeds, resetting the run
				<-c.sendCh
			}
		}

		select {
		case <-c.closed:
			t.Fatal("a connection that keeps up between drops was evicted")
		default:
		}
	})

	t.Run("zero budget disables eviction", func(t *testing.T) {
		m := metrics.New()
		c := newTestConn(t, m, ConnConfig{SendChanCapacity: 1, MaxBufferedBytes: 1 << 20})
		for i := 0; i < 500; i++ {
			c.Send([]byte(`{"x":1}`))
		}

		select {
		case <-c.closed:
			t.Fatal("eviction fired although MaxConsecutiveDrops was 0")
		default:
		}
		if got := testutil.ToFloat64(m.FramesDropped.WithLabelValues(metrics.DropChannelFull)); got == 0 {
			t.Fatal("drops should still be counted when eviction is disabled")
		}
	})

	// Oversized messages hit a different branch; it must feed the same
	// budget, or a client wedged that way is never evicted.
	t.Run("buffer-threshold drops also count toward the budget", func(t *testing.T) {
		m := metrics.New()
		c := newTestConn(t, m, ConnConfig{
			SendChanCapacity:    8,
			MaxBufferedBytes:    4,
			MaxConsecutiveDrops: 2,
		})
		c.Send([]byte("this is larger than the buffer"))
		c.Send([]byte("this is larger than the buffer"))

		select {
		case <-c.closed:
		default:
			t.Fatal("repeated buffer-threshold drops did not evict")
		}
	})
}
