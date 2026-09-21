package connection

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/damiensmith1/go-ws-server/internal/authz"
	"github.com/damiensmith1/go-ws-server/internal/metrics"
	"github.com/damiensmith1/go-ws-server/internal/protocol"
)

// env.Type comes straight from the client, so it must never reach a label
// unsanitised: unbounded label values mean unbounded time series.
func TestFrameTypeLabel_BoundsCardinality(t *testing.T) {
	for _, known := range []string{
		protocol.TypeSubscribe,
		protocol.TypePublish,
		protocol.TypeScheduleJob,
		protocol.TypeBroadcast,
	} {
		if got := frameTypeLabel(known); got != known {
			t.Fatalf("known type %q became %q", known, got)
		}
	}
	for _, junk := range []string{"", "SUBSCRIBE", "../../etc/passwd", "a-random-string"} {
		if got := frameTypeLabel(junk); got != "unknown" {
			t.Fatalf("junk type %q became %q, want \"unknown\"", junk, got)
		}
	}
}

func TestAuthorize(t *testing.T) {
	conn := func(t *testing.T) *Conn {
		return newTestConn(t, metrics.New(), ConnConfig{SendChanCapacity: 4, MaxBufferedBytes: 1 << 20})
	}

	// Absent policy must behave exactly as the server did before
	// authorization existed, or upgrading breaks every deployment.
	t.Run("a nil Authorizer allows", func(t *testing.T) {
		m := metrics.New()
		d := Deps{Log: quietLogger(), Metrics: m}
		if err := authorize(context.Background(), conn(t), authz.ActionPublish, "any.topic", d); err != nil {
			t.Fatalf("nil Authorizer denied: %v", err)
		}
		if got := testutil.ToFloat64(m.AuthzDecisions.WithLabelValues("publish", "allowed")); got != 1 {
			t.Fatalf("allowed counter = %v, want 1", got)
		}
	})

	t.Run("a denial names the action and topic", func(t *testing.T) {
		m := metrics.New()
		d := Deps{Log: quietLogger(), Metrics: m, Authorizer: authz.DenyAll()}

		err := authorize(context.Background(), conn(t), authz.ActionSubscribe, "tenant.other.orders", d)
		if err == nil {
			t.Fatal("want a denial")
		}
		if !strings.Contains(err.Error(), "subscribe") || !strings.Contains(err.Error(), "tenant.other.orders") {
			t.Fatalf("client-facing message should name what was refused, got %q", err)
		}
		if got := testutil.ToFloat64(m.AuthzDecisions.WithLabelValues("subscribe", "denied")); got != 1 {
			t.Fatalf("denied counter = %v, want 1", got)
		}
	})

	// The distinction that matters operationally: an unreachable policy
	// store is not a permission change, and must not be reported to the
	// client as one. It still fails closed.
	t.Run("an authorizer failure is not reported as a denial", func(t *testing.T) {
		m := metrics.New()
		broken := authz.AuthorizerFunc(func(context.Context, authz.Request) error {
			return errors.New("policy store unreachable")
		})
		d := Deps{Log: quietLogger(), Metrics: m, Authorizer: broken}

		err := authorize(context.Background(), conn(t), authz.ActionLock, "t", d)
		if err == nil {
			t.Fatal("want the request to fail closed")
		}
		if strings.Contains(err.Error(), "authorized") {
			t.Fatalf("an outage was reported to the client as a permission problem: %q", err)
		}
		if got := testutil.ToFloat64(m.AuthzDecisions.WithLabelValues("lock", "error")); got != 1 {
			t.Fatalf("error counter = %v, want 1; denials and outages must not share a bucket", got)
		}
		if got := testutil.ToFloat64(m.AuthzDecisions.WithLabelValues("lock", "denied")); got != 0 {
			t.Fatalf("denied counter = %v, want 0", got)
		}
	})

	t.Run("claims reach the authorizer", func(t *testing.T) {
		var seen authz.Request
		spy := authz.AuthorizerFunc(func(_ context.Context, r authz.Request) error {
			seen = r
			return nil
		})
		c := newTestConn(t, metrics.New(), ConnConfig{
			SendChanCapacity: 4,
			MaxBufferedBytes: 1 << 20,
			Claims:           map[string]any{"role": "admin"},
		})
		d := Deps{Log: quietLogger(), Metrics: metrics.New(), Authorizer: spy}

		if err := authorize(context.Background(), c, authz.ActionPublish, "t", d); err != nil {
			t.Fatalf("authorize: %v", err)
		}
		if seen.Claims["role"] != "admin" {
			t.Fatalf("claims did not reach the policy: %#v", seen.Claims)
		}
		if seen.UserKey != c.UserKey() {
			t.Fatalf("UserKey = %q, want %q", seen.UserKey, c.UserKey())
		}
	})
}

// The gate must sit in front of every side effect. These call the real
// handlers with a Deps whose RDB and Bus are nil: if authorization is
// skipped or runs late, the handler dereferences nil and panics instead of
// returning a denial, which is exactly the regression worth catching.
func TestHandlersAuthorizeBeforeActing(t *testing.T) {
	handlers := map[string]struct {
		fn     func(context.Context, *Conn, *protocol.Envelope, Deps) (string, error)
		action string
	}{
		"subscribe":   {handleSubscribe, "subscribe"},
		"publish":     {handlePublish, "publish"},
		"lockTopic":   {handleLockTopic, "lock"},
		"unlockTopic": {handleUnlockTopic, "lock"},
		"renewLock":   {handleRenewLock, "lock"},
	}

	for name, h := range handlers {
		t.Run(name, func(t *testing.T) {
			m := metrics.New()
			c := newTestConn(t, m, ConnConfig{SendChanCapacity: 4, MaxBufferedBytes: 1 << 20})
			d := Deps{Log: quietLogger(), Metrics: m, Authorizer: authz.DenyAll()}
			env := &protocol.Envelope{Topic: "tenant.other.orders", LockType: protocol.LockPublish}

			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("handler acted before authorizing (panicked on nil deps): %v", r)
				}
			}()

			msg, err := h.fn(context.Background(), c, env, d)
			if err == nil {
				t.Fatal("denied request was allowed through")
			}
			if msg != "" {
				t.Fatalf("denied request produced a success message %q", msg)
			}
			if got := testutil.ToFloat64(m.AuthzDecisions.WithLabelValues(h.action, "denied")); got != 1 {
				t.Fatalf("denied counter for %s = %v, want 1", h.action, got)
			}
		})
	}
}

// Unsubscribe is intentionally ungated: dropping your own subscription
// only removes access. Gating it would let a policy change trap a client
// inside a topic it can no longer read.
func TestUnsubscribeIsNotGated(t *testing.T) {
	m := metrics.New()
	c := newTestConn(t, m, ConnConfig{SendChanCapacity: 4, MaxBufferedBytes: 1 << 20})
	d := Deps{Log: quietLogger(), Metrics: m, Authorizer: authz.DenyAll()}

	// A gated handler would return a denial before touching RDB; an
	// ungated one reaches Redis. Either way it must not be a denial.
	defer func() { _ = recover() }()
	_, err := handleUnsubscribe(context.Background(), c, &protocol.Envelope{Topic: "t"}, d)
	if err != nil && strings.Contains(err.Error(), "Not authorized") {
		t.Fatal("unsubscribe is gated; a revoked client cannot leave the topic")
	}
}
