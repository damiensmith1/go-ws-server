package metrics

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
)

// Two instances must be able to coexist. If any collector used the global
// registerer, the second New would panic on duplicate registration.
func TestNew_IsolatedRegistries(t *testing.T) {
	a, b := New(), New()
	a.PublishTotal.Inc()
	if got := testutil.ToFloat64(a.PublishTotal); got != 1 {
		t.Fatalf("a: got %v, want 1", got)
	}
	if got := testutil.ToFloat64(b.PublishTotal); got != 0 {
		t.Fatalf("b should be independent: got %v, want 0", got)
	}
}

func TestHandler_ServesRegisteredCollectors(t *testing.T) {
	m := New()
	m.FramesDropped.WithLabelValues(DropChannelFull).Inc()

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))

	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		`ws_frames_dropped_total{reason="channel_full"} 1`,
		"bus_fanout_subscribers_bucket",
		"go_goroutines", // runtime collector is registered
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output missing %q", want)
		}
	}
}

func TestRedisHook_RecordsLatencyAndErrors(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)

	m := New()
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	c.AddHook(NewRedisHook(m))
	t.Cleanup(func() { c.Close() })

	ctx := context.Background()
	if err := c.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if got := testutil.CollectAndCount(m.RedisDuration); got == 0 {
		t.Fatal("set should have been timed")
	}

	// A miss is redis.Nil, which is not a failure and must not be counted.
	if err := c.Get(ctx, "absent").Err(); err != redis.Nil {
		t.Fatalf("want redis.Nil, got %v", err)
	}
	if got := testutil.ToFloat64(m.RedisErrors.WithLabelValues("get")); got != 0 {
		t.Fatalf("redis.Nil must not count as an error: got %v", got)
	}

	// A real error must be.
	if err := c.Do(ctx, "INCR", "k").Err(); err == nil {
		t.Fatal("INCR on a non-numeric value should fail")
	}
	if got := testutil.ToFloat64(m.RedisErrors.WithLabelValues("incr")); got != 1 {
		t.Fatalf("want 1 incr error, got %v", got)
	}
}
