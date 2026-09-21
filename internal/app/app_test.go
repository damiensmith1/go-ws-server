package app

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/damiensmith1/go-ws-server/metrics"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func TestOriginChecker(t *testing.T) {
	t.Run("empty allowlist defers to gorilla", func(t *testing.T) {
		if originChecker(nil, quietLogger()) != nil {
			t.Fatal("want nil so the upgrader applies its same-origin default")
		}
	})

	allowed := []string{"https://app.example.com", "http://localhost:3000"}
	check := originChecker(allowed, quietLogger())

	tests := []struct {
		name   string
		origin string
		want   bool
	}{
		{"listed origin", "https://app.example.com", true},
		{"other listed origin", "http://localhost:3000", true},
		{"case-insensitive", "https://APP.example.com", true},
		{"unlisted origin", "https://evil.example.com", false},
		{"scheme must match", "http://app.example.com", false},
		{"port must match", "http://localhost:3001", false},
		{"absent header is a non-browser client", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _ := http.NewRequest("GET", "/ws", nil)
			if tt.origin != "" {
				r.Header.Set("Origin", tt.origin)
			}
			if got := check(r); got != tt.want {
				t.Fatalf("origin %q: got %v, want %v", tt.origin, got, tt.want)
			}
		})
	}

	t.Run("wildcard allows anything", func(t *testing.T) {
		check := originChecker([]string{"*"}, quietLogger())
		r, _ := http.NewRequest("GET", "/ws", nil)
		r.Header.Set("Origin", "https://evil.example.com")
		if !check(r) {
			t.Fatal("wildcard should allow any origin")
		}
	})
}

func TestMetricsServer(t *testing.T) {
	m := metrics.New()

	if metricsServer("", m) != nil {
		t.Fatal("empty METRICS_ADDR must disable the endpoint")
	}

	srv := metricsServer(":9090", m)
	if srv == nil {
		t.Fatal("want a server for a non-empty addr")
	}
	if srv.Addr != ":9090" {
		t.Fatalf("addr = %q", srv.Addr)
	}

	m.ConnectionsActive.Set(3)
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "ws_connections_active 3") {
		t.Fatal("metrics body missing ws_connections_active")
	}

	// Nothing else is mounted on the metrics listener.
	rec = httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest("GET", "/ws", nil))
	if rec.Code != 404 {
		t.Fatalf("/ws on the metrics listener: status %d, want 404", rec.Code)
	}
}

func TestReadyHandler(t *testing.T) {
	newProbe := func(t *testing.T) (redis.UniversalClient, *miniredis.Miniredis) {
		t.Helper()
		mr := miniredis.RunT(t)
		return redis.NewClient(&redis.Options{Addr: mr.Addr()}), mr
	}

	t.Run("ready when redis answers", func(t *testing.T) {
		rdb, _ := newProbe(t)
		rec := httptest.NewRecorder()
		readyHandler(rdb, time.Second, quietLogger())(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
		}
		if got := rec.Body.String(); got != "ok" {
			t.Fatalf("body = %q, want %q", got, "ok")
		}
	})

	// The whole point of /readyz over /healthz: an instance whose Redis is
	// gone must stop advertising itself as routable.
	t.Run("not ready when redis is gone", func(t *testing.T) {
		rdb, mr := newProbe(t)
		mr.Close()

		rec := httptest.NewRecorder()
		readyHandler(rdb, time.Second, quietLogger())(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
		}
		if !strings.Contains(rec.Body.String(), "redis unavailable") {
			t.Fatalf("body = %q, want it to name the failing dependency", rec.Body.String())
		}
	})

	t.Run("non-positive timeout falls back to the default", func(t *testing.T) {
		rdb, _ := newProbe(t)
		rec := httptest.NewRecorder()
		readyHandler(rdb, 0, quietLogger())(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
		}
	})
}
