package app

import (
	"io"
	"log/slog"
	"net/http"
	"testing"
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
