// Package logger provides a structured logger with automatic redaction of
// sensitive fields. The redacting handler walks every log record's attrs and
// groups, replacing the value of any key whose lowercased name ends with one
// of the sensitive suffixes (password, token, authorization, secret) with
// "[REDACTED]".
//
// Redaction is best-effort. If you log a struct via slog.Any, slog won't
// introspect the struct's fields — implement slog.LogValuer on the struct
// to expose its fields as a slog.Group, or log fields explicitly.
package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

const RedactedValue = "[REDACTED]"

// sensitive returns true if a key name should have its value redacted.
// Match is case-insensitive on the suffix, so "Authorization", "X-Auth-Token",
// "userPassword", "AUTH_JWT_SECRET" all redact.
func sensitive(key string) bool {
	k := strings.ToLower(key)
	return strings.HasSuffix(k, "password") ||
		strings.HasSuffix(k, "token") ||
		strings.HasSuffix(k, "authorization") ||
		strings.HasSuffix(k, "secret")
}

type redactingHandler struct {
	inner slog.Handler
}

// New returns a slog.Logger that redacts sensitive attribute values.
// level is parsed as one of: debug, info, warn, error. Unknown values
// fall through to info.
func New(out io.Writer, level string) *slog.Logger {
	if out == nil {
		out = os.Stdout
	}
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	h := slog.NewJSONHandler(out, &slog.HandlerOptions{Level: lvl})
	return slog.New(&redactingHandler{inner: h}).With("service", "websocket-server")
}

func (h *redactingHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *redactingHandler) Handle(ctx context.Context, r slog.Record) error {
	// Build a fresh record so we can rewrite attrs without mutating the input.
	out := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(redactAttr(a))
		return true
	})
	return h.inner.Handle(ctx, out)
}

func (h *redactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	cleaned := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		cleaned[i] = redactAttr(a)
	}
	return &redactingHandler{inner: h.inner.WithAttrs(cleaned)}
}

func (h *redactingHandler) WithGroup(name string) slog.Handler {
	return &redactingHandler{inner: h.inner.WithGroup(name)}
}

func redactAttr(a slog.Attr) slog.Attr {
	// Resolve LogValuer first so structs implementing slog.LogValuer get
	// expanded into Groups we can walk.
	v := a.Value.Resolve()

	if v.Kind() == slog.KindGroup {
		group := v.Group()
		out := make([]slog.Attr, len(group))
		for i, g := range group {
			out[i] = redactAttr(g)
		}
		return slog.Attr{Key: a.Key, Value: slog.GroupValue(out...)}
	}

	if sensitive(a.Key) {
		return slog.String(a.Key, RedactedValue)
	}
	return slog.Attr{Key: a.Key, Value: v}
}
