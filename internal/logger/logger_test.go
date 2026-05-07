package logger

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestRedacts_TopLevel(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, "debug")
	log.Info("hello",
		"password", "hunter2",
		"token", "secret-jwt",
		"Authorization", "Bearer xyz",
		"AUTH_JWT_SECRET", "shh",
		"plain", "visible",
	)
	got := buf.String()
	for _, leak := range []string{"hunter2", "secret-jwt", "Bearer xyz", "shh"} {
		if strings.Contains(got, leak) {
			t.Errorf("expected %q to be redacted: %s", leak, got)
		}
	}
	if !strings.Contains(got, "visible") {
		t.Errorf("non-sensitive field missing: %s", got)
	}
	if !strings.Contains(got, RedactedValue) {
		t.Errorf("expected redaction marker: %s", got)
	}
}

// redactableValue implements slog.LogValuer to emit fields as a slog.Group
// — the supported pattern for nested struct redaction.
type redactableValue struct {
	Authorization string
	User          string
}

func (v redactableValue) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("Authorization", v.Authorization),
		slog.String("user", v.User),
	)
}

func TestRedacts_ResolvesLogValuer(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, "debug")
	v := redactableValue{Authorization: "Bearer leak", User: "alice"}
	log.Info("call", "headers", v)
	out := buf.String()
	if strings.Contains(out, "Bearer leak") {
		t.Errorf("LogValuer-emitted group must be walked: %s", out)
	}
	if !strings.Contains(out, "alice") {
		t.Errorf("non-sensitive field should still be present: %s", out)
	}
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatalf("not valid JSON: %s", line)
		}
	}
}

func TestRedacts_PreservedAcrossWith(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, "debug")
	child := log.With("password", "hunter2", "ok", 1)
	child.Info("event")
	if strings.Contains(buf.String(), "hunter2") {
		t.Errorf("With() must propagate redaction: %s", buf.String())
	}
}
