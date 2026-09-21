package app

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

// Correlation is the point of connID: a user with several sockets open is
// exactly the case where timestamps and userKey are not enough to tell
// which connection a log line belongs to.
func TestConnectionLoggerCarriesCorrelationFields(t *testing.T) {
	var buf bytes.Buffer
	base := slog.New(slog.NewJSONHandler(&buf, nil))

	connLog := base.With("connID", "conn-abc", "userKey", "alice")
	connLog.Info("websocket client connected")
	connLog.With("reqID", "r7").Error("message handling failed", "type", "publish")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}

	for i, line := range lines {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("line %d is not JSON: %v", i, err)
		}
		if rec["connID"] != "conn-abc" {
			t.Errorf("line %d missing connID: %v", i, rec)
		}
		if rec["userKey"] != "alice" {
			t.Errorf("line %d missing userKey: %v", i, rec)
		}
	}

	var second map[string]any
	_ = json.Unmarshal([]byte(lines[1]), &second)
	if second["reqID"] != "r7" {
		t.Errorf("frame-scoped line missing reqID: %v", second)
	}
}
