package protocol

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{"missing type", `{}`, "must have a type"},
		{"unknown type", `{"type":"frobnicate"}`, "Unsupported message type"},
		{"subscribe ok", `{"type":"subscribe","topic":"x"}`, ""},
		{"subscribe missing topic", `{"type":"subscribe"}`, "must contain topic"},
		{"subscribe since iso", `{"type":"subscribe","topic":"x","since":"2026-05-01T00:00:00Z"}`, ""},
		{"subscribe since stream id", `{"type":"subscribe","topic":"x","since":"100-0"}`, ""},
		{"subscribe since number", `{"type":"subscribe","topic":"x","since":12345}`, ""},
		{"subscribe since bad", `{"type":"subscribe","topic":"x","since":true}`, "ISO timestamp"},
		{"publish ok", `{"type":"publish","topic":"x","data":{"k":1}}`, ""},
		{"publish missing data", `{"type":"publish","topic":"x"}`, "topic and data"},
		{"lock ok", `{"type":"lockTopic","topic":"x","lockType":"publish"}`, ""},
		{"lock bad lockType", `{"type":"lockTopic","topic":"x","lockType":"frob"}`, "valid lockType"},
		{"removeJob ok", `{"type":"removeJob","jobId":"j1"}`, ""},
		{"removeJob missing", `{"type":"removeJob"}`, "jobId"},
		{"broadcast ok", `{"type":"broadcast","data":{"k":1}}`, ""},
		{"broadcast missing data", `{"type":"broadcast"}`, "data field"},
		{
			"scheduleJob ok",
			`{"type":"scheduleJob","jobData":{"jobId":"j1","apiEndpoint":"https://x.test/","method":"POST","executeAt":"2099-01-01T00:00:00Z"}}`,
			"",
		},
		{
			"scheduleJob bad method",
			`{"type":"scheduleJob","jobData":{"jobId":"j1","apiEndpoint":"https://x.test/","method":"PATCH","executeAt":"2099-01-01T00:00:00Z"}}`,
			"invalid method",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var env Envelope
			if err := json.Unmarshal([]byte(tc.raw), &env); err != nil {
				t.Fatalf("unexpected unmarshal error: %v", err)
			}
			err := Validate(&env)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("expected error containing %q, got: %v", tc.wantErr, err)
			}
		})
	}
}

func TestEncodeRoundTrips(t *testing.T) {
	out := EncodeSuccess("r1", "ok")
	var f map[string]any
	if err := json.Unmarshal(out, &f); err != nil {
		t.Fatal(err)
	}
	if f["type"] != "success" || f["reqID"] != "r1" || f["message"] != "ok" {
		t.Fatalf("unexpected success frame: %v", f)
	}

	out = EncodePublish("chat", json.RawMessage(`{"x":1}`), "100-0", true)
	var p map[string]any
	if err := json.Unmarshal(out, &p); err != nil {
		t.Fatal(err)
	}
	if p["type"] != "publish" || p["topic"] != "chat" || p["streamId"] != "100-0" || p["replay"] != true {
		t.Fatalf("unexpected publish frame: %v", p)
	}
}

func TestSinceToString(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{`"100-0"`, "100-0"},
		{`12345`, "12345"},
		{`"2026-05-01T00:00:00Z"`, "2026-05-01T00:00:00Z"},
	}
	for _, tc := range cases {
		got, err := SinceToString(json.RawMessage(tc.raw))
		if err != nil {
			t.Fatalf("%s: %v", tc.raw, err)
		}
		if got != tc.want {
			t.Fatalf("%s: got %q, want %q", tc.raw, got, tc.want)
		}
	}
}
