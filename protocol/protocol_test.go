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

func TestEncodePresence(t *testing.T) {
	// A topic with no subscribers must encode as [], not null: a client
	// decoding into a list should not have to special-case nil.
	t.Run("empty topic encodes as an empty list", func(t *testing.T) {
		got := string(EncodePresence("r1", "chat", nil))
		if strings.Contains(got, "null") {
			t.Fatalf("got %s, want an empty JSON array", got)
		}
		if !strings.Contains(got, `"count":0`) {
			t.Fatalf("got %s, want count 0", got)
		}
	})

	t.Run("carries subscribers, count and reqID", func(t *testing.T) {
		var frame struct {
			Type        string   `json:"type"`
			ReqID       string   `json:"reqID"`
			Topic       string   `json:"topic"`
			Subscribers []string `json:"subscribers"`
			Count       int      `json:"count"`
		}
		if err := json.Unmarshal(EncodePresence("r1", "chat", []string{"alice", "bob"}), &frame); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if frame.Type != OutPresence || frame.ReqID != "r1" || frame.Topic != "chat" {
			t.Fatalf("frame = %+v", frame)
		}
		if frame.Count != 2 || len(frame.Subscribers) != 2 {
			t.Fatalf("frame = %+v, want 2 subscribers", frame)
		}
	})
}

func TestEncodeSubscriptions(t *testing.T) {
	got := string(EncodeSubscriptions("r2", nil))
	if strings.Contains(got, "null") {
		t.Fatalf("got %s, want an empty JSON array", got)
	}

	var frame struct {
		Type   string   `json:"type"`
		ReqID  string   `json:"reqID"`
		Topics []string `json:"topics"`
	}
	if err := json.Unmarshal(EncodeSubscriptions("r2", []string{"a", "b"}), &frame); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if frame.Type != OutSubscriptions || frame.ReqID != "r2" || len(frame.Topics) != 2 {
		t.Fatalf("frame = %+v", frame)
	}
}

func TestValidateNewVerbs(t *testing.T) {
	if err := Validate(&Envelope{Type: TypePresence}); err == nil {
		t.Fatal("presence without a topic was accepted")
	}
	if err := Validate(&Envelope{Type: TypePresence, Topic: "chat"}); err != nil {
		t.Fatalf("valid presence rejected: %v", err)
	}
	// listSubscriptions takes no fields: a client can only ask about its
	// own subscriptions, so there is nothing to name.
	if err := Validate(&Envelope{Type: TypeListSubs}); err != nil {
		t.Fatalf("listSubscriptions rejected: %v", err)
	}
}

func TestProtocolVersionNegotiation(t *testing.T) {
	v := func(n int) *int { return &n }

	tests := []struct {
		name    string
		version *int
		wantErr bool
	}{
		// Absent must keep working: every client written before
		// versioning existed omits the field.
		{"absent is the current version", nil, false},
		{"current version is accepted", v(Version), false},

		// An explicit 0 is a client naming a version that never existed,
		// which is why Version is a pointer rather than an int.
		{"zero is rejected", v(0), true},
		{"future version is rejected", v(Version + 1), true},
		{"negative is rejected", v(-1), true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(&Envelope{Type: TypeSubscribe, Topic: "t", Version: tc.version})
			if tc.wantErr && err == nil {
				t.Fatal("want rejection")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("want acceptance, got %v", err)
			}
			// The error must state the range, or a client has no way to
			// learn what this server actually speaks.
			if tc.wantErr && !strings.Contains(err.Error(), "supports") {
				t.Fatalf("error %q does not state the supported range", err)
			}
		})
	}
}
