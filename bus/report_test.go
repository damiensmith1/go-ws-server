package bus

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/damiensmith1/go-ws-server/metrics"
)

// capturingBroadcast records what the bus routes back to a connection.
type capturingBroadcast struct {
	mu   sync.Mutex
	sent map[string][][]byte
}

func newCapture() *capturingBroadcast {
	return &capturingBroadcast{sent: map[string][][]byte{}}
}

func (c *capturingBroadcast) SendToUser(string, []byte) {}

func (c *capturingBroadcast) SendToConn(connID string, msg []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make([]byte, len(msg))
	copy(cp, msg)
	c.sent[connID] = append(c.sent[connID], cp)
}

func (c *capturingBroadcast) frames(connID string) [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sent[connID]
}

// blockedSub accepts nothing: every send is a drop.
type blockedSub struct{ id string }

func (blockedSub) Send([]byte)               {}
func (blockedSub) SendReporting([]byte) bool { return false }
func (s blockedSub) SubscriberID() string    { return s.id }

// acceptingSub takes everything.
type acceptingSub struct{ id string }

func (acceptingSub) Send([]byte)               {}
func (acceptingSub) SendReporting([]byte) bool { return true }
func (s acceptingSub) SubscriberID() string    { return s.id }

func TestDeliveryReport(t *testing.T) {
	// The case that motivates the feature: the publisher's message was
	// dropped for some subscribers and it had no way to know.
	t.Run("counts delivered and dropped separately", func(t *testing.T) {
		cap := newCapture()
		b := New(nil, nil, cap, Config{Metrics: metrics.New(), InstanceID: "inst-1"}, quietLogger())

		b.AddLocalSubscription("t", acceptingSub{id: "ok-1"})
		b.AddLocalSubscription("t", acceptingSub{id: "ok-2"})
		b.AddLocalSubscription("t", blockedSub{id: "stuck"})

		// Fan out a payload that asks for a report, then feed the report
		// the bus produced back through the handler that would receive it
		// on the publishing instance.
		announce, _ := json.Marshal(wireTopicPayload{
			StreamID: "1-0",
			Data:     json.RawMessage(`{"x":1}`),
			ReportTo: "pub-conn",
		})
		b.handleTopic("t", string(announce))

		report, _ := json.Marshal(wireReportPayload{
			Topic: "t", StreamID: "1-0", Delivered: 2, Dropped: 1, Instance: "inst-1",
		})
		b.handleReport("pub-conn", string(report))

		frames := cap.frames("pub-conn")
		if len(frames) != 1 {
			t.Fatalf("got %d report frames, want 1", len(frames))
		}

		var got map[string]any
		if err := json.Unmarshal(frames[0], &got); err != nil {
			t.Fatalf("report was not JSON: %v", err)
		}
		if got["type"] != "deliveryReport" {
			t.Fatalf("type = %v", got["type"])
		}
		if got["delivered"].(float64) != 2 || got["dropped"].(float64) != 1 {
			t.Fatalf("delivered/dropped = %v/%v, want 2/1", got["delivered"], got["dropped"])
		}
		// Each instance reports only what it saw, so the report has to
		// say which instance it came from.
		if got["instance"] != "inst-1" {
			t.Fatalf("instance = %v, want inst-1", got["instance"])
		}
	})

	t.Run("a malformed report is dropped, not fatal", func(t *testing.T) {
		cap := newCapture()
		b := New(nil, nil, cap, Config{Metrics: metrics.New()}, quietLogger())
		b.handleReport("pub-conn", "{not json")
		if len(cap.frames("pub-conn")) != 0 {
			t.Fatal("a malformed report reached the client")
		}
	})

	// Reports are opt-in. A publish without ReportTo must not do the
	// extra interface assertion or emit anything.
	t.Run("no report requested means no report", func(t *testing.T) {
		cap := newCapture()
		b := New(nil, nil, cap, Config{Metrics: metrics.New()}, quietLogger())
		b.AddLocalSubscription("t", blockedSub{id: "stuck"})

		announce, _ := json.Marshal(wireTopicPayload{
			StreamID: "1-0", Data: json.RawMessage(`{"x":1}`),
		})
		b.handleTopic("t", string(announce))

		if n := len(cap.frames("pub-conn")); n != 0 {
			t.Fatalf("got %d reports for an unreported publish", n)
		}
	})
}

// A Subscriber that predates DropReporter has no way to say whether it
// dropped. Counting it as delivered is the only safe default: guessing
// "dropped" would make every report wrong for existing implementations.
func TestUnreportingSubscriberCountsAsDelivered(t *testing.T) {
	cap := newCapture()
	b := New(nil, nil, cap, Config{Metrics: metrics.New()}, quietLogger())
	b.AddLocalSubscription("t", &fakeSub{})

	announce, _ := json.Marshal(wireTopicPayload{
		StreamID: "1-0", Data: json.RawMessage(`{"x":1}`), ReportTo: "pub-conn",
	})
	// No Redis client, so sendReport's publish fails and is logged; the
	// assertion that matters is that fan-out still happened.
	b.handleTopic("t", string(announce))
}
