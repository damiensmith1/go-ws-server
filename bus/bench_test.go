package bus

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/damiensmith1/go-ws-server/metrics"
	"github.com/damiensmith1/go-ws-server/protocol"
)

// nopSub is the cheapest possible subscriber, so these benchmarks measure
// the bus rather than the consumer.
type nopSub struct{ id string }

func (nopSub) Send([]byte)            {}
func (s nopSub) SubscriberID() string { return s.id }

func benchBus(b *testing.B) *Bus {
	b.Helper()
	return New(nil, nil, nil, Config{Metrics: metrics.New()}, quietLogger())
}

// BenchmarkHandleTopic measures fan-out as subscriber count grows. This is
// the hot path: one publish, N local subscribers, one critical section on
// b.mu. Watch for superlinear growth, which would mean lock contention
// rather than the unavoidable per-subscriber send.
func BenchmarkHandleTopic(b *testing.B) {
	for _, n := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("subscribers=%d", n), func(b *testing.B) {
			bus := benchBus(b)
			for i := 0; i < n; i++ {
				bus.AddLocalSubscription("t", nopSub{id: fmt.Sprintf("s%d", i)})
			}
			payload := `{"streamId":"1-1","data":{"x":1}}`

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bus.handleTopic("t", payload)
			}
		})
	}
}

// BenchmarkHandleTopicParallel measures contention on the single global
// b.mu across topics. If sharding the mutex by topic hash is worth doing
// (roadmap S3), the evidence shows up here as a gap between this and the
// serial benchmark above.
func BenchmarkHandleTopicParallel(b *testing.B) {
	const topics = 64
	bus := benchBus(b)
	for t := 0; t < topics; t++ {
		topic := fmt.Sprintf("t%d", t)
		for i := 0; i < 10; i++ {
			bus.AddLocalSubscription(topic, nopSub{id: fmt.Sprintf("s%d-%d", t, i)})
		}
	}
	payload := `{"streamId":"1-1","data":{"x":1}}`

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			bus.handleTopic(fmt.Sprintf("t%d", i%topics), payload)
			i++
		}
	})
}

// BenchmarkHandleTopicJudged measures the cost of honouring a publish-time
// recipient allowlist during fan-out, against the unfiltered path above.
func BenchmarkHandleTopicJudged(b *testing.B) {
	const n = 100
	bus := benchBus(b)
	recipients := make([]string, 0, n/2)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("s%d", i)
		bus.AddLocalSubscription("t", nopSub{id: id})
		if i%2 == 0 {
			recipients = append(recipients, id)
		}
	}
	payload, _ := json.Marshal(wireTopicPayload{
		StreamID:   "1-1",
		Data:       json.RawMessage(`{"x":1}`),
		Recipients: recipients,
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.handleTopic("t", string(payload))
	}
}

// BenchmarkEncodePublish isolates frame encoding, which handleTopic does
// once per message and shares across subscribers. If this ever moves back
// inside the send loop, the fan-out benchmarks above will show it.
func BenchmarkEncodePublish(b *testing.B) {
	data := json.RawMessage(`{"service":"checkout","severity":"warning","latencyMs":1180}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = protocol.EncodePublish("alerts.infra", data, "1699999999999-0", false)
	}
}

// BenchmarkSubscriptionChurn measures add/remove under the same lock the
// fan-out path uses. Connection churn competes with delivery for b.mu.
func BenchmarkSubscriptionChurn(b *testing.B) {
	bus := benchBus(b)
	subs := make([]nopSub, 100)
	for i := range subs {
		subs[i] = nopSub{id: fmt.Sprintf("s%d", i)}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := subs[i%len(subs)]
		bus.AddLocalSubscription("t", s)
		bus.RemoveLocalSubscription("t", s)
	}
}
