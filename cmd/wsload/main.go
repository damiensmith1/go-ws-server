// Command wsload is a load generator for go-ws-server.
//
// It opens N subscriber connections to one topic, publishes M messages
// from a separate connection, and reports delivery latency percentiles,
// throughput and loss. The point is to turn "designed to scale
// horizontally" into a number, and to say which of the scalability
// backlog items actually matter for a given deployment.
//
//	go run ./cmd/wsload -url ws://localhost:8080/ws -subscribers 500 -messages 2000
//
// It measures end-to-end delivery: publish to receipt, through Redis and
// back out via fan-out, so it captures the whole path rather than any one
// component. Latency is per delivered message per subscriber.
//
// Loss is reported rather than treated as failure: fan-out is explicitly
// lossy for slow consumers, so a run that drops frames is information
// about the configured thresholds, not a broken test.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	var (
		url         = flag.String("url", "ws://localhost:8080/ws", "websocket endpoint")
		subscribers = flag.Int("subscribers", 100, "number of subscriber connections")
		messages    = flag.Int("messages", 1000, "messages to publish")
		topic       = flag.String("topic", "loadtest", "topic to use")
		rate        = flag.Int("rate", 0, "publishes per second (0 = as fast as possible)")
		payload     = flag.Int("payload", 256, "payload size in bytes")
		settle      = flag.Duration("settle", 3*time.Second, "how long to wait for stragglers after the last publish")
		token       = flag.String("token", "", "bearer token, if the server requires auth")
	)
	flag.Parse()

	if err := run(*url, *token, *topic, *subscribers, *messages, *rate, *payload, *settle); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

type result struct {
	mu        sync.Mutex
	latencies []time.Duration
	received  atomic.Int64
}

func (r *result) record(d time.Duration) {
	r.received.Add(1)
	r.mu.Lock()
	r.latencies = append(r.latencies, d)
	r.mu.Unlock()
}

func run(url, token, topic string, subscribers, messages, rate, payloadBytes int, settle time.Duration) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Printf("connecting %d subscribers to %s ...\n", subscribers, topic)

	res := &result{}
	var wg sync.WaitGroup
	conns := make([]*websocket.Conn, 0, subscribers)

	for i := 0; i < subscribers; i++ {
		c, err := dial(url, token, fmt.Sprintf("sub-%d", i))
		if err != nil {
			return fmt.Errorf("subscriber %d: %w", i, err)
		}
		conns = append(conns, c)

		if err := c.WriteJSON(map[string]any{"type": "subscribe", "topic": topic}); err != nil {
			return fmt.Errorf("subscriber %d subscribe: %w", i, err)
		}

		wg.Add(1)
		go func(c *websocket.Conn) {
			defer wg.Done()
			readLoop(c, res)
		}(c)
	}
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()

	// Let every subscription register before the first publish, or early
	// messages are counted as lost when they were simply not subscribed
	// for yet.
	time.Sleep(time.Duration(subscribers)*time.Millisecond + 500*time.Millisecond)

	pub, err := dial(url, token, "publisher")
	if err != nil {
		return fmt.Errorf("publisher: %w", err)
	}
	defer pub.Close()

	// Read the publisher's own replies. Without this the server's
	// rejections are invisible and a rate-limited run looks like message
	// loss, which is a completely different problem with a completely
	// different fix.
	var rejected atomic.Int64
	rejectReason := &sync.Map{}
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		for {
			_, raw, err := pub.ReadMessage()
			if err != nil {
				return
			}
			var frame struct {
				Type    string `json:"type"`
				Message string `json:"message"`
			}
			if json.Unmarshal(raw, &frame) != nil || frame.Type != "error" {
				continue
			}
			rejected.Add(1)
			n, _ := rejectReason.LoadOrStore(frame.Message, new(atomic.Int64))
			n.(*atomic.Int64).Add(1)
		}
	}()

	filler := make([]byte, payloadBytes)
	for i := range filler {
		filler[i] = 'x'
	}

	fmt.Printf("publishing %d messages ...\n", messages)
	var ticker *time.Ticker
	if rate > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(rate))
		defer ticker.Stop()
	}

	start := time.Now()
	for i := 0; i < messages; i++ {
		if ctx.Err() != nil {
			break
		}
		if ticker != nil {
			<-ticker.C
		}
		msg := map[string]any{
			"type":  "publish",
			"topic": topic,
			"data": map[string]any{
				"seq":    i,
				"sent":   time.Now().UnixNano(),
				"filler": string(filler),
			},
		}
		if err := pub.WriteJSON(msg); err != nil {
			return fmt.Errorf("publish %d: %w", i, err)
		}
	}
	publishElapsed := time.Since(start)

	fmt.Printf("published in %s, waiting %s for stragglers ...\n", publishElapsed.Round(time.Millisecond), settle)
	time.Sleep(settle)
	_ = pub.Close()
	<-pubDone

	for _, c := range conns {
		_ = c.Close()
	}
	wg.Wait()

	accepted := int64(messages) - rejected.Load()
	report(subscribers, messages, accepted, publishElapsed, res)
	if n := rejected.Load(); n > 0 {
		fmt.Println()
		fmt.Printf("publisher rejections %d of %d\n", n, messages)
		rejectReason.Range(func(k, v any) bool {
			fmt.Printf("  %6d  %s\n", v.(*atomic.Int64).Load(), k)
			return true
		})
		fmt.Println("\nRejected publishes never reached fan-out, so they are not loss.")
		fmt.Println("Raise RATE_LIMIT_MESSAGES_PER_SEC or use -rate to stay under it.")
	}
	return nil
}

func dial(url, token, userKey string) (*websocket.Conn, error) {
	full := url + "?userKey=" + userKey
	hdr := map[string][]string{}
	if token != "" {
		hdr["Authorization"] = []string{"Bearer " + token}
	}
	c, _, err := websocket.DefaultDialer.Dial(full, hdr)
	if err != nil {
		return nil, err
	}
	c.SetReadLimit(1 << 20)
	return c, nil
}

func readLoop(c *websocket.Conn, res *result) {
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			return
		}
		var frame struct {
			Type string `json:"type"`
			Data struct {
				Sent int64 `json:"sent"`
			} `json:"data"`
		}
		if json.Unmarshal(raw, &frame) != nil || frame.Type != "publish" {
			continue
		}
		if frame.Data.Sent != 0 {
			res.record(time.Duration(time.Now().UnixNano() - frame.Data.Sent))
		}
	}
}

// report takes accepted rather than messages for the expected count:
// deliveries can only be expected for publishes the server actually took.
func report(subscribers, messages int, accepted int64, publishElapsed time.Duration, res *result) {
	res.mu.Lock()
	lat := res.latencies
	res.mu.Unlock()
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })

	expected := int64(subscribers) * accepted
	got := res.received.Load()

	fmt.Println()
	fmt.Println("=== results ===")
	fmt.Printf("subscribers        %d\n", subscribers)
	fmt.Printf("messages published %d\n", messages)
	fmt.Printf("messages accepted  %d\n", accepted)
	fmt.Printf("deliveries expected %d\n", expected)
	fmt.Printf("deliveries received %d (%.2f%%)\n", got, 100*float64(got)/float64(expected))
	fmt.Printf("lost               %d\n", expected-got)
	fmt.Printf("publish wall time  %s (%.0f msg/s)\n",
		publishElapsed.Round(time.Millisecond),
		float64(messages)/publishElapsed.Seconds())
	if publishElapsed > 0 {
		fmt.Printf("delivery rate      %.0f deliveries/s\n", float64(got)/publishElapsed.Seconds())
	}

	if len(lat) == 0 {
		fmt.Println("no deliveries: nothing to report on latency")
		return
	}
	fmt.Println()
	fmt.Println("delivery latency (publish -> receipt)")
	for _, p := range []struct {
		name string
		q    float64
	}{{"p50", 0.50}, {"p90", 0.90}, {"p99", 0.99}, {"p999", 0.999}, {"max", 1.0}} {
		i := int(p.q * float64(len(lat)-1))
		fmt.Printf("  %-5s %s\n", p.name, lat[i].Round(time.Microsecond))
	}
}
