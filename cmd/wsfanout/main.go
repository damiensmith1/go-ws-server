// Command wsfanout measures cross-instance fan-out waste.
//
// Every instance of this server runs PSUBSCRIBE on "ws:topic:*", so every
// instance receives every message published to every topic — including
// topics it has no local subscribers for, which it decodes, locks for,
// finds nobody for, and discards. Cost is O(instances x all messages)
// rather than O(instances x relevant messages).
//
// That the waste exists is arithmetic, not a discovery: with N instances,
// each message is handled N times and useful once. What is not obvious is
// whether it *costs* enough to matter at a given deployment's size, which
// is the question this answers, and the question that decides whether
// interest-based routing is worth building.
//
//	go run ./cmd/wsfanout -redis localhost:6379 -instances 4 -topics 40 -messages 500
//
// Instances run in this process rather than as separate programs. The
// headline result is a ratio — messages handled against messages that
// mattered — and a ratio is unaffected by sharing a CPU, so co-locating
// them costs nothing that matters and removes a pile of orchestration.
//
// One instance is deliberately given no subscriptions at all. Every
// message it handles is therefore waste, so its mean handling time is a
// direct measurement of what discarding one irrelevant message costs —
// no arithmetic needed to separate it from useful work.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"

	"github.com/damiensmith1/go-ws-server/wsserver"
)

func main() {
	var (
		redisAddr = flag.String("redis", "localhost:6379", "Redis address shared by every instance")
		instances = flag.Int("instances", 4, "server instances to run (one extra idle instance is always added)")
		topics    = flag.Int("topics", 40, "total topics, partitioned evenly across instances")
		messages  = flag.Int("messages", 500, "messages to publish, spread across topics")
		subsPer   = flag.Int("subscribers-per-topic", 1, "subscriber connections per topic")
		payload   = flag.Int("payload", 0, "extra payload bytes per message; Redis egress scales with this")
		settle    = flag.Duration("settle", 3*time.Second, "wait for delivery to finish before reading metrics")
		verbose   = flag.Bool("v", false, "log server output")
	)
	flag.Parse()

	if err := run(*redisAddr, *instances, *topics, *messages, *subsPer, *payload, *settle, *verbose); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

type instance struct {
	name   string
	srv    *wsserver.Server
	topics []string // topics this instance holds subscribers for
	cancel context.CancelFunc
	done   chan struct{}
}

func run(redisAddr string, n, nTopics, nMessages, subsPer, payloadBytes int, settle time.Duration, verbose bool) error {
	if n < 1 {
		return fmt.Errorf("-instances must be at least 1")
	}
	if nTopics < n {
		return fmt.Errorf("-topics (%d) must be at least -instances (%d)", nTopics, n)
	}

	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	if verbose {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	}

	topicNames := make([]string, nTopics)
	for i := range topicNames {
		topicNames[i] = fmt.Sprintf("topic-%03d", i)
	}

	// Partition topics across the working instances. Instance i holds
	// subscribers only for its own slice, so every message published to
	// another slice is waste for it.
	perInstance := nTopics / n

	var insts []*instance
	defer func() {
		for _, in := range insts {
			in.cancel()
			select {
			case <-in.done:
			case <-time.After(5 * time.Second):
			}
		}
	}()

	for i := 0; i < n; i++ {
		lo := i * perInstance
		hi := lo + perInstance
		if i == n-1 {
			hi = nTopics // last instance absorbs the remainder
		}
		in, err := startInstance(fmt.Sprintf("inst-%d", i), redisAddr, topicNames[lo:hi], log)
		if err != nil {
			return err
		}
		insts = append(insts, in)
	}

	// The idle instance: no subscriptions, so everything it handles is
	// waste and its mean handling time is the cost of a discard.
	idle, err := startInstance("idle", redisAddr, nil, log)
	if err != nil {
		return err
	}
	insts = append(insts, idle)

	fmt.Printf("started %d instances (%d working + 1 idle) against %s\n", len(insts), n, redisAddr)
	fmt.Printf("%d topics, ~%d per instance\n\n", nTopics, perInstance)

	// Connect subscribers to the instance that owns each topic.
	var conns []*websocket.Conn
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()
	subscribed := 0
	for _, in := range insts {
		for _, topic := range in.topics {
			for s := 0; s < subsPer; s++ {
				c, err := dialAndSubscribe(in.srv.Addr(), fmt.Sprintf("%s-%s-%d", in.name, topic, s), topic)
				if err != nil {
					return fmt.Errorf("%s subscribe %s: %w", in.name, topic, err)
				}
				conns = append(conns, c)
				subscribed++
				go drain(c)
			}
		}
	}
	fmt.Printf("connected %d subscribers\n", subscribed)
	time.Sleep(time.Duration(subscribed)*2*time.Millisecond + 500*time.Millisecond)

	// Baseline the counters after setup so subscription traffic does not
	// land in the measurement.
	base := make(map[string]counters, len(insts))
	for _, in := range insts {
		base[in.name] = readCounters(in)
	}

	statsClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer statsClient.Close()
	ctx := context.Background()
	redisBefore := readRedisStats(ctx, statsClient)

	// Publish through the first instance, round-robin across every topic.
	pub, err := dial(insts[0].srv.Addr(), "publisher")
	if err != nil {
		return fmt.Errorf("publisher: %w", err)
	}
	defer pub.Close()
	go drain(pub)

	filler := strings.Repeat("x", payloadBytes)

	fmt.Printf("publishing %d messages across %d topics (payload %d B) ...\n", nMessages, nTopics, payloadBytes)
	start := time.Now()
	for i := 0; i < nMessages; i++ {
		topic := topicNames[i%nTopics]
		data := map[string]any{"seq": i}
		if payloadBytes > 0 {
			data["filler"] = filler
		}
		if err := pub.WriteJSON(map[string]any{
			"type":  "publish",
			"topic": topic,
			"data":  data,
		}); err != nil {
			return fmt.Errorf("publish %d: %w", i, err)
		}
	}
	elapsed := time.Since(start)
	time.Sleep(settle)
	redisAfter := readRedisStats(ctx, statsClient)

	report(insts, base, nTopics, nMessages, elapsed, redisBefore, redisAfter)
	return nil
}

func startInstance(name, redisAddr string, topics []string, log *slog.Logger) (*instance, error) {
	srv, err := wsserver.New(wsserver.Options{
		ListenAddr: ":0", // let the OS pick
		RedisAddrs: []string{redisAddr},
		InstanceID: name,
		Logger:     log,
		// Out of the way of the measurement: the publisher sends as fast
		// as it can, and connections must not be evicted mid-run.
		MessagesPerSec:      1_000_000,
		MaxConsecutiveDrops: -1,
		MaxBufferedBytes:    64 << 20,
	})
	if err != nil {
		return nil, fmt.Errorf("start %s: %w", name, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Run(ctx)
	}()

	return &instance{name: name, srv: srv, topics: topics, cancel: cancel, done: done}, nil
}

func dial(addr, userKey string) (*websocket.Conn, error) {
	url := "ws://" + addr + "/ws?userKey=" + userKey
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}
	c.SetReadLimit(1 << 20)
	return c, nil
}

func dialAndSubscribe(addr, userKey, topic string) (*websocket.Conn, error) {
	c, err := dial(addr, userKey)
	if err != nil {
		return nil, err
	}
	if err := c.WriteJSON(map[string]any{"type": "subscribe", "topic": topic}); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// drain reads and discards, so a subscriber never stops reading and
// nothing is dropped for the wrong reason.
func drain(c *websocket.Conn) {
	for {
		if _, _, err := c.ReadMessage(); err != nil {
			return
		}
	}
}

// redisStats is Redis's own view of the cost. The server-side metric only
// covers time inside handleTopic; it cannot see the work Redis does
// turning one PUBLISH into N deliveries, which is the other half of the
// waste and the half that would make Redis, not the server, the first
// thing to fall over.
type redisStats struct {
	outputBytes uint64
	commands    uint64
}

func readRedisStats(ctx context.Context, c *redis.Client) redisStats {
	var s redisStats
	info, err := c.Info(ctx, "stats").Result()
	if err != nil {
		return s
	}
	for _, line := range strings.Split(info, "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok {
			continue
		}
		var n uint64
		switch key {
		case "total_net_output_bytes":
			_, _ = fmt.Sscanf(value, "%d", &n)
			s.outputBytes = n
		case "total_commands_processed":
			_, _ = fmt.Sscanf(value, "%d", &n)
			s.commands = n
		}
	}
	return s
}

type counters struct {
	handled   uint64  // bus_fanout_duration_seconds_count
	seconds   float64 // bus_fanout_duration_seconds_sum
	published uint64  // bus_publish_total
}

func readCounters(in *instance) counters {
	var c counters
	families, err := in.srv.Metrics().Registry().Gather()
	if err != nil {
		return c
	}
	for _, f := range families {
		switch f.GetName() {
		case "bus_fanout_duration_seconds":
			for _, m := range f.GetMetric() {
				if h := m.GetHistogram(); h != nil {
					c.handled += h.GetSampleCount()
					c.seconds += h.GetSampleSum()
				}
			}
		case "bus_publish_total":
			for _, m := range f.GetMetric() {
				c.published += uint64(counterValue(m))
			}
		}
	}
	return c
}

func counterValue(m *dto.Metric) float64 {
	if c := m.GetCounter(); c != nil {
		return c.GetValue()
	}
	return 0
}

func report(insts []*instance, base map[string]counters, nTopics, nMessages int, elapsed time.Duration, rBefore, rAfter redisStats) {
	type row struct {
		name     string
		topics   int
		handled  uint64
		relevant int
		seconds  float64
	}

	var rows []row
	var totalHandled uint64
	var totalRelevant int
	var totalSeconds float64
	var idleSeconds float64
	var idleHandled uint64

	for _, in := range insts {
		now := readCounters(in)
		b := base[in.name]
		handled := now.handled - b.handled
		seconds := now.seconds - b.seconds

		// Messages published to topics this instance actually serves.
		// Publishing round-robins across topics, so a topic gets either
		// floor or ceil of nMessages/nTopics.
		relevant := 0
		for _, t := range in.topics {
			relevant += messagesForTopic(t, nTopics, nMessages)
		}

		rows = append(rows, row{in.name, len(in.topics), handled, relevant, seconds})
		totalHandled += handled
		totalRelevant += relevant
		totalSeconds += seconds
		if in.name == "idle" {
			idleSeconds = seconds
			idleHandled = handled
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })

	fmt.Println()
	fmt.Println("=== per instance ===")
	fmt.Printf("%-8s %7s %9s %9s %7s %12s\n", "instance", "topics", "handled", "relevant", "waste", "fanout time")
	for _, r := range rows {
		waste := "-"
		if r.relevant > 0 {
			waste = fmt.Sprintf("%.1fx", float64(r.handled)/float64(r.relevant))
		} else if r.handled > 0 {
			waste = "all"
		}
		fmt.Printf("%-8s %7d %9d %9d %7s %12s\n",
			r.name, r.topics, r.handled, r.relevant, waste,
			time.Duration(r.seconds*float64(time.Second)).Round(time.Microsecond))
	}

	fmt.Println()
	fmt.Println("=== totals ===")
	fmt.Printf("messages published         %d\n", nMessages)
	fmt.Printf("handled across instances   %d\n", totalHandled)
	fmt.Printf("of which useful            %d\n", totalRelevant)
	fmt.Printf("of which discarded         %d\n", totalHandled-uint64(totalRelevant))
	if totalRelevant > 0 {
		fmt.Printf("waste factor               %.2fx\n", float64(totalHandled)/float64(totalRelevant))
	}
	fmt.Printf("publish wall time          %s (%.0f msg/s)\n",
		elapsed.Round(time.Millisecond), float64(nMessages)/elapsed.Seconds())

	if out := rAfter.outputBytes - rBefore.outputBytes; out > 0 {
		fmt.Println()
		fmt.Println("=== redis side ===")
		fmt.Printf("bytes Redis pushed out     %s\n", humanBytes(out))
		fmt.Printf("per published message      %s\n", humanBytes(out/uint64(max(nMessages, 1))))
		fmt.Printf("commands processed         %d\n", rAfter.commands-rBefore.commands)
		fmt.Println("Redis delivers each PUBLISH once per subscribed instance, so this")
		fmt.Println("scales with instance count whether or not the message is wanted.")
	}

	// The idle instance turns the ratio into a cost.
	if idleHandled > 0 {
		perDiscard := idleSeconds / float64(idleHandled)
		fmt.Println()
		fmt.Println("=== cost of a discard ===")
		fmt.Printf("measured on the idle instance, where every message handled is waste\n")
		fmt.Printf("  messages handled         %d\n", idleHandled)
		fmt.Printf("  time in fan-out          %s\n", time.Duration(idleSeconds*float64(time.Second)).Round(time.Microsecond))
		fmt.Printf("  per discarded message    %s\n", time.Duration(perDiscard*float64(time.Second)).Round(time.Nanosecond))

		fmt.Println()
		fmt.Println("=== projection ===")
		fmt.Println("Server CPU spent discarding, per instance, at a sustained publish rate:")
		fmt.Printf("  %-14s %s\n", "msgs/sec", "per instance")
		for _, rate := range []int{1_000, 10_000, 50_000, 100_000} {
			fmt.Printf("  %-14d %.2f%% of a core\n", rate, perDiscard*float64(rate)*100)
		}
	}

	// Redis egress is the part that scales badly, so project it
	// explicitly rather than leaving it as a single total.
	if out := rAfter.outputBytes - rBefore.outputBytes; out > 0 && nMessages > 0 {
		nInstances := len(insts)
		perMsgPerInstance := float64(out) / float64(nMessages) / float64(nInstances)

		fmt.Println()
		fmt.Println("Redis network egress, which grows with BOTH instance count and payload size:")
		fmt.Printf("  %.0f B per message per subscribed instance (at this payload size)\n\n", perMsgPerInstance)
		fmt.Printf("  %-12s", "msgs/sec")
		counts := []int{5, 10, 20, 50}
		for _, n := range counts {
			fmt.Printf("%14s", fmt.Sprintf("%d inst", n))
		}
		fmt.Println()
		for _, rate := range []int{1_000, 10_000, 50_000, 100_000} {
			fmt.Printf("  %-12d", rate)
			for _, n := range counts {
				gbps := perMsgPerInstance * float64(rate) * float64(n) * 8 / 1e9
				fmt.Printf("%14s", fmt.Sprintf("%.2f Gbps", gbps))
			}
			fmt.Println()
		}
		fmt.Println()
		fmt.Println(interpretEgress(perMsgPerInstance))
	}

	if idleHandled > 0 {
		fmt.Println()
		fmt.Println(interpret(idleSeconds / float64(idleHandled)))
	}
}

// messagesForTopic mirrors the round-robin the publisher uses.
func messagesForTopic(topic string, nTopics, nMessages int) int {
	var idx int
	_, _ = fmt.Sscanf(topic, "topic-%d", &idx)
	full := nMessages / nTopics
	if idx < nMessages%nTopics {
		return full + 1
	}
	return full
}

func humanBytes(n uint64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2f KiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// interpretEgress judges the Redis-side cost, which is the one that
// actually decides this.
func interpretEgress(perMsgPerInstance float64) string {
	var b strings.Builder
	b.WriteString("Reading this: Redis delivers every message to every instance, so its\n")
	b.WriteString("egress is (message size x instance count) per publish whether or not the\n")
	b.WriteString("message is wanted. Interest-based routing (S2) is what removes it.\n")
	b.WriteString("Note this scales with payload size too -- a 10x bigger message is 10x\n")
	b.WriteString("this table.\n\n")

	// Where a 1 Gbps link saturates: bytes * rate * instances * 8 = 1e9.
	budget := 1e9 / (perMsgPerInstance * 8)
	b.WriteString(fmt.Sprintf("Verdict: at this payload size, a 1 Gbps Redis link saturates at roughly\n"))
	b.WriteString(fmt.Sprintf("  messages/sec x instances = %.0f\n", budget))
	b.WriteString("  (e.g. 10 instances at ")
	b.WriteString(fmt.Sprintf("%.0f msgs/sec, or 50 instances at %.0f).\n\n", budget/10, budget/50))
	b.WriteString("Compare that against your real publish rate, instance count and payload\n")
	b.WriteString("size. If you are an order of magnitude below it, S2/S4 is not worth\n")
	b.WriteString("building. If you are close, Redis egress -- not server CPU -- is the wall\n")
	b.WriteString("you will hit first, and it is the thing S2/S4 actually fixes.")
	return b.String()
}

func interpret(perDiscard float64) string {
	var b strings.Builder
	switch {
	case perDiscard*100_000 < 0.10:
		b.WriteString("Server CPU verdict: even at 100k messages/sec, discarding costs each\n")
		b.WriteString("instance under a tenth of a core. Server CPU is not the constraint here;\n")
		b.WriteString("Redis egress above is.")
	case perDiscard*10_000 < 0.10:
		b.WriteString("Server CPU verdict: negligible below ~10k messages/sec.")
	default:
		b.WriteString("Server CPU verdict: a real cost at moderate publish rates, on top of the\n")
		b.WriteString("Redis egress above.")
	}
	return b.String()
}
