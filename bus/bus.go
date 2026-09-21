// Package bus is the cross-instance message-delivery layer.
//
// It uses two Redis primitives:
//
//   - Pub/Sub for live fan-out. PSUBSCRIBE on "ws:topic:*" and
//     "ws:broadcast:*" picks up every publish from every server instance.
//   - Streams for replay history. Each topic gets a bounded stream at
//     "topic:stream:<topic>" sized by STREAM_MAX_LENGTH. Late subscribers
//     XRANGE from a `since` cursor.
//
// The bus is the only place that has to handle the live-vs-replay race.
// When a client subscribes with `since`, we:
//
//  1. Register the subscriber AND set buffering=true (one critical section)
//     so any live publish that arrives mid-replay is buffered, not sent.
//  2. XRANGE the stream, send each entry tagged replay=true, mark its
//     streamId as already-replayed.
//  3. Flip buffering=false and drain the buffer, skipping IDs we've
//     already replayed.
//
// Pub/Sub fan-out to subscribers is non-blocking: if a slow client's send
// channel is full we drop and log, so one stalled client cannot back up
// the entire bus.
package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/damiensmith1/go-ws-server/metrics"
	"github.com/damiensmith1/go-ws-server/protocol"
)

const (
	topicChannelPrefix     = "ws:topic:"
	broadcastChannelPrefix = "ws:broadcast:"
	streamKeyPrefix        = "topic:stream:"
)

// Subscriber is the minimal interface a recipient must satisfy. Send must
// be non-blocking — the bus relies on it to drop fan-out messages rather
// than block one client at the cost of all the others.
type Subscriber interface {
	Send(msg []byte)
}

// BroadcastTarget is implemented by the connection hub. The bus calls it
// to deliver per-userKey broadcast frames to every socket for that user.
type BroadcastTarget interface {
	SendToUser(userKey string, msg []byte)
}

// Config configures the bus.
type Config struct {
	StreamMaxLength int64
	StreamTTL       time.Duration

	// Metrics is optional. A nil value gets a private collector set, so
	// call sites never need a nil check.
	Metrics *metrics.Metrics
}

// Bus is the running pub/sub + replay coordinator.
type Bus struct {
	cfg       Config
	pub       redis.UniversalClient
	sub       redis.UniversalClient
	log       *slog.Logger
	broadcast BroadcastTarget

	pubsub *redis.PubSub
	m      *metrics.Metrics

	mu        sync.Mutex
	topicSubs map[string]map[Subscriber]struct{}
	replay    map[replayKey]*replayState

	once sync.Once
	done chan struct{}
}

type replayKey struct {
	sub   Subscriber
	topic string
}

type replayState struct {
	buffering bool
	seen      map[string]bool
	buf       []bufferedMsg
}

type bufferedMsg struct {
	streamID string
	data     json.RawMessage
}

// New constructs a Bus. The two redis clients should be distinct: pub
// handles regular commands (XADD, XRANGE, PUBLISH); sub holds the
// PSUBSCRIBE.
func New(pub, sub redis.UniversalClient, target BroadcastTarget, cfg Config, log *slog.Logger) *Bus {
	if log == nil {
		log = slog.Default()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.New()
	}
	return &Bus{
		cfg:       cfg,
		m:         cfg.Metrics,
		pub:       pub,
		sub:       sub,
		log:       log,
		broadcast: target,
		topicSubs: make(map[string]map[Subscriber]struct{}),
		replay:    make(map[replayKey]*replayState),
		done:      make(chan struct{}),
	}
}

// Run starts the pub/sub listener and (if configured) the periodic stream
// trim. It returns when ctx is cancelled.
func (b *Bus) Run(ctx context.Context) error {
	b.pubsub = b.sub.PSubscribe(ctx, topicChannelPrefix+"*", broadcastChannelPrefix+"*")

	if err := b.pubsub.Ping(ctx); err != nil {
		return fmt.Errorf("psubscribe ping: %w", err)
	}

	ch := b.pubsub.Channel()

	if b.cfg.StreamTTL > 0 {
		go b.runTrimLoop(ctx)
	}

	b.log.Info("bus initialized",
		"streamMaxLength", b.cfg.StreamMaxLength,
		"streamTtlSeconds", int64(b.cfg.StreamTTL.Seconds()),
	)

	for {
		select {
		case <-ctx.Done():
			close(b.done)
			return nil
		case msg, ok := <-ch:
			if !ok {
				close(b.done)
				return nil
			}
			b.dispatch(msg)
		}
	}
}

// Close releases the pubsub subscription. Must be called after the Run
// goroutine exits.
func (b *Bus) Close() error {
	b.once.Do(func() {
		if b.pubsub != nil {
			_ = b.pubsub.Close()
		}
	})
	return nil
}

func (b *Bus) dispatch(msg *redis.Message) {
	switch {
	case strings.HasPrefix(msg.Channel, topicChannelPrefix):
		topic := msg.Channel[len(topicChannelPrefix):]
		b.handleTopic(topic, msg.Payload)
	case strings.HasPrefix(msg.Channel, broadcastChannelPrefix):
		userKey := msg.Channel[len(broadcastChannelPrefix):]
		b.handleBroadcast(userKey, msg.Payload)
	}
}

type wireTopicPayload struct {
	StreamID string          `json:"streamId"`
	Data     json.RawMessage `json:"data"`
}

func (b *Bus) handleTopic(topic, payload string) {
	var p wireTopicPayload
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		b.log.Error("invalid topic payload", "channel", topic, "err", err.Error())
		return
	}

	// Decide the entire fan-out in one critical section: buffer for any
	// subscriber mid-replay, collect the rest. Taking the lock once per
	// subscriber instead serialises every topic's fan-out on b.mu as soon
	// as a single topic has many subscribers.
	start := time.Now()
	buffered := 0

	b.mu.Lock()
	deliver := make([]Subscriber, 0, len(b.topicSubs[topic]))
	for sub := range b.topicSubs[topic] {
		if st := b.replay[replayKey{sub, topic}]; st != nil && st.buffering {
			st.buf = append(st.buf, bufferedMsg{streamID: p.StreamID, data: p.Data})
			buffered++
			continue
		}
		deliver = append(deliver, sub)
	}
	b.mu.Unlock()

	b.m.FanoutSubscribers.Observe(float64(len(deliver)))
	b.m.FanoutBuffered.Add(float64(buffered))
	defer func() { b.m.FanoutDuration.Observe(time.Since(start).Seconds()) }()

	if len(deliver) == 0 {
		return
	}

	// Encode once and share the frame across subscribers: Send only
	// enqueues the slice and the writer goroutine never mutates it.
	frame := protocol.EncodePublish(topic, p.Data, p.StreamID, false)
	for _, sub := range deliver {
		sub.Send(frame)
	}
}

type wireBroadcastPayload struct {
	Data json.RawMessage `json:"data"`
}

func (b *Bus) handleBroadcast(userKey, payload string) {
	if b.broadcast == nil {
		return
	}
	var p wireBroadcastPayload
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		b.log.Error("invalid broadcast payload", "channel", userKey, "err", err.Error())
		return
	}
	b.broadcast.SendToUser(userKey, protocol.EncodeBroadcast(p.Data))
}

// PublishTopic appends to the topic stream and announces via pub/sub.
// Returns the stream ID assigned by Redis.
func (b *Bus) PublishTopic(ctx context.Context, topic string, data json.RawMessage) (string, error) {
	streamKey := streamKeyPrefix + topic
	dataJSON := []byte(data)
	if len(dataJSON) == 0 {
		dataJSON = []byte("null")
	}
	streamID, err := b.pub.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: b.cfg.StreamMaxLength,
		Approx: true,
		Values: map[string]any{"d": string(dataJSON)},
	}).Result()
	if err != nil {
		return "", fmt.Errorf("xadd: %w", err)
	}

	announce, _ := json.Marshal(wireTopicPayload{StreamID: streamID, Data: data})
	if err := b.pub.Publish(ctx, topicChannelPrefix+topic, announce).Err(); err != nil {
		return "", fmt.Errorf("publish topic: %w", err)
	}
	b.m.PublishTotal.Inc()
	return streamID, nil
}

// PublishBroadcast announces a per-userKey broadcast across instances.
// The bus subscriber on each instance dispatches to local sockets via
// BroadcastTarget.
func (b *Bus) PublishBroadcast(ctx context.Context, userKey string, data json.RawMessage) error {
	announce, _ := json.Marshal(wireBroadcastPayload{Data: data})
	if err := b.pub.Publish(ctx, broadcastChannelPrefix+userKey, announce).Err(); err != nil {
		return fmt.Errorf("publish broadcast: %w", err)
	}
	return nil
}

// AddLocalSubscription is the simple variant: register a subscriber for
// live messages only, no replay.
func (b *Bus) AddLocalSubscription(topic string, sub Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.addLocalSubLocked(topic, sub)
}

func (b *Bus) addLocalSubLocked(topic string, sub Subscriber) {
	set, ok := b.topicSubs[topic]
	if !ok {
		set = make(map[Subscriber]struct{})
		b.topicSubs[topic] = set
	}
	if _, dup := set[sub]; !dup {
		set[sub] = struct{}{}
		b.m.LocalSubscriptions.Inc()
	}
}

// RemoveLocalSubscription removes a subscriber from a topic. Also clears
// any replay state (buffering would no longer find a recipient).
func (b *Bus) RemoveLocalSubscription(topic string, sub Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if set, ok := b.topicSubs[topic]; ok {
		if _, present := set[sub]; present {
			delete(set, sub)
			b.m.LocalSubscriptions.Dec()
		}
		if len(set) == 0 {
			delete(b.topicSubs, topic)
		}
	}
	delete(b.replay, replayKey{sub, topic})
}

// RemoveSubscriberAll drops every subscription owned by sub. Called when a
// connection closes.
func (b *Bus) RemoveSubscriberAll(sub Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for topic, set := range b.topicSubs {
		if _, ok := set[sub]; ok {
			delete(set, sub)
			b.m.LocalSubscriptions.Dec()
			if len(set) == 0 {
				delete(b.topicSubs, topic)
			}
		}
	}
	for k := range b.replay {
		if k.sub == sub {
			delete(b.replay, k)
		}
	}
}

// SubscribeWithReplay registers sub and replays any messages newer than
// the `since` cursor. Returns truncation info: if `since` predates the
// oldest entry currently retained in the stream, the caller should emit a
// `replayTruncated` frame.
func (b *Bus) SubscribeWithReplay(ctx context.Context, sub Subscriber, topic, since string) (truncated bool, oldestAvailable string, err error) {
	cursor, err := parseSinceCursor(since)
	if err != nil {
		return false, "", err
	}
	start := time.Now()
	defer func() { b.m.ReplayDuration.Observe(time.Since(start).Seconds()) }()

	// 1. Register subscriber AND mark buffering under a single critical
	//    section. From this moment on, any live publish on this topic to
	//    this sub will be buffered instead of sent.
	b.mu.Lock()
	b.addLocalSubLocked(topic, sub)
	b.replay[replayKey{sub, topic}] = &replayState{
		buffering: true,
		seen:      make(map[string]bool),
	}
	b.mu.Unlock()

	// 2. Drain the stream from the cursor.
	msgs, trunc, oldest, err := b.replayMessages(ctx, topic, cursor)
	if err != nil {
		// Best-effort cleanup of the buffering state on failure.
		b.mu.Lock()
		delete(b.replay, replayKey{sub, topic})
		b.mu.Unlock()
		return false, "", err
	}

	b.m.ReplayMessages.Add(float64(len(msgs)))
	if trunc {
		b.m.ReplayTruncated.Inc()
	}
	for _, m := range msgs {
		b.mu.Lock()
		if st := b.replay[replayKey{sub, topic}]; st != nil {
			st.seen[m.streamID] = true
		}
		b.mu.Unlock()
		sub.Send(protocol.EncodePublish(topic, m.data, m.streamID, true))
	}

	// 3. Stop buffering; drain any concurrent live messages, skipping IDs
	//    we already replayed. Send them as live (replay=false).
	b.mu.Lock()
	st := b.replay[replayKey{sub, topic}]
	var leftover []bufferedMsg
	if st != nil {
		st.buffering = false
		for _, m := range st.buf {
			if !st.seen[m.streamID] {
				leftover = append(leftover, m)
			}
		}
	}
	delete(b.replay, replayKey{sub, topic})
	b.mu.Unlock()

	for _, m := range leftover {
		sub.Send(protocol.EncodePublish(topic, m.data, m.streamID, false))
	}

	return trunc, oldest, nil
}

func (b *Bus) replayMessages(ctx context.Context, topic, since string) ([]bufferedMsg, bool, string, error) {
	streamKey := streamKeyPrefix + topic

	// Pass only the millisecond prefix to XRANGE; Redis treats `<ms>` as
	// `<ms>-0`. We post-filter using compareStreamIDs so the seq portion
	// of the cursor is honored exclusively.
	sinceMs := since
	if i := strings.IndexByte(since, '-'); i > 0 {
		sinceMs = since[:i]
	}

	entries, err := b.pub.XRange(ctx, streamKey, sinceMs, "+").Result()
	if err != nil {
		return nil, false, "", fmt.Errorf("xrange: %w", err)
	}

	out := make([]bufferedMsg, 0, len(entries))
	for _, e := range entries {
		if compareStreamIDs(e.ID, since) <= 0 {
			continue
		}
		dataStr, _ := e.Values["d"].(string)
		out = append(out, bufferedMsg{streamID: e.ID, data: json.RawMessage(dataStr)})
	}

	// `since` of `0-0` is the universal "from the beginning" sentinel —
	// no truncation flag even if entries older than 0 don't exist.
	truncated := false
	oldest := ""
	if sinceMsNum, _ := strconv.ParseInt(sinceMs, 10, 64); sinceMsNum > 0 {
		oldestEntries, err := b.pub.XRangeN(ctx, streamKey, "-", "+", 1).Result()
		if err == nil && len(oldestEntries) > 0 {
			if compareStreamIDs(since, oldestEntries[0].ID) < 0 {
				truncated = true
				oldest = oldestEntries[0].ID
			}
		}
	}

	return out, truncated, oldest, nil
}

func (b *Bus) runTrimLoop(ctx context.Context) {
	period := b.cfg.StreamTTL / 4
	if period < time.Minute {
		period = time.Minute
	}
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.trimAllStreams(ctx)
		}
	}
}

func (b *Bus) trimAllStreams(ctx context.Context) {
	cutoffMs := time.Now().Add(-b.cfg.StreamTTL).UnixMilli()
	minID := fmt.Sprintf("%d-0", cutoffMs)
	iter := b.pub.Scan(ctx, 0, streamKeyPrefix+"*", 100).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		if err := b.pub.XTrimMinID(ctx, key, minID).Err(); err != nil {
			b.log.Warn("xtrim failed", "key", key, "err", err.Error())
		}
	}
	if err := iter.Err(); err != nil {
		b.log.Warn("scan streams failed", "err", err.Error())
	}
}

// parseSinceCursor accepts:
//   - a stream ID like "1714589342175-0"
//   - all-digit milliseconds-since-epoch
//   - an ISO 8601 timestamp
//
// In every case it returns a "<ms>-<seq>" string suitable as a cursor.
var streamIDPattern = regexp.MustCompile(`^\d+-\d+$`)
var allDigitsPattern = regexp.MustCompile(`^\d+$`)

func parseSinceCursor(since string) (string, error) {
	if since == "" {
		return "", errors.New("empty since cursor")
	}
	if streamIDPattern.MatchString(since) {
		return since, nil
	}
	if allDigitsPattern.MatchString(since) {
		return since + "-0", nil
	}
	t, err := time.Parse(time.RFC3339Nano, since)
	if err != nil {
		return "", fmt.Errorf("Invalid `since`: must be ISO timestamp or stream ID (got %q)", since)
	}
	return fmt.Sprintf("%d-0", t.UnixMilli()), nil
}

// compareStreamIDs returns -1, 0, or 1 like strings.Compare, but treats
// the IDs as (ms, seq) pairs so "1000-2" < "1000-10".
func compareStreamIDs(a, b string) int {
	aMs, aSeq := splitStreamID(a)
	bMs, bSeq := splitStreamID(b)
	if aMs != bMs {
		if aMs < bMs {
			return -1
		}
		return 1
	}
	if aSeq != bSeq {
		if aSeq < bSeq {
			return -1
		}
		return 1
	}
	return 0
}

func splitStreamID(id string) (int64, int64) {
	i := strings.IndexByte(id, '-')
	if i < 0 {
		ms, _ := strconv.ParseInt(id, 10, 64)
		return ms, 0
	}
	ms, _ := strconv.ParseInt(id[:i], 10, 64)
	seq, _ := strconv.ParseInt(id[i+1:], 10, 64)
	return ms, seq
}
