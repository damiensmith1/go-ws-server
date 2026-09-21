// Package metrics holds every Prometheus collector the server exports.
//
// Two rules shape what is in here.
//
// Cardinality: nothing is ever labelled by topic, userKey, job ID, or any
// other value a client controls. Those are unbounded, and one label with
// a million values is one million time series. Where a breakdown is
// useful it uses a closed set the server defines — a frame type, a close
// reason, a Redis command name.
//
// Cost when unscraped: collectors are always constructed and always
// updated, so no call site needs a nil check or a branch in a hot path.
// Only the HTTP listener is conditional. An unscraped counter is an
// atomic add.
//
// Collectors register into a Registry owned by the Metrics value rather
// than prometheus.DefaultRegisterer, so tests can build isolated
// instances and two servers can run in one process.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Drop reasons for FramesDropped.
const (
	DropBufferThreshold = "buffer_threshold"
	DropChannelFull     = "channel_full"
)

// Connection close reasons for ConnectionsClosed.
const (
	CloseIdleTimeout  = "idle_timeout"
	ClosePeer         = "peer"
	CloseServer       = "server"
	CloseReadError    = "read_error"
	CloseTokenExpired = "token_expired"
	CloseSlowConsumer = "slow_consumer"
)

// Upgrade outcomes for ConnectionsTotal.
const (
	UpgradeAccepted      = "accepted"
	UpgradeRejectedAuth  = "rejected_auth"
	UpgradeRejectedOther = "rejected_other"
)

// Metrics is the full collector set. Pass one value everywhere; it is
// safe for concurrent use.
type Metrics struct {
	reg *prometheus.Registry

	ConnectionsActive  prometheus.Gauge
	ConnectionsTotal   *prometheus.CounterVec // result
	ConnectionsClosed  *prometheus.CounterVec // reason
	ConnectionDuration prometheus.Histogram

	FramesReceived *prometheus.CounterVec // type
	FramesSent     prometheus.Counter
	FramesDropped  *prometheus.CounterVec // reason

	PublishTotal       prometheus.Counter
	FanoutSubscribers  prometheus.Histogram
	FanoutDuration     prometheus.Histogram
	FanoutBuffered     prometheus.Counter
	LocalSubscriptions prometheus.Gauge

	ReplayMessages  prometheus.Counter
	ReplayDuration  prometheus.Histogram
	ReplayTruncated prometheus.Counter

	RedisDuration *prometheus.HistogramVec // command
	RedisErrors   *prometheus.CounterVec   // command

	RateLimitDecisions *prometheus.CounterVec // bucket, decision
	AuthzDecisions     *prometheus.CounterVec // action, decision

	SchedulerExecuted   *prometheus.CounterVec // result
	SchedulerDuration   prometheus.Histogram
	SchedulerQueueDepth prometheus.Gauge
	SchedulerClaimLost  prometheus.Counter
}

// New builds and registers every collector.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	f := promauto(reg)

	m := &Metrics{
		reg: reg,

		ConnectionsActive: f.gauge("ws_connections_active",
			"Websocket connections currently open on this instance."),
		ConnectionsTotal: f.counterVec("ws_connections_total",
			"Upgrade attempts by outcome.", "result"),
		ConnectionsClosed: f.counterVec("ws_connections_closed_total",
			"Closed connections by reason.", "reason"),
		ConnectionDuration: f.histogram("ws_connection_duration_seconds",
			"How long connections stayed open.",
			// Seconds to hours: short-lived probes through day-long sockets.
			prometheus.ExponentialBuckets(1, 4, 9)),

		FramesReceived: f.counterVec("ws_frames_received_total",
			"Inbound frames by protocol message type.", "type"),
		FramesSent: f.counter("ws_frames_sent_total",
			"Frames handed to a connection's writer."),
		FramesDropped: f.counterVec("ws_frames_dropped_total",
			"Frames dropped rather than queued, by reason.", "reason"),

		PublishTotal: f.counter("bus_publish_total",
			"Messages appended to a topic stream and announced."),
		FanoutSubscribers: f.histogram("bus_fanout_subscribers",
			"Local subscribers delivered to per published message.",
			prometheus.ExponentialBuckets(1, 4, 8)),
		FanoutDuration: f.histogram("bus_fanout_duration_seconds",
			"Time to decide and enqueue one message's fan-out.",
			prometheus.ExponentialBuckets(0.00001, 4, 9)),
		FanoutBuffered: f.counter("bus_fanout_buffered_total",
			"Messages buffered because a subscriber was mid-replay."),
		LocalSubscriptions: f.gauge("bus_local_subscriptions",
			"Topic subscriptions held by this instance."),

		ReplayMessages: f.counter("bus_replay_messages_total",
			"Historical messages sent to replaying subscribers."),
		ReplayDuration: f.histogram("bus_replay_duration_seconds",
			"Time to drain a subscriber's replay backlog.",
			prometheus.ExponentialBuckets(0.001, 4, 8)),
		ReplayTruncated: f.counter("bus_replay_truncated_total",
			"Replays that began after the oldest retained entry."),

		RedisDuration: f.histogramVec("redis_command_duration_seconds",
			"Redis command latency.",
			prometheus.ExponentialBuckets(0.0001, 4, 9), "command"),
		RedisErrors: f.counterVec("redis_command_errors_total",
			"Redis commands returning an error (redis.Nil excluded).", "command"),

		RateLimitDecisions: f.counterVec("ratelimit_decisions_total",
			"Rate-limit checks by bucket and outcome.", "bucket", "decision"),

		// "error" is a separate outcome from "denied" on purpose: a spike
		// in denials is a client or policy problem, a spike in errors is
		// an outage in whatever backs the Authorizer.
		AuthzDecisions: f.counterVec("authz_decisions_total",
			"Per-topic authorization checks by action and outcome.", "action", "decision"),

		SchedulerExecuted: f.counterVec("scheduler_jobs_executed_total",
			"Scheduled jobs run by outcome.", "result"),
		SchedulerDuration: f.histogram("scheduler_job_duration_seconds",
			"Outbound scheduled request duration.",
			prometheus.ExponentialBuckets(0.001, 4, 8)),
		SchedulerQueueDepth: f.gauge("scheduler_queue_depth",
			"Jobs currently in the scheduler sorted set."),
		SchedulerClaimLost: f.counter("scheduler_claim_lost_total",
			"Due jobs another instance claimed first."),
	}

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Handler serves the registry in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// Registry exposes the underlying registry, for tests and for callers
// that want to add their own collectors.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// factory trims the repetition of building and registering collectors.
type factory struct{ reg *prometheus.Registry }

func promauto(reg *prometheus.Registry) factory { return factory{reg} }

func (f factory) gauge(name, help string) prometheus.Gauge {
	c := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
	f.reg.MustRegister(c)
	return c
}

func (f factory) counter(name, help string) prometheus.Counter {
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
	f.reg.MustRegister(c)
	return c
}

func (f factory) counterVec(name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	f.reg.MustRegister(c)
	return c
}

func (f factory) histogram(name, help string, buckets []float64) prometheus.Histogram {
	c := prometheus.NewHistogram(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets})
	f.reg.MustRegister(c)
	return c
}

func (f factory) histogramVec(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	c := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labels)
	f.reg.MustRegister(c)
	return c
}
