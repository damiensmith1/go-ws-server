// Package wsserver embeds the WebSocket server in another Go program.
//
// The command in the repository root is a thin wrapper over this package:
// it reads Options from the environment and runs them. Embedding it
// directly is what you want when the server is one component of a larger
// binary, or when policy has to be supplied in code rather than through
// environment variables.
//
//	srv, err := wsserver.New(wsserver.Options{
//	    ListenAddr: ":8080",
//	    RedisAddrs: []string{"localhost:6379"},
//	    Verifier:   myVerifier,
//	    Authorizer: myAuthorizer,
//	})
//	if err != nil {
//	    return err
//	}
//	return srv.Run(ctx)
//
// Options is plain data with useful zero values, so only the fields that
// differ from the defaults need setting. Verifier, Authorizer and Metrics
// are the three seams that cannot be expressed as configuration; leaving
// any of them nil falls back to the same default the command uses.
//
// To keep environment-based configuration, call OptionsFromEnv and adjust
// what you need:
//
//	opts, err := wsserver.OptionsFromEnv()
//	opts.Authorizer = myAuthorizer
package wsserver

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/damiensmith1/go-ws-server/auth"
	"github.com/damiensmith1/go-ws-server/authz"
	"github.com/damiensmith1/go-ws-server/bus"
	"github.com/damiensmith1/go-ws-server/internal/app"
	"github.com/damiensmith1/go-ws-server/internal/config"
	"github.com/damiensmith1/go-ws-server/internal/logger"
	"github.com/damiensmith1/go-ws-server/metrics"
)

// Options configures a Server. The zero value is usable: it listens on a
// free port, talks to Redis on localhost:6379, accepts any client in
// insecure dev mode and allows every topic.
type Options struct {
	// ListenAddr is the websocket listen address, e.g. ":8080". Empty
	// listens on a free port, which Server.Addr then reports.
	ListenAddr string

	// MetricsAddr is a separate listen address for /metrics, e.g. ":9090".
	// Empty disables the endpoint. Keeping it off the websocket port is
	// deliberate: runtime internals should not be reachable by clients.
	MetricsAddr string

	// RedisAddrs selects the Redis topology. Empty means localhost:6379;
	// one address is a plain client; several are a Cluster client. Setting
	// RedisMasterName makes it a Sentinel failover client instead.
	RedisAddrs      []string
	RedisPassword   string
	RedisMasterName string

	// Verifier authenticates upgrade requests. Nil selects a JWT verifier
	// when JWTSecret is set, and otherwise the insecure dev verifier,
	// which accepts any ?userKey= and logs a warning.
	Verifier auth.Verifier

	// JWTSecret, JWTAudience and JWTIssuer configure the built-in HS256
	// verifier. Ignored when Verifier is non-nil.
	JWTSecret   string
	JWTAudience string
	JWTIssuer   string

	// Authorizer gates per-topic access. Nil allows every topic to every
	// authenticated client, which is only appropriate for a single tenant.
	Authorizer authz.Authorizer

	// Metrics is the collector set. Nil creates a private one, reachable
	// afterwards through Server.Metrics.
	Metrics *metrics.Metrics

	// Logger receives structured logs. Nil logs to stdout at LogLevel.
	Logger   *slog.Logger
	LogLevel string

	// AllowedOrigins is the Origin allowlist for upgrades. Empty keeps
	// gorilla's same-origin default; a single "*" allows all.
	AllowedOrigins []string

	// TLSCertPath and TLSKeyPath serve wss:// directly. Usually better
	// terminated at a load balancer.
	TLSCertPath string
	TLSKeyPath  string

	// IdleTimeout closes a socket with no inbound traffic. Zero disables.
	IdleTimeout time.Duration

	// MaxPayloadBytes rejects larger inbound frames. Zero uses 64 KiB.
	MaxPayloadBytes int64

	// MaxBufferedBytes is the per-socket outbound buffer above which
	// fan-out messages are dropped. Zero uses 1 MiB.
	MaxBufferedBytes int64

	// MaxConsecutiveDrops evicts a connection after this many back-to-back
	// drops. Negative disables eviction; zero uses the default of 100.
	MaxConsecutiveDrops int

	// StreamMaxLength bounds each topic's replay stream. Zero uses 1000.
	StreamMaxLength int64
	// StreamTTL expires idle replay streams. Zero keeps them indefinitely.
	StreamTTL time.Duration

	// MessagesPerSec and JobsPerMin are per-userKey rate limits. Zero uses
	// 50 and 30 respectively.
	MessagesPerSec int
	JobsPerMin     int

	// SchedulerAllowedHosts is the SSRF allowlist for outbound scheduled
	// requests. Empty allows any host that passes the guard's private-range
	// checks.
	SchedulerAllowedHosts []string

	// ReadinessTimeout bounds the Redis ping behind /readyz. Zero uses 2s.
	ReadinessTimeout time.Duration

	// ShutdownTimeout bounds graceful shutdown, including flushing queued
	// frames. Zero uses 10s.
	ShutdownTimeout time.Duration

	// InstanceID identifies this instance in scheduler lock ownership and
	// logs. Empty generates a UUID.
	InstanceID string
}

// OptionsFromEnv reads the same environment variables the command reads.
// See .env.example for the full list.
func OptionsFromEnv() (Options, error) {
	cfg, err := config.Load()
	if err != nil {
		return Options{}, err
	}
	rules, err := authz.ParseRules(cfg.AuthzRules)
	if err != nil {
		return Options{}, err
	}
	opts := Options{
		ListenAddr:            ":" + strconv.Itoa(cfg.WebSocketPort),
		MetricsAddr:           cfg.MetricsAddr,
		RedisAddrs:            cfg.RedisAddrList(),
		RedisPassword:         cfg.RedisPassword,
		RedisMasterName:       cfg.RedisMasterName,
		JWTSecret:             cfg.AuthJWTSecret,
		JWTAudience:           cfg.AuthJWTAudience,
		JWTIssuer:             cfg.AuthJWTIssuer,
		LogLevel:              cfg.LogLevel,
		AllowedOrigins:        cfg.AllowedOrigins,
		TLSCertPath:           cfg.TLSCertPath,
		TLSKeyPath:            cfg.TLSKeyPath,
		IdleTimeout:           cfg.WebSocketTimeout,
		MaxPayloadBytes:       cfg.MaxPayloadBytes,
		MaxBufferedBytes:      cfg.MaxBufferedBytes,
		MaxConsecutiveDrops:   cfg.MaxConsecutiveDrops,
		StreamMaxLength:       cfg.StreamMaxLength,
		StreamTTL:             cfg.StreamTTL,
		MessagesPerSec:        cfg.RateLimitMessagesPerSec,
		JobsPerMin:            cfg.RateLimitJobsPerMin,
		SchedulerAllowedHosts: cfg.SchedulerAllowedHosts,
		ReadinessTimeout:      cfg.ReadinessTimeout,
		ShutdownTimeout:       cfg.ShutdownTimeout,
		InstanceID:            cfg.InstanceID,
	}
	if rules != nil {
		opts.Authorizer = rules
	}
	// The two layers spell "no eviction" differently: the environment uses
	// 0, while Options reserves 0 for "unset, use the default" so a zero
	// struct is usable. Translate, or MAX_CONSECUTIVE_DROPS=0 would come
	// back on at the default of 100.
	if cfg.MaxConsecutiveDrops == 0 {
		opts.MaxConsecutiveDrops = -1
	}
	return opts, nil
}

// Server is a configured, listening instance. Run starts it.
type Server struct {
	app *app.App
	log *slog.Logger
}

// New binds the listeners and wires every subsystem. It returns an error
// without leaking resources if anything fails, so a failed New needs no
// cleanup.
func New(opts Options) (*Server, error) {
	log := opts.Logger
	if log == nil {
		level := opts.LogLevel
		if level == "" {
			level = "info"
		}
		log = logger.New(os.Stdout, level)
	}

	cfg, err := opts.toConfig()
	if err != nil {
		return nil, err
	}
	a, err := app.New(cfg, log, app.Ext{
		Verifier:   opts.Verifier,
		Authorizer: opts.Authorizer,
		Metrics:    opts.Metrics,
	})
	if err != nil {
		return nil, err
	}
	return &Server{app: a, log: log}, nil
}

// Run starts every subsystem and blocks until ctx is cancelled or a
// subsystem fails. It returns nil on a clean shutdown.
func (s *Server) Run(ctx context.Context) error { return s.app.Run(ctx) }

// Addr reports the resolved listen address, which is how you discover the
// port when ListenAddr asked for a free one.
func (s *Server) Addr() string { return s.app.Addr() }

// Port reports the resolved TCP port.
func (s *Server) Port() int { return s.app.Port() }

// Metrics returns the collector set, whether supplied in Options or
// created here. Use Registry to expose it on your own HTTP server instead
// of setting MetricsAddr.
func (s *Server) Metrics() *metrics.Metrics { return s.app.Metrics() }

// Bus returns the message bus, for publishing from inside the host
// process without a websocket client round trip.
func (s *Server) Bus() *bus.Bus { return s.app.Bus() }

// toConfig maps Options onto the internal configuration, applying the
// documented zero-value defaults. Options exists precisely so that
// internal/config never appears in the public API and can change freely.
func (o Options) toConfig() (*config.Config, error) {
	port := 0
	if o.ListenAddr != "" {
		_, portStr, err := net.SplitHostPort(o.ListenAddr)
		if err != nil {
			return nil, fmt.Errorf("parse ListenAddr %q: %w", o.ListenAddr, err)
		}
		if port, err = strconv.Atoi(portStr); err != nil {
			return nil, fmt.Errorf("parse ListenAddr port %q: %w", portStr, err)
		}
	}

	addrs := o.RedisAddrs
	if len(addrs) == 0 {
		addrs = []string{"localhost:6379"}
	}

	// Negative disables eviction; zero means "unset", so it takes the
	// default. Without the distinction there is no way to ask for no
	// eviction through a zero-valued struct.
	drops := o.MaxConsecutiveDrops
	switch {
	case drops < 0:
		drops = 0
	case drops == 0:
		drops = 100
	}

	cfg := &config.Config{
		RedisAddrs:              addrs,
		RedisPassword:           o.RedisPassword,
		RedisMasterName:         o.RedisMasterName,
		WebSocketPort:           port,
		MetricsAddr:             o.MetricsAddr,
		AllowedOrigins:          o.AllowedOrigins,
		TLSCertPath:             o.TLSCertPath,
		TLSKeyPath:              o.TLSKeyPath,
		AuthJWTSecret:           o.JWTSecret,
		AuthJWTAudience:         o.JWTAudience,
		AuthJWTIssuer:           o.JWTIssuer,
		WebSocketTimeout:        o.IdleTimeout,
		MaxPayloadBytes:         orInt64(o.MaxPayloadBytes, 64*1024),
		MaxBufferedBytes:        orInt64(o.MaxBufferedBytes, 1024*1024),
		MaxConsecutiveDrops:     drops,
		StreamMaxLength:         orInt64(o.StreamMaxLength, 1000),
		StreamTTL:               o.StreamTTL,
		RateLimitMessagesPerSec: orInt(o.MessagesPerSec, 50),
		RateLimitJobsPerMin:     orInt(o.JobsPerMin, 30),
		SchedulerAllowedHosts:   o.SchedulerAllowedHosts,
		ReadinessTimeout:        orDuration(o.ReadinessTimeout, config.DefaultReadinessTimeout),
		ShutdownTimeout:         orDuration(o.ShutdownTimeout, config.DefaultShutdownTimeout),
		LogLevel:                o.LogLevel,
		InstanceID:              o.InstanceID,
	}
	if cfg.InstanceID == "" {
		cfg.InstanceID = config.NewInstanceID()
	}
	return cfg, nil
}

func orInt(v, fallback int) int {
	if v == 0 {
		return fallback
	}
	return v
}

func orInt64(v, fallback int64) int64 {
	if v == 0 {
		return fallback
	}
	return v
}

func orDuration(v, fallback time.Duration) time.Duration {
	if v == 0 {
		return fallback
	}
	return v
}
