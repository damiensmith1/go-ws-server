// Package app wires the server's components into a runnable unit. main()
// is now a thin shim that loads config, builds an App, and runs it; tests
// import this package directly so they can boot the whole stack against
// a containerized Redis on a random port.
package app

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"

	"github.com/damiensmith1/go-ws-server/auth"
	"github.com/damiensmith1/go-ws-server/authz"
	"github.com/damiensmith1/go-ws-server/bus"
	"github.com/damiensmith1/go-ws-server/internal/config"
	"github.com/damiensmith1/go-ws-server/internal/connection"
	"github.com/damiensmith1/go-ws-server/internal/ratelimit"
	"github.com/damiensmith1/go-ws-server/internal/redisx"
	"github.com/damiensmith1/go-ws-server/internal/scheduler"
	"github.com/damiensmith1/go-ws-server/internal/ssrf"
	"github.com/damiensmith1/go-ws-server/metrics"
)

// App holds every long-lived resource. New constructs it; Run blocks
// until the supplied context is cancelled or a subsystem fails fatally.
type App struct {
	cfg      *config.Config
	log      *slog.Logger
	rdb      redis.UniversalClient
	pub      redis.UniversalClient
	sub      redis.UniversalClient
	hub      *connection.Hub
	bus      *bus.Bus
	sched    *scheduler.Scheduler
	server   *http.Server
	listener net.Listener
	verifier auth.Verifier
	metrics  *metrics.Metrics
	metricsS *http.Server
}

// originChecker builds the upgrader's CheckOrigin. Returning nil leaves
// gorilla's default in place: allow requests carrying no Origin header,
// and otherwise require Origin to match Host.
//
// With an allowlist configured, a request with no Origin header is still
// allowed — browsers always send one, so its absence means a non-browser
// client such as a CLI or another service. Any Origin that is present
// must appear in the list. A single "*" entry allows every origin and is
// intended for local development only.
func originChecker(allowed []string, log *slog.Logger) func(*http.Request) bool {
	if len(allowed) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(allowed))
	wildcard := false
	for _, o := range allowed {
		if o == "*" {
			wildcard = true
		}
		set[strings.ToLower(o)] = struct{}{}
	}
	return func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" || wildcard {
			return true
		}
		if _, ok := set[strings.ToLower(origin)]; ok {
			return true
		}
		log.Warn("rejected websocket upgrade: origin not allowed", "origin", origin)
		return false
	}
}

// metricsServer builds the /metrics listener. It is deliberately separate
// from the websocket server: the websocket port is usually public, and
// process and Go runtime internals should not be.
// readyHandler reports whether this instance can actually serve traffic.
//
// /healthz answers "the process is up", which an orchestrator can get from
// the TCP connect alone. /readyz answers the question that decides routing:
// is Redis reachable? Every meaningful operation this server performs —
// fan-out, replay, presence, locks, scheduling — is a Redis round trip, so
// an instance whose Redis is gone is a black hole that still accepts
// sockets. Pinging on each probe rather than caching a background result
// keeps the answer honest at the moment it is asked; the probe interval is
// the orchestrator's to choose.
func readyHandler(rdb redis.UniversalClient, timeout time.Duration, log *slog.Logger) http.HandlerFunc {
	if timeout <= 0 {
		timeout = config.DefaultReadinessTimeout
	}
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			log.Warn("readiness probe failed", "err", err.Error())
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("redis unavailable"))
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok"))
	}
}

func metricsServer(addr string, m *metrics.Metrics) *http.Server {
	if addr == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// Metrics exposes the collector set, so callers embedding this package
// can register collectors of their own on the same registry.
func (a *App) Metrics() *metrics.Metrics { return a.metrics }

// Bus exposes the message bus so an embedding process can publish without
// a websocket client round trip.
func (a *App) Bus() *bus.Bus { return a.bus }

// Ext carries the pluggable pieces an embedder supplies in code rather
// than through configuration. A nil field is built from cfg exactly as it
// was before Ext existed, so the command-line server passes a zero Ext.
type Ext struct {
	Verifier   auth.Verifier
	Authorizer authz.Authorizer
	Metrics    *metrics.Metrics

	Judge              bus.Judge
	Candidates         bus.CandidateSource
	JudgeTimeout       time.Duration
	JudgeFailurePolicy bus.FailurePolicy
}

func New(cfg *config.Config, log *slog.Logger, ext Ext) (*App, error) {
	if log == nil {
		log = slog.Default()
	}

	ropts := redisx.Options{
		Addrs:      cfg.RedisAddrList(),
		Password:   cfg.RedisPassword,
		MasterName: cfg.RedisMasterName,
	}
	m := ext.Metrics
	if m == nil {
		m = metrics.New()
	}

	rdb := redisx.New(ropts)
	pub := redisx.New(ropts)
	sub := redisx.New(ropts)

	// One hook covers every Redis call the server makes, so redisx, bus,
	// ratelimit and scheduler need no instrumentation of their own.
	hook := metrics.NewRedisHook(m)
	rdb.AddHook(hook)
	pub.AddHook(hook)
	sub.AddHook(hook)

	verifier := ext.Verifier
	switch {
	case verifier != nil:
	case cfg.AuthJWTSecret == "":
		verifier = auth.NewInsecure(log)
	default:
		verifier = auth.NewJWT(auth.JWTConfig{
			Secret:   cfg.AuthJWTSecret,
			Audience: cfg.AuthJWTAudience,
			Issuer:   cfg.AuthJWTIssuer,
		}, log)
	}

	rules, err := authz.ParseRules(cfg.AuthzRules)
	if err != nil {
		rdb.Close()
		pub.Close()
		sub.Close()
		return nil, err
	}
	authorizer := ext.Authorizer
	switch {
	case authorizer != nil:
	case rules == nil:
		log.Warn("AUTHZ_RULES is not set — every authenticated client can subscribe to, publish to and lock every topic. Set it before serving more than one tenant.")
		authorizer = authz.AllowAll()
	default:
		authorizer = rules
	}

	hub := connection.NewHub(log)

	busInst := bus.New(pub, sub, hub, bus.Config{
		StreamMaxLength:    cfg.StreamMaxLength,
		StreamTTL:          cfg.StreamTTL,
		Judge:              ext.Judge,
		Candidates:         ext.Candidates,
		JudgeTimeout:       ext.JudgeTimeout,
		JudgeFailurePolicy: ext.JudgeFailurePolicy,
		Metrics:            m,
	}, log)

	guard := &ssrf.Guard{AllowedHosts: cfg.SchedulerAllowedHosts}
	sched := scheduler.New(rdb, scheduler.Config{
		InstanceID: cfg.InstanceID,
		Guard:      guard,
		Metrics:    m,
	}, log)

	deps := connection.Deps{
		RDB:              rdb,
		Bus:              busInst,
		Hub:              hub,
		Log:              log,
		MessageRateLimit: ratelimit.DefaultMessageConfig(cfg.RateLimitMessagesPerSec),
		JobRateLimit:     ratelimit.DefaultJobConfig(cfg.RateLimitJobsPerMin),
		OnSchedulerWake:  sched.Wake,
		Authorizer:       authorizer,
		Metrics:          m,
	}

	upgrader := websocket.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin:     originChecker(cfg.AllowedOrigins, log),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWS(r.Context(), log, cfg, &upgrader, verifier, hub, deps, r, w)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", readyHandler(rdb, cfg.ReadinessTimeout, log))

	if cfg.TLSEnabled() {
		// Pre-load to fail fast on bad paths.
		if _, err := tls.LoadX509KeyPair(cfg.TLSCertPath, cfg.TLSKeyPath); err != nil {
			rdb.Close()
			pub.Close()
			sub.Close()
			return nil, fmt.Errorf("load TLS keypair: %w", err)
		}
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.WebSocketPort))
	if err != nil {
		rdb.Close()
		pub.Close()
		sub.Close()
		return nil, fmt.Errorf("listen: %w", err)
	}

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	return &App{
		cfg:      cfg,
		log:      log,
		rdb:      rdb,
		pub:      pub,
		sub:      sub,
		hub:      hub,
		bus:      busInst,
		sched:    sched,
		server:   server,
		listener: listener,
		verifier: verifier,
		metrics:  m,
		metricsS: metricsServer(cfg.MetricsAddr, m),
	}, nil
}

// Addr returns the address the server is listening on, including the
// resolved port when WebSocketPort was 0.
func (a *App) Addr() string { return a.listener.Addr().String() }

// Port returns the resolved TCP port.
func (a *App) Port() int {
	return a.listener.Addr().(*net.TCPAddr).Port
}

// Run starts every subsystem and blocks. Returns nil on graceful shutdown
// (ctx cancelled), or the first fatal error from any subsystem.
func (a *App) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	errCh := make(chan error, 3)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := a.bus.Run(ctx); err != nil {
			errCh <- fmt.Errorf("bus: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := scheduler.ResumeOnStartup(ctx, a.rdb, a.log); err != nil {
			a.log.Warn("resume scheduler on startup failed", "err", err.Error())
		}
		if err := a.sched.Run(ctx); err != nil {
			errCh <- fmt.Errorf("scheduler: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		scheme := "ws"
		if a.cfg.TLSEnabled() {
			scheme = "wss"
		}
		a.log.Info("server listening", "scheme", scheme, "addr", a.Addr(), "instanceId", a.cfg.InstanceID)

		var listenErr error
		if a.cfg.TLSEnabled() {
			listenErr = a.server.ServeTLS(a.listener, a.cfg.TLSCertPath, a.cfg.TLSKeyPath)
		} else {
			listenErr = a.server.Serve(a.listener)
		}
		if listenErr != nil && !errors.Is(listenErr, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", listenErr)
		}
	}()

	if a.metricsS != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.log.Info("metrics listening", "addr", a.metricsS.Addr)
			if err := a.metricsS.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				// Losing metrics must not take the server down.
				a.log.Error("metrics server stopped", "err", err.Error())
			}
		}()
	}

	var runErr error
	select {
	case <-ctx.Done():
		a.log.Info("shutdown signal received")
	case err := <-errCh:
		a.log.Error("subsystem failed", "err", err.Error())
		runErr = err
	}

	a.shutdown()
	wg.Wait()
	a.log.Info("shutdown complete")
	return runErr
}

func (a *App) shutdown() {
	timeout := a.cfg.ShutdownTimeout
	if timeout <= 0 {
		timeout = config.DefaultShutdownTimeout
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := a.server.Shutdown(shutdownCtx); err != nil {
		a.log.Warn("http shutdown error", "err", err.Error())
	}
	if a.metricsS != nil {
		if err := a.metricsS.Shutdown(shutdownCtx); err != nil {
			a.log.Warn("metrics shutdown error", "err", err.Error())
		}
	}
	// Close queues a close frame behind whatever is already in each send
	// queue, so the writer drains the backlog before it sends the close.
	// Without the wait that follows, the process exits mid-drain and every
	// queued frame is lost — messages the server had already accepted and
	// acknowledged to their publisher.
	conns := a.hub.Snapshot()
	for _, c := range conns {
		c.Close(websocket.CloseGoingAway, "server shutting down")
	}
	if stuck := a.hub.WaitDrained(shutdownCtx, conns); stuck > 0 {
		a.log.Warn("shutdown timed out with connections still draining",
			"stuck", stuck, "total", len(conns), "timeout", timeout.String())
	}

	if err := a.bus.Close(); err != nil {
		a.log.Warn("bus close error", "err", err.Error())
	}
	a.rdb.Close()
	a.pub.Close()
	a.sub.Close()
}

func serveWS(
	rootCtx context.Context,
	log *slog.Logger,
	cfg *config.Config,
	upgrader *websocket.Upgrader,
	verifier auth.Verifier,
	hub *connection.Hub,
	deps connection.Deps,
	r *http.Request,
	w http.ResponseWriter,
) {
	m := deps.Metrics
	if m == nil {
		m = metrics.New()
	}

	res, err := verifier.Verify(r)
	if err != nil || res == nil {
		m.ConnectionsTotal.WithLabelValues(metrics.UpgradeRejectedAuth).Inc()
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Upgrade can fail on a rejected Origin, a bad handshake, or a client
	// that hung up; gorilla has already written the response.
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		m.ConnectionsTotal.WithLabelValues(metrics.UpgradeRejectedOther).Inc()
		return
	}
	m.ConnectionsTotal.WithLabelValues(metrics.UpgradeAccepted).Inc()

	keepAlive := r.URL.Query().Get("keepAlive") == "true"

	conn := connection.NewConn(ws, connection.ConnConfig{
		UserKey:             res.UserKey,
		MaxBufferedBytes:    cfg.MaxBufferedBytes,
		MaxConsecutiveDrops: cfg.MaxConsecutiveDrops,
		SendChanCapacity:    128,
		WriteWait:           10 * time.Second,
		ExpiresAt:           res.ExpiresAt,
		Claims:              res.Claims,
		Metrics:             deps.Metrics,
	}, log)

	hub.Add(conn)
	if err := redisx.AddConnection(rootCtx, deps.RDB, conn.UserKey()); err != nil {
		log.Warn("AddConnection failed", "err", err.Error())
	}
	log.Info("websocket client connected", "userKey", conn.UserKey())

	connCtx, cancel := context.WithCancel(rootCtx)
	defer cancel()

	conn.Run(
		connCtx,
		cfg.MaxPayloadBytes,
		cfg.WebSocketTimeout,
		keepAlive,
		func(raw []byte) {
			connection.Dispatch(connCtx, conn, raw, deps)
		},
		func() {
			isLast := hub.Remove(conn)
			cleanupCtx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer ccancel()
			connection.Cleanup(cleanupCtx, conn, deps, isLast)
		},
	)
}
