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
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"

	"github.com/damiensmith1/go-ws-server/internal/auth"
	"github.com/damiensmith1/go-ws-server/internal/bus"
	"github.com/damiensmith1/go-ws-server/internal/config"
	"github.com/damiensmith1/go-ws-server/internal/connection"
	"github.com/damiensmith1/go-ws-server/internal/ratelimit"
	"github.com/damiensmith1/go-ws-server/internal/redisx"
	"github.com/damiensmith1/go-ws-server/internal/scheduler"
	"github.com/damiensmith1/go-ws-server/internal/ssrf"
)

// App holds every long-lived resource. New constructs it; Run blocks
// until the supplied context is cancelled or a subsystem fails fatally.
type App struct {
	cfg      *config.Config
	log      *slog.Logger
	rdb      *redis.Client
	pub      *redis.Client
	sub      *redis.Client
	hub      *connection.Hub
	bus      *bus.Bus
	sched    *scheduler.Scheduler
	server   *http.Server
	listener net.Listener
	verifier auth.Verifier
}

func New(cfg *config.Config, log *slog.Logger) (*App, error) {
	if log == nil {
		log = slog.Default()
	}

	addr := cfg.RedisHost + ":" + strconv.Itoa(cfg.RedisPort)
	rdb := redisx.New(redisx.Options{Addr: addr, Password: cfg.RedisPassword})
	pub := redisx.New(redisx.Options{Addr: addr, Password: cfg.RedisPassword})
	sub := redisx.New(redisx.Options{Addr: addr, Password: cfg.RedisPassword})

	var verifier auth.Verifier
	if cfg.AuthJWTSecret == "" {
		verifier = auth.NewInsecure(log)
	} else {
		verifier = auth.NewJWT(auth.JWTConfig{
			Secret:   cfg.AuthJWTSecret,
			Audience: cfg.AuthJWTAudience,
			Issuer:   cfg.AuthJWTIssuer,
		}, log)
	}

	hub := connection.NewHub(log)

	busInst := bus.New(pub, sub, hub, bus.Config{
		StreamMaxLength: cfg.StreamMaxLength,
		StreamTTL:       cfg.StreamTTL,
	}, log)

	guard := &ssrf.Guard{AllowedHosts: cfg.SchedulerAllowedHosts}
	sched := scheduler.New(rdb, scheduler.Config{
		InstanceID: cfg.InstanceID,
		Guard:      guard,
	}, log)

	deps := connection.Deps{
		RDB:              rdb,
		Bus:              busInst,
		Hub:              hub,
		Log:              log,
		MessageRateLimit: ratelimit.DefaultMessageConfig(cfg.RateLimitMessagesPerSec),
		JobRateLimit:     ratelimit.DefaultJobConfig(cfg.RateLimitJobsPerMin),
		OnSchedulerWake:  sched.Wake,
	}

	upgrader := websocket.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWS(r.Context(), log, cfg, &upgrader, verifier, hub, deps, r, w)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

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
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := a.server.Shutdown(shutdownCtx); err != nil {
		a.log.Warn("http shutdown error", "err", err.Error())
	}
	for _, c := range a.hub.Snapshot() {
		c.Close(websocket.CloseGoingAway, "server shutting down")
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
	res, err := verifier.Verify(r)
	if err != nil || res == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	keepAlive := r.URL.Query().Get("keepAlive") == "true"

	conn := connection.NewConn(ws, connection.ConnConfig{
		UserKey:          res.UserKey,
		MaxBufferedBytes: cfg.MaxBufferedBytes,
		SendChanCapacity: 128,
		WriteWait:        10 * time.Second,
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
