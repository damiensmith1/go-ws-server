// Package connection wires a websocket socket up to the rest of the
// system: it manages the userKey → []*Conn hub, runs a per-connection
// writer goroutine, applies idle/keepalive timing, and dispatches inbound
// messages.
package connection

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/damiensmith1/go-ws-server/metrics"
)

// Hub tracks every active connection grouped by userKey. It implements
// bus.BroadcastTarget so the bus can fan out per-user broadcast messages.
type Hub struct {
	mu    sync.RWMutex
	users map[string][]*Conn
	log   *slog.Logger
}

func NewHub(log *slog.Logger) *Hub {
	if log == nil {
		log = slog.Default()
	}
	return &Hub{users: make(map[string][]*Conn), log: log}
}

// Add registers a connection and reports whether it is the first socket
// for its userKey. Remove mirrors it, reporting whether it was the last.
// The pair is what a presence feed needs: a user with three sockets is
// present once, not three times.
func (h *Hub) Add(c *Conn) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	first := len(h.users[c.userKey]) == 0
	h.users[c.userKey] = append(h.users[c.userKey], c)
	return first
}

// Remove drops c from the hub. Returns true if c was the last connection
// for its userKey.
func (h *Hub) Remove(c *Conn) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	conns := h.users[c.userKey]
	out := conns[:0]
	for _, x := range conns {
		if x != c {
			out = append(out, x)
		}
	}
	if len(out) == 0 {
		delete(h.users, c.userKey)
		return true
	}
	h.users[c.userKey] = out
	return false
}

// SendToUser delivers msg to every active connection for userKey.
// Non-blocking — relies on Conn.Send being non-blocking.
func (h *Hub) SendToUser(userKey string, msg []byte) {
	h.mu.RLock()
	conns := h.users[userKey]
	targets := make([]*Conn, len(conns))
	copy(targets, conns)
	h.mu.RUnlock()
	for _, c := range targets {
		c.Send(msg)
	}
}

// SendToConn delivers a frame to one specific socket, used for delivery
// reports addressed to the publishing connection rather than the user.
//
// The hub is indexed by userKey, so this scans that user's sockets — but
// a connID is not a userKey, so it scans all of them. That is acceptable
// only because reports are opt-in and rare; making it cheap would mean a
// second index maintained on every connect and disconnect.
func (h *Hub) SendToConn(connID string, msg []byte) {
	h.mu.RLock()
	var target *Conn
	for _, conns := range h.users {
		for _, c := range conns {
			if c.id == connID {
				target = c
				break
			}
		}
		if target != nil {
			break
		}
	}
	h.mu.RUnlock()
	if target != nil {
		target.Send(msg)
	}
}

// Snapshot returns a slice of every active connection, useful for graceful
// shutdown.
// WaitDrained waits for every connection in the hub to flush its send
// queue, or for ctx to expire. It returns the number that did not drain
// in time, so the caller can report how much was dropped rather than
// claiming a clean shutdown it did not achieve.
func (h *Hub) WaitDrained(ctx context.Context, conns []*Conn) int {
	var wg sync.WaitGroup
	var stuck atomic.Int64
	for _, c := range conns {
		wg.Add(1)
		go func(c *Conn) {
			defer wg.Done()
			if !c.WaitDrained(ctx) {
				stuck.Add(1)
			}
		}(c)
	}
	wg.Wait()
	return int(stuck.Load())
}

func (h *Hub) Snapshot() []*Conn {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]*Conn, 0)
	for _, conns := range h.users {
		out = append(out, conns...)
	}
	return out
}

// frameKind tags an outgoing frame so the writer goroutine knows which
// websocket method to call.
type frameKind int

const (
	frameText frameKind = iota
	framePing
	frameClose
)

type outFrame struct {
	kind      frameKind
	payload   []byte
	closeCode int
}

// Conn is one client websocket. All writes flow through writeLoop via
// sendCh; gorilla forbids concurrent writes, and routing pongs and close
// frames through the same channel keeps that contract regardless of which
// goroutine produced the frame.
type Conn struct {
	ws        *websocket.Conn
	id        string
	userKey   string
	log       *slog.Logger
	sendCh    chan outFrame
	bufBytes  atomic.Int64
	maxBuf    int64
	writeWait time.Duration

	// maxDrops is the consecutive-drop budget before eviction; 0 disables
	// eviction. consecDrops is reset by any successful send.
	maxDrops    int64
	consecDrops atomic.Int64

	// idleReset is signaled whenever the reader sees inbound traffic; the
	// idle-timeout watcher consumes it.
	idleReset chan struct{}

	closeOnce sync.Once
	closed    chan struct{}

	// drained is closed by runWriter when it returns, i.e. once every
	// frame queued ahead of the close frame has been written to the
	// socket. Shutdown waits on it so a graceful stop does not discard
	// messages the server already accepted.
	drained chan struct{}

	m           *metrics.Metrics
	openedAt    time.Time
	expiresAt   time.Time
	claims      map[string]any
	closeReason string // written once, inside closeOnce
	readErr     bool   // set by runReader before it returns; read after
}

// ConnConfig configures a new Conn.
type ConnConfig struct {
	UserKey          string
	MaxBufferedBytes int64
	SendChanCapacity int
	WriteWait        time.Duration

	// ID is this connection's cluster-unique identifier, used by a
	// bus.Judge to address it. Empty generates a UUID.
	ID string

	// MaxConsecutiveDrops evicts a connection after this many fan-out
	// messages are dropped back to back. Zero disables eviction, which is
	// the behaviour this server had before: drops were counted but a
	// wedged client kept its socket and kept losing messages forever.
	//
	// Consecutive rather than cumulative on purpose. Every long-lived
	// connection will drop the occasional frame under a burst, and a
	// cumulative budget would eventually evict all of them. Only a run of
	// drops with no successful send in between means the peer has stopped
	// reading.
	MaxConsecutiveDrops int

	// ExpiresAt is the credential deadline from auth.Result. Zero means the
	// credential does not expire and no deadline watcher is started.
	ExpiresAt time.Time

	// Claims are the credential's claims from auth.Result, carried so an
	// authz.Authorizer can read them per frame.
	Claims map[string]any

	// Metrics is optional. A nil value gets a private collector set, so
	// call sites never need a nil check.
	Metrics *metrics.Metrics
}

// NewConn wraps a websocket.Conn. Run() must be called to start the
// reader/writer goroutines.
func NewConn(ws *websocket.Conn, cfg ConnConfig, log *slog.Logger) *Conn {
	if cfg.SendChanCapacity <= 0 {
		cfg.SendChanCapacity = 64
	}
	if cfg.WriteWait <= 0 {
		cfg.WriteWait = 10 * time.Second
	}
	if cfg.ID == "" {
		cfg.ID = uuid.NewString()
	}
	if cfg.MaxBufferedBytes <= 0 {
		cfg.MaxBufferedBytes = 1024 * 1024
	}
	if log == nil {
		log = slog.Default()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.New()
	}
	cfg.Metrics.ConnectionsActive.Inc()
	return &Conn{
		m:         cfg.Metrics,
		id:        cfg.ID,
		openedAt:  time.Now(),
		maxDrops:  int64(cfg.MaxConsecutiveDrops),
		expiresAt: cfg.ExpiresAt,
		claims:    cfg.Claims,
		ws:        ws,
		userKey:   cfg.UserKey,
		log:       log.With("userKey", cfg.UserKey),
		sendCh:    make(chan outFrame, cfg.SendChanCapacity),
		maxBuf:    cfg.MaxBufferedBytes,
		writeWait: cfg.WriteWait,
		idleReset: make(chan struct{}, 1),
		closed:    make(chan struct{}),
		drained:   make(chan struct{}),
	}
}

// UserKey returns the authenticated identity for this connection.
func (c *Conn) UserKey() string { return c.userKey }

// WaitDrained blocks until the writer goroutine has flushed everything
// queued on this connection, or until ctx is done. It returns true if the
// connection drained.
//
// A connection whose writer never started — every Conn in the unit tests,
// and any Conn built with a nil socket — would block here forever, so a
// caller must always bound this with a context.
func (c *Conn) WaitDrained(ctx context.Context) bool {
	select {
	case <-c.drained:
		return true
	case <-ctx.Done():
		return false
	}
}

// SubscriberID returns this connection's cluster-unique identifier,
// satisfying bus.Identified so a bus.Judge can address it individually.
//
// It is per-connection, not per-userKey: one user may hold several
// sockets with different interests, and routing has to be able to tell
// them apart.
func (c *Conn) SubscriberID() string { return c.id }

// Claims returns the credential claims this connection was authorized
// with, or nil. The map is shared, not copied: callers must treat it as
// read-only.
func (c *Conn) Claims() map[string]any { return c.claims }

// Send enqueues a text frame. Non-blocking: on full queue or
// over-threshold buffered bytes the message is dropped with a warn log.
// This is the policy that keeps one slow client from stalling the bus.
func (c *Conn) Send(msg []byte) { _ = c.SendReporting(msg) }

// SendReporting is Send, reporting whether the frame was queued. It is
// what makes a delivery report able to distinguish delivered from
// dropped; Send discards the answer.
func (c *Conn) SendReporting(msg []byte) bool {
	select {
	case <-c.closed:
		return false
	default:
	}
	n := int64(len(msg))
	if c.bufBytes.Load()+n > c.maxBuf {
		c.log.Warn("dropping message: socket buffer above threshold",
			"bufferedAmount", c.bufBytes.Load(),
			"messageBytes", n,
		)
		c.recordDrop(metrics.DropBufferThreshold)
		return false
	}
	select {
	case c.sendCh <- outFrame{kind: frameText, payload: msg}:
		c.bufBytes.Add(n)
		c.m.FramesSent.Inc()
		c.consecDrops.Store(0)
		return true
	case <-c.closed:
		return false
	default:
		c.log.Warn("dropping message: send chan full")
		c.recordDrop(metrics.DropChannelFull)
		return false
	}
}

// recordDrop counts a dropped fan-out message and evicts the connection
// once it has dropped MaxConsecutiveDrops in a row.
//
// Dropping rather than blocking is what keeps one stalled client from
// backing up the whole bus, but on its own it has no end state: a peer
// that has stopped reading holds its socket, its Redis subscription entry
// and its share of every fan-out for as long as it stays connected, while
// silently receiving nothing. Eviction gives that a bound and makes the
// failure visible to the client, which can reconnect and replay.
func (c *Conn) recordDrop(reason string) {
	c.m.FramesDropped.WithLabelValues(reason).Inc()
	if c.maxDrops <= 0 {
		return
	}
	if c.consecDrops.Add(1) < c.maxDrops {
		return
	}
	c.log.Warn("evicting slow consumer",
		"userKey", c.userKey, "consecutiveDrops", c.maxDrops, "reason", reason)
	// 1013 Try Again Later: the client is not at fault in a way it can
	// fix by changing its request, and it should reconnect — with `since`
	// to replay what it missed.
	c.closeWith(websocket.CloseTryAgainLater, "slow consumer", metrics.CloseSlowConsumer)
}

// SendNow is a blocking variant used for reply frames (success/error
// responses to a reqID). It still respects context cancellation. Drops if
// the connection has already begun shutting down.
func (c *Conn) SendNow(ctx context.Context, msg []byte) {
	n := int64(len(msg))
	select {
	case c.sendCh <- outFrame{kind: frameText, payload: msg}:
		c.bufBytes.Add(n)
		c.m.FramesSent.Inc()
	case <-ctx.Done():
	case <-c.closed:
	}
}

// Ping enqueues a ping frame. Used by the keepalive loop.
func (c *Conn) Ping() {
	select {
	case <-c.closed:
		return
	default:
	}
	select {
	case c.sendCh <- outFrame{kind: framePing}:
	case <-c.closed:
	default:
		// If the queue is full a ping won't fix anything; drop quietly.
	}
}

// Close requests a graceful shutdown. Idempotent. The reason recorded
// for metrics is metrics.CloseServer; internal callers that know better
// use closeWith.
func (c *Conn) Close(code int, reason string) {
	c.closeWith(code, reason, metrics.CloseServer)
}

// closeWith is Close plus the category to attribute the close to. Only
// the first caller's category is recorded, matching closeOnce semantics.
func (c *Conn) closeWith(code int, reason, category string) {
	c.closeOnce.Do(func() {
		c.closeReason = category
		select {
		case c.sendCh <- outFrame{kind: frameClose, closeCode: code, payload: []byte(reason)}:
		default:
		}
		close(c.closed)
	})
}

// IsClosed reports whether Close has been called.
func (c *Conn) IsClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

// resetIdle nudges the idle watcher.
func (c *Conn) resetIdle() {
	select {
	case c.idleReset <- struct{}{}:
	default:
	}
}

// runWriter is the only goroutine that calls into the gorilla socket for
// writes. Mixing writes from multiple goroutines is a panic in gorilla.
func (c *Conn) runWriter() {
	defer c.ws.Close()
	defer close(c.drained)
	for frame := range c.sendCh {
		_ = c.ws.SetWriteDeadline(time.Now().Add(c.writeWait))
		switch frame.kind {
		case frameText:
			err := c.ws.WriteMessage(websocket.TextMessage, frame.payload)
			c.bufBytes.Add(-int64(len(frame.payload)))
			if err != nil {
				c.log.Debug("writer: write error", "err", err.Error())
				return
			}
		case framePing:
			if err := c.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.log.Debug("writer: ping error", "err", err.Error())
				return
			}
		case frameClose:
			code := frame.closeCode
			if code == 0 {
				code = websocket.CloseNormalClosure
			}
			msg := websocket.FormatCloseMessage(code, string(frame.payload))
			_ = c.ws.WriteMessage(websocket.CloseMessage, msg)
			return
		}
	}
}

// runReader is the only goroutine that calls ReadMessage. It dispatches
// each inbound message via the supplied handler and resets the idle timer
// on every successful read (and on pong frames).
type readerDeps struct {
	maxPayload int64
	dispatch   func(raw []byte)
}

func (c *Conn) runReader(deps readerDeps) {
	c.ws.SetReadLimit(deps.maxPayload)
	c.ws.SetPongHandler(func(string) error {
		c.resetIdle()
		return nil
	})
	for {
		_, msg, err := c.ws.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err,
				websocket.CloseNormalClosure,
				websocket.CloseGoingAway,
				websocket.CloseAbnormalClosure,
			) && !errors.Is(err, websocket.ErrCloseSent) {
				c.log.Debug("reader: read error", "err", err.Error())
				c.readErr = true
			}
			return
		}
		c.resetIdle()
		deps.dispatch(msg)
	}
}

// runIdleWatcher closes the connection if no inbound activity has been
// seen for idle. It also drives keepalive pings when keepAlive is true.
// runExpiryWatcher closes the socket when the credential that authorized
// it expires.
//
// Verification runs once, at upgrade. Without this, a token that is
// revoked or simply expires keeps its socket for as long as the client
// keeps it warm — up to WEBSOCKET_TIMEOUT past the moment it stopped
// being valid, and indefinitely if the client pings. The close code is
// 1008 (policy violation) so a client can tell "your credential ran out,
// re-authenticate and reconnect" apart from an idle close or a restart.
func (c *Conn) runExpiryWatcher(ctx context.Context) {
	if c.expiresAt.IsZero() {
		return
	}
	d := time.Until(c.expiresAt)
	if d <= 0 {
		c.log.Info("credential already expired at upgrade")
		c.closeWith(websocket.ClosePolicyViolation, "token expired", metrics.CloseTokenExpired)
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-c.closed:
	case <-timer.C:
		c.log.Info("credential expired", "expiredAt", c.expiresAt.UTC().Format(time.RFC3339))
		c.closeWith(websocket.ClosePolicyViolation, "token expired", metrics.CloseTokenExpired)
	}
}

func (c *Conn) runIdleWatcher(ctx context.Context, idle time.Duration, keepAlive bool) {
	if idle <= 0 {
		<-ctx.Done()
		return
	}
	timer := time.NewTimer(idle)
	defer timer.Stop()

	var pingTicker *time.Ticker
	if keepAlive {
		pingTicker = time.NewTicker(idle / 2)
		defer pingTicker.Stop()
	}

	pingCh := func() <-chan time.Time {
		if pingTicker == nil {
			return nil
		}
		return pingTicker.C
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closed:
			return
		case <-c.idleReset:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(idle)
		case <-pingCh():
			c.Ping()
		case <-timer.C:
			c.log.Info("socket idle timeout")
			c.closeWith(websocket.CloseNormalClosure, "idle timeout", metrics.CloseIdleTimeout)
			return
		}
	}
}

// Run spins up the reader, writer, and idle watcher goroutines. It blocks
// until the connection is fully torn down.
func (c *Conn) Run(
	ctx context.Context,
	maxPayload int64,
	idleTimeout time.Duration,
	keepAlive bool,
	dispatch func(raw []byte),
	onDone func(),
) {
	go c.runWriter()
	go c.runIdleWatcher(ctx, idleTimeout, keepAlive)
	go c.runExpiryWatcher(ctx)

	c.runReader(readerDeps{
		maxPayload: maxPayload,
		dispatch:   dispatch,
	})

	reason := metrics.ClosePeer
	if c.readErr {
		reason = metrics.CloseReadError
	}
	c.closeWith(websocket.CloseNormalClosure, "", reason)

	c.m.ConnectionsActive.Dec()
	c.m.ConnectionsClosed.WithLabelValues(c.closeReason).Inc()
	c.m.ConnectionDuration.Observe(time.Since(c.openedAt).Seconds())
	// We deliberately do NOT close c.sendCh: external goroutines (the bus
	// pubsub callback, the hub) may still hold references and try to Send
	// after we exit. Send/Ping gate on c.closed and become no-ops; the
	// channel is GC'd once all references drop.
	if onDone != nil {
		onDone()
	}
}
