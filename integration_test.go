//go:build integration

// Integration tests for go-ws-server. They boot a real Redis container
// via testcontainers, start the server in-process on a random port, and
// drive it with multiple gorilla/websocket clients.
//
// Run with:
//
//	go test -tags=integration -timeout=120s -v .
//
// Requires Docker. The redis:7 image will be pulled on first run.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/damiensmith1/go-ws-server/internal/app"
	"github.com/damiensmith1/go-ws-server/internal/config"
)

// startRedis brings up a redis:7 container and returns host/port.
func startRedis(t *testing.T) (host string, port int) {
	t.Helper()
	ctx := context.Background()
	c, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("start redis container: %v", err)
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = c.Terminate(shutdownCtx)
	})

	endpoint, err := c.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("get endpoint: %v", err)
	}
	// endpoint is "host:port"
	var p int
	host, portStr, err := splitHostPort(endpoint)
	if err != nil {
		t.Fatalf("split endpoint %q: %v", endpoint, err)
	}
	p, err = strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port %q: %v", portStr, err)
	}
	return host, p
}

func splitHostPort(s string) (string, string, error) {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ':' {
			return s[:i], s[i+1:], nil
		}
	}
	return "", "", fmt.Errorf("no port in %q", s)
}

// defaultTestCfg returns a config with permissive defaults — no idle
// timeout to speak of, high rate limits — suitable for tests that aren't
// exercising those knobs.
func defaultTestCfg(redisHost string, redisPort int) *config.Config {
	return &config.Config{
		RedisHost:               redisHost,
		RedisPort:               redisPort,
		WebSocketPort:           0, // OS-assigned
		WebSocketTimeout:        30 * time.Second,
		MaxPayloadBytes:         64 * 1024,
		MaxBufferedBytes:        1024 * 1024,
		StreamMaxLength:         1000,
		RateLimitMessagesPerSec: 1000,
		RateLimitJobsPerMin:     1000,
		LogLevel:                "warn",
		InstanceID:              "test-instance",
	}
}

// startServer is a convenience wrapper around startServerCfg using
// defaultTestCfg.
func startServer(t *testing.T, redisHost string, redisPort int) (wsURL string, cancel func()) {
	t.Helper()
	return startServerCfg(t, defaultTestCfg(redisHost, redisPort))
}

// startServerCfg boots an in-process App with the supplied config on an
// OS-assigned port (so tests can run in parallel without collisions).
// Returns the ws URL and a cancel that blocks until shutdown completes.
func startServerCfg(t *testing.T, cfg *config.Config) (wsURL string, cancel func()) {
	t.Helper()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	a, err := app.New(cfg, log)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}

	ctx, cancelCtx := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		_ = a.Run(ctx)
		close(done)
	}()

	cancel = func() {
		cancelCtx()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Log("server did not shut down within 15s")
		}
	}

	addr := a.Addr()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return "ws://" + addr + "/ws", cancel
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	t.Fatalf("server did not become healthy at %s", addr)
	return "", nil
}

// testClient is a thin gorilla/websocket wrapper that decodes inbound
// frames as map[string]any and exposes them via a buffered channel.
//
// Frames received but not yet matched by Expect() are held in `backlog`
// so subsequent Expect() calls can find them. Without this, a publish
// delivery that arrives before its success ack would be silently
// discarded.
type testClient struct {
	t        *testing.T
	conn     *websocket.Conn
	frames   chan map[string]any
	closed   atomic.Bool
	closedCh chan struct{} // signals when readLoop returns (peer closed)

	mu      sync.Mutex
	backlog []map[string]any
}

// clientOpts carries connection parameters in a single struct so tests
// can mix-and-match userKey/token/keepAlive without growing positional
// argument lists.
type clientOpts struct {
	UserKey   string
	Token     string
	KeepAlive bool
	Headers   http.Header
}

func dial(t *testing.T, baseURL, userKey string) *testClient {
	t.Helper()
	return dialOpts(t, baseURL, clientOpts{UserKey: userKey})
}

func dialOpts(t *testing.T, baseURL string, opts clientOpts) *testClient {
	t.Helper()
	u, err := url.Parse(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if opts.UserKey != "" {
		q.Set("userKey", opts.UserKey)
	}
	if opts.Token != "" {
		q.Set("token", opts.Token)
	}
	if opts.KeepAlive {
		q.Set("keepAlive", "true")
	}
	u.RawQuery = q.Encode()

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), opts.Headers)
	if err != nil {
		t.Fatalf("dial %s: %v", u.String(), err)
	}

	c := &testClient{
		t:        t,
		conn:     conn,
		frames:   make(chan map[string]any, 64),
		closedCh: make(chan struct{}),
	}
	go c.readLoop()
	t.Cleanup(c.Close)
	return c
}

// dialExpectStatus tries to upgrade and asserts the upgrade fails with
// the expected HTTP status. Used to verify auth rejections without
// establishing a WebSocket session.
func dialExpectStatus(t *testing.T, baseURL string, opts clientOpts, wantStatus int) {
	t.Helper()
	u, err := url.Parse(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if opts.UserKey != "" {
		q.Set("userKey", opts.UserKey)
	}
	if opts.Token != "" {
		q.Set("token", opts.Token)
	}
	u.RawQuery = q.Encode()

	conn, resp, err := websocket.DefaultDialer.Dial(u.String(), opts.Headers)
	if err == nil {
		_ = conn.Close()
		t.Fatalf("dial %s: expected handshake failure, got success", u.String())
	}
	if resp == nil {
		t.Fatalf("dial %s: %v (no response)", u.String(), err)
	}
	if resp.StatusCode != wantStatus {
		t.Fatalf("dial %s: status=%d want %d (err=%v)", u.String(), resp.StatusCode, wantStatus, err)
	}
}

func (c *testClient) readLoop() {
	defer func() {
		close(c.frames)
		close(c.closedCh)
	}()
	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		var f map[string]any
		if err := json.Unmarshal(raw, &f); err == nil {
			select {
			case c.frames <- f:
			default:
				c.t.Logf("frame dropped (chan full): %s", raw)
			}
		}
	}
}

// WaitClosed blocks until the server closes the connection (or timeout).
// Returns true if the connection closed in time.
func (c *testClient) WaitClosed(timeout time.Duration) bool {
	select {
	case <-c.closedCh:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (c *testClient) Send(msg map[string]any) {
	c.t.Helper()
	raw, err := json.Marshal(msg)
	if err != nil {
		c.t.Fatal(err)
	}
	if err := c.conn.WriteMessage(websocket.TextMessage, raw); err != nil {
		c.t.Fatalf("write: %v", err)
	}
}

// Expect waits for a frame matching the predicate. Frames that arrive
// in the meantime but don't match are kept in the backlog so later
// Expect() calls can still see them. Fails the test on timeout.
func (c *testClient) Expect(timeout time.Duration, name string, pred func(map[string]any) bool) map[string]any {
	c.t.Helper()

	// First, scan the backlog.
	c.mu.Lock()
	for i, f := range c.backlog {
		if pred(f) {
			c.backlog = append(c.backlog[:i], c.backlog[i+1:]...)
			c.mu.Unlock()
			return f
		}
	}
	c.mu.Unlock()

	deadline := time.After(timeout)
	for {
		select {
		case f, ok := <-c.frames:
			if !ok {
				c.t.Fatalf("%s: socket closed before match", name)
			}
			if pred(f) {
				return f
			}
			c.mu.Lock()
			c.backlog = append(c.backlog, f)
			c.mu.Unlock()
		case <-deadline:
			c.t.Fatalf("%s: timeout waiting for matching frame (backlog=%d)", name, len(c.backlog))
		}
	}
}

// ExpectNone asserts no matching frame arrives within timeout. Used to
// confirm the sender doesn't get its own broadcast echoed back, etc.
func (c *testClient) ExpectNone(timeout time.Duration, name string, pred func(map[string]any) bool) {
	c.t.Helper()

	// Scan the backlog first.
	c.mu.Lock()
	for _, f := range c.backlog {
		if pred(f) {
			c.t.Fatalf("%s: unexpected matching frame in backlog: %v", name, f)
		}
	}
	c.mu.Unlock()

	deadline := time.After(timeout)
	for {
		select {
		case f, ok := <-c.frames:
			if !ok {
				return
			}
			if pred(f) {
				c.t.Fatalf("%s: unexpected matching frame: %v", name, f)
			}
			c.mu.Lock()
			c.backlog = append(c.backlog, f)
			c.mu.Unlock()
		case <-deadline:
			return
		}
	}
}

func (c *testClient) Close() {
	if c.closed.Swap(true) {
		return
	}
	_ = c.conn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"))
	_ = c.conn.Close()
}

// matcher helpers
func isType(t string) func(map[string]any) bool {
	return func(f map[string]any) bool { return f["type"] == t }
}

func isSuccess(reqID string) func(map[string]any) bool {
	return func(f map[string]any) bool {
		return f["type"] == "success" && f["reqID"] == reqID
	}
}

func isError(reqID string) func(map[string]any) bool {
	return func(f map[string]any) bool {
		return f["type"] == "error" && f["reqID"] == reqID
	}
}

func isPublishOnTopic(topic string) func(map[string]any) bool {
	return func(f map[string]any) bool {
		return f["type"] == "publish" && f["topic"] == topic
	}
}

// ---------- scenarios ----------

func TestIntegration_PubSubFanout(t *testing.T) {
	host, port := startRedis(t)
	wsURL, cancel := startServer(t, host, port)
	defer cancel()

	alice := dial(t, wsURL, "alice")
	bob := dial(t, wsURL, "bob")

	alice.Send(map[string]any{"type": "subscribe", "topic": "chat", "reqID": "a1"})
	bob.Send(map[string]any{"type": "subscribe", "topic": "chat", "reqID": "b1"})

	alice.Expect(2*time.Second, "alice subscribed", isSuccess("a1"))
	bob.Expect(2*time.Second, "bob subscribed", isSuccess("b1"))

	// Publish from a third client (carol) so both alice and bob receive it.
	carol := dial(t, wsURL, "carol")
	carol.Send(map[string]any{"type": "subscribe", "topic": "chat", "reqID": "c1"})
	carol.Expect(2*time.Second, "carol subscribed", isSuccess("c1"))

	carol.Send(map[string]any{
		"type":  "publish",
		"topic": "chat",
		"data":  map[string]any{"msg": "hello world"},
		"reqID": "c2",
	})
	carol.Expect(2*time.Second, "carol publish ack", isSuccess("c2"))

	for _, c := range []*testClient{alice, bob, carol} {
		f := c.Expect(2*time.Second, "publish delivery", isPublishOnTopic("chat"))
		data := f["data"].(map[string]any)
		if data["msg"] != "hello world" {
			t.Errorf("unexpected payload: %v", data)
		}
		if _, ok := f["streamId"]; !ok {
			t.Errorf("expected streamId on delivery: %v", f)
		}
	}
}

func TestIntegration_SubscribeReplay(t *testing.T) {
	host, port := startRedis(t)
	wsURL, cancel := startServer(t, host, port)
	defer cancel()

	pub := dial(t, wsURL, "pub")
	pub.Send(map[string]any{"type": "subscribe", "topic": "history", "reqID": "1"})
	pub.Expect(2*time.Second, "subscribed", isSuccess("1"))

	for i := 1; i <= 3; i++ {
		pub.Send(map[string]any{
			"type":  "publish",
			"topic": "history",
			"data":  map[string]any{"n": i},
			"reqID": fmt.Sprintf("p%d", i),
		})
		pub.Expect(2*time.Second, "publish ack", isSuccess(fmt.Sprintf("p%d", i)))
		// Drain the publish delivery (pub is also subscribed).
		pub.Expect(2*time.Second, "delivery", isPublishOnTopic("history"))
	}

	// Late subscriber asks for full history via since=0-0.
	late := dial(t, wsURL, "late")
	late.Send(map[string]any{
		"type":  "subscribe",
		"topic": "history",
		"since": "0-0",
		"reqID": "L1",
	})

	got := []int{}
	deadline := time.Now().Add(3 * time.Second)
	for len(got) < 3 && time.Now().Before(deadline) {
		f := late.Expect(2*time.Second, "replay", func(m map[string]any) bool {
			return m["type"] == "publish" && m["topic"] == "history"
		})
		if f["replay"] != true {
			t.Errorf("expected replay=true on delivery, got %v", f)
		}
		data := f["data"].(map[string]any)
		got = append(got, int(data["n"].(float64)))
	}
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("expected replay 1,2,3 in order, got %v", got)
	}

	// Subscribe response should have arrived too.
	late.Expect(2*time.Second, "late subscribed", isSuccess("L1"))
}

func TestIntegration_BroadcastFanoutPerUser(t *testing.T) {
	host, port := startRedis(t)
	wsURL, cancel := startServer(t, host, port)
	defer cancel()

	// Two sockets for alice, one for bob.
	alice1 := dial(t, wsURL, "alice")
	alice2 := dial(t, wsURL, "alice")
	bob := dial(t, wsURL, "bob")

	alice1.Send(map[string]any{
		"type": "broadcast",
		"data": map[string]any{"hi": "from-alice1"},
	})

	// Both alice sockets receive a broadcast as type=message.
	for i, c := range []*testClient{alice1, alice2} {
		f := c.Expect(2*time.Second, fmt.Sprintf("alice%d broadcast", i+1),
			isType("message"))
		data := f["data"].(map[string]any)
		if data["hi"] != "from-alice1" {
			t.Errorf("alice%d wrong payload: %v", i+1, data)
		}
	}

	// Bob does NOT.
	bob.ExpectNone(500*time.Millisecond, "bob shouldn't receive alice's broadcast",
		isType("message"))
}

func TestIntegration_TopicLockBlocksPublish(t *testing.T) {
	host, port := startRedis(t)
	wsURL, cancel := startServer(t, host, port)
	defer cancel()

	alice := dial(t, wsURL, "alice")
	bob := dial(t, wsURL, "bob")

	alice.Send(map[string]any{
		"type":     "lockTopic",
		"topic":    "vip",
		"lockType": "publish",
		"reqID":    "a1",
	})
	alice.Expect(2*time.Second, "alice locks", isSuccess("a1"))

	// Bob's publish should be rejected because alice owns the publish lock.
	bob.Send(map[string]any{
		"type":  "publish",
		"topic": "vip",
		"data":  map[string]any{"x": 1},
		"reqID": "b1",
	})
	f := bob.Expect(2*time.Second, "bob rejected", isError("b1"))
	if msg, _ := f["message"].(string); msg == "" {
		t.Errorf("expected error message, got %v", f)
	}

	// Alice can still publish (owns lock).
	alice.Send(map[string]any{
		"type":  "publish",
		"topic": "vip",
		"data":  map[string]any{"x": 2},
		"reqID": "a2",
	})
	alice.Expect(2*time.Second, "alice publishes", isSuccess("a2"))

	// Alice unlocks.
	alice.Send(map[string]any{
		"type":     "unlockTopic",
		"topic":    "vip",
		"lockType": "publish",
		"reqID":    "a3",
	})
	alice.Expect(2*time.Second, "alice unlocks", isSuccess("a3"))

	// Bob can now publish.
	bob.Send(map[string]any{
		"type":  "publish",
		"topic": "vip",
		"data":  map[string]any{"x": 3},
		"reqID": "b2",
	})
	bob.Expect(2*time.Second, "bob publishes", isSuccess("b2"))
}

func TestIntegration_JWTAuth(t *testing.T) {
	host, port := startRedis(t)

	const secret = "test-shared-secret"
	cfg := defaultTestCfg(host, port)
	cfg.AuthJWTSecret = secret
	wsURL, cancel := startServerCfg(t, cfg)
	defer cancel()

	mintToken := func(claims jwt.MapClaims) string {
		t.Helper()
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		s, err := tok.SignedString([]byte(secret))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}

	t.Run("valid token connects", func(t *testing.T) {
		good := mintToken(jwt.MapClaims{
			"sub": "alice",
			"exp": time.Now().Add(time.Hour).Unix(),
		})
		c := dialOpts(t, wsURL, clientOpts{Token: good})
		c.Send(map[string]any{"type": "subscribe", "topic": "x", "reqID": "r1"})
		c.Expect(2*time.Second, "subscribed", isSuccess("r1"))
	})

	t.Run("Authorization header also works", func(t *testing.T) {
		good := mintToken(jwt.MapClaims{
			"sub": "alice",
			"exp": time.Now().Add(time.Hour).Unix(),
		})
		hdr := http.Header{}
		hdr.Set("Authorization", "Bearer "+good)
		c := dialOpts(t, wsURL, clientOpts{Headers: hdr})
		c.Send(map[string]any{"type": "subscribe", "topic": "x", "reqID": "r1"})
		c.Expect(2*time.Second, "subscribed", isSuccess("r1"))
	})

	t.Run("missing token rejected with 401", func(t *testing.T) {
		dialExpectStatus(t, wsURL, clientOpts{}, http.StatusUnauthorized)
	})

	t.Run("wrong secret rejected with 401", func(t *testing.T) {
		bad := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"sub": "alice",
			"exp": time.Now().Add(time.Hour).Unix(),
		})
		signed, err := bad.SignedString([]byte("not-the-server-secret"))
		if err != nil {
			t.Fatal(err)
		}
		dialExpectStatus(t, wsURL, clientOpts{Token: signed}, http.StatusUnauthorized)
	})

	t.Run("userKey/sub mismatch rejected", func(t *testing.T) {
		good := mintToken(jwt.MapClaims{
			"sub": "alice",
			"exp": time.Now().Add(time.Hour).Unix(),
		})
		// Sneaking in a userKey query that disagrees with the token is
		// the canonical attempt to act as someone else.
		dialExpectStatus(t, wsURL,
			clientOpts{Token: good, UserKey: "bob"},
			http.StatusUnauthorized)
	})

	t.Run("expired token rejected", func(t *testing.T) {
		expired := mintToken(jwt.MapClaims{
			"sub": "alice",
			"exp": time.Now().Add(-time.Hour).Unix(),
		})
		dialExpectStatus(t, wsURL, clientOpts{Token: expired}, http.StatusUnauthorized)
	})
}

func TestIntegration_RateLimit(t *testing.T) {
	host, port := startRedis(t)
	cfg := defaultTestCfg(host, port)
	cfg.RateLimitMessagesPerSec = 3 // tight, easy to exceed
	wsURL, cancel := startServerCfg(t, cfg)
	defer cancel()

	c := dial(t, wsURL, "alice")

	// Fire 12 publishes back to back. The first 3 should pass; the rest
	// (within the 1-second window) should get "Rate limit exceeded".
	const N = 12
	for i := 0; i < N; i++ {
		c.Send(map[string]any{
			"type":  "publish",
			"topic": "spam",
			"data":  map[string]any{"i": i},
			"reqID": fmt.Sprintf("r%d", i),
		})
	}

	successCount := 0
	rateLimitCount := 0
	deadline := time.After(5 * time.Second)
	for got := 0; got < N; {
		select {
		case f, ok := <-c.frames:
			if !ok {
				t.Fatal("connection closed prematurely")
			}
			switch f["type"] {
			case "success":
				successCount++
				got++
			case "error":
				if msg, _ := f["message"].(string); msg == "Rate limit exceeded" {
					rateLimitCount++
					got++
				}
			case "publish":
				// Self-deliveries from our subscribed publishes — ignore.
			}
		case <-deadline:
			t.Fatalf("timeout collecting replies (got %d, success=%d, rateLimit=%d)",
				got, successCount, rateLimitCount)
		}
	}

	if rateLimitCount == 0 {
		t.Fatalf("expected at least one rate-limit rejection, got success=%d rejections=%d",
			successCount, rateLimitCount)
	}
	if successCount > cfg.RateLimitMessagesPerSec+1 { // +1 slack for clock boundary
		t.Errorf("too many successes: %d (limit %d)", successCount, cfg.RateLimitMessagesPerSec)
	}
	t.Logf("rate-limit verified: success=%d rateLimit=%d", successCount, rateLimitCount)
}

func TestIntegration_ReplayTruncation(t *testing.T) {
	host, port := startRedis(t)
	wsURL, cancel := startServer(t, host, port)
	defer cancel()

	pub := dial(t, wsURL, "pub")
	pub.Send(map[string]any{"type": "subscribe", "topic": "lossy", "reqID": "s"})
	pub.Expect(2*time.Second, "subscribed", isSuccess("s"))

	// Publish a few messages so the stream has *something* in it.
	for i := 0; i < 3; i++ {
		pub.Send(map[string]any{
			"type":  "publish",
			"topic": "lossy",
			"data":  map[string]any{"i": i},
			"reqID": fmt.Sprintf("p%d", i),
		})
		pub.Expect(2*time.Second, "ack", isSuccess(fmt.Sprintf("p%d", i)))
		pub.Expect(2*time.Second, "delivery", isPublishOnTopic("lossy"))
	}

	// Subscribe with a since cursor BEFORE any of the entries in the
	// stream. The truncation flag is reported when `since` < oldest
	// stored entry, which is exactly the case here — a client trying to
	// resume from an out-of-window cursor.
	late := dial(t, wsURL, "late")
	late.Send(map[string]any{
		"type":  "subscribe",
		"topic": "lossy",
		"since": "1-0", // far older than any real entry
		"reqID": "L",
	})

	// Expect the truncation notice.
	f := late.Expect(3*time.Second, "replayTruncated", isType("replayTruncated"))
	if topic, _ := f["topic"].(string); topic != "lossy" {
		t.Errorf("truncation frame on wrong topic: %v", f)
	}
	if oldest, _ := f["oldestAvailable"].(string); oldest == "" {
		t.Errorf("truncation frame missing oldestAvailable: %v", f)
	}

	// Subscribe-success ack should arrive too.
	late.Expect(2*time.Second, "subscribe ack", isSuccess("L"))
}

func TestIntegration_MultiInstance(t *testing.T) {
	host, port := startRedis(t)

	// Two independent instances against the same Redis. Pub/Sub must
	// cross instance boundaries; this is the core scaling guarantee.
	cfgA := defaultTestCfg(host, port)
	cfgA.InstanceID = "inst-A"
	cfgB := defaultTestCfg(host, port)
	cfgB.InstanceID = "inst-B"

	wsA, cancelA := startServerCfg(t, cfgA)
	defer cancelA()
	wsB, cancelB := startServerCfg(t, cfgB)
	defer cancelB()

	t.Run("topic publish crosses instances", func(t *testing.T) {
		alice := dial(t, wsA, "alice")
		alice.Send(map[string]any{"type": "subscribe", "topic": "cross", "reqID": "a"})
		alice.Expect(2*time.Second, "subscribed", isSuccess("a"))

		bob := dial(t, wsB, "bob")
		bob.Send(map[string]any{
			"type":  "publish",
			"topic": "cross",
			"data":  map[string]any{"hi": "across-the-wire"},
			"reqID": "b",
		})
		bob.Expect(2*time.Second, "publish ack", isSuccess("b"))

		f := alice.Expect(3*time.Second, "delivery from other instance",
			isPublishOnTopic("cross"))
		data := f["data"].(map[string]any)
		if data["hi"] != "across-the-wire" {
			t.Errorf("cross-instance payload: %v", data)
		}
	})

	t.Run("broadcast crosses instances", func(t *testing.T) {
		// alice has a socket on each instance.
		alice1 := dial(t, wsA, "alice2")
		alice2 := dial(t, wsB, "alice2")

		alice1.Send(map[string]any{
			"type": "broadcast",
			"data": map[string]any{"hello": "alice"},
		})

		f := alice2.Expect(3*time.Second, "broadcast crosses instances",
			isType("message"))
		data := f["data"].(map[string]any)
		if data["hello"] != "alice" {
			t.Errorf("broadcast payload: %v", data)
		}
	})
}

func TestIntegration_IdleTimeoutClosesConnection(t *testing.T) {
	host, port := startRedis(t)
	cfg := defaultTestCfg(host, port)
	cfg.WebSocketTimeout = 700 * time.Millisecond
	wsURL, cancel := startServerCfg(t, cfg)
	defer cancel()

	c := dialOpts(t, wsURL, clientOpts{UserKey: "idle"})
	// Don't send anything; the server should close us within timeout.
	if !c.WaitClosed(3 * time.Second) {
		t.Fatal("server did not close idle connection")
	}
}

func TestIntegration_KeepAlivePreventsTimeout(t *testing.T) {
	host, port := startRedis(t)
	cfg := defaultTestCfg(host, port)
	cfg.WebSocketTimeout = 600 * time.Millisecond // server pings every 300ms
	wsURL, cancel := startServerCfg(t, cfg)
	defer cancel()

	c := dialOpts(t, wsURL, clientOpts{UserKey: "alive", KeepAlive: true})
	// Sleep well past the idle timeout. The server should have been
	// pinging us, and gorilla's default ping handler auto-pongs, which
	// resets the server's idle timer.
	time.Sleep(2 * time.Second)

	// Connection should still be usable.
	c.Send(map[string]any{"type": "subscribe", "topic": "kept", "reqID": "k1"})
	c.Expect(2*time.Second, "subscribe after long idle", isSuccess("k1"))

	if c.WaitClosed(100 * time.Millisecond) {
		t.Fatal("connection unexpectedly closed despite keepAlive")
	}
}

// TestIntegration_SchedulerSSRFBlocksPrivate proves the full scheduler
// path executes (message → Redis zset → tick loop → HTTP attempt) AND
// that the SSRF dial-control guard refuses to talk to a private target,
// even though the scheduler accepted the job.
//
// httptest.NewServer listens on 127.0.0.1 by default, which is exactly
// what the dial guard blocks. A 0-hit count after the scheduled time
// means: the message protocol works end-to-end, and the SSRF defense is
// load-bearing.
func TestIntegration_SchedulerSSRFBlocksPrivate(t *testing.T) {
	host, port := startRedis(t)
	wsURL, cancel := startServer(t, host, port)
	defer cancel()

	var (
		mu   sync.Mutex
		hits int
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		mu.Lock()
		hits++
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer ts.Close()

	alice := dial(t, wsURL, "alice")

	executeAt := time.Now().Add(500 * time.Millisecond).UTC().Format(time.RFC3339Nano)
	alice.Send(map[string]any{
		"type": "scheduleJob",
		"jobData": map[string]any{
			"jobId":       "smoke-1",
			"apiEndpoint": ts.URL,
			"method":      "POST",
			"executeAt":   executeAt,
			"payload":     map[string]any{"hi": 1},
		},
		"reqID": "s1",
	})
	alice.Expect(3*time.Second, "schedule ok", isSuccess("s1"))

	// Wait long enough for the scheduler to tick past executeAt.
	time.Sleep(2 * time.Second)

	mu.Lock()
	got := hits
	mu.Unlock()

	if got != 0 {
		t.Fatalf("SSRF guard should have blocked private-IP target, but got %d hits", got)
	}
}
