# go-ws-server

A Redis-backed WebSocket server in Go with pub/sub topics, topic locking, per-user broadcast, and a job scheduler for delayed/recurring HTTP calls. Designed to scale horizontally — every server instance is stateless, with Redis as the shared coordinator and message bus.

## Features

- **Pub/sub topics with replay** — clients subscribe, unsubscribe, and publish to named topics. Messages are routed across all server instances via Redis Pub/Sub, and a bounded per-topic Redis Stream lets late subscribers replay missed messages by timestamp or stream ID.
- **Topic locking** — atomic publish or subscribe locks on a topic with TTL and renewal.
- **Per-user broadcast** — fan a message out to every socket connected with the same `userKey`, on any instance.
- **Job scheduler** — schedule HTTP calls at a future time, with optional interval, validity window, and retry policy. Distributed via Redis sorted sets and atomic per-job claims.
- **JWT authentication** — pluggable; HS256 by default with configurable audience/issuer.
- **SSRF protection** — outbound scheduler requests are validated *and* their underlying TCP connect is gated, blocking private/loopback/link-local/CGNAT/multicast targets even under DNS rebinding or HTTP redirects.
- **Rate limiting** — per-userKey, per-bucket fixed-window limits in Redis.
- **Optional TLS** — runs as `ws://` by default, `wss://` when `TLS_KEY_PATH` and `TLS_CERT_PATH` are set.
- **Keep-alive** — opt-in ping/pong to hold long-lived connections.
- **Prometheus metrics** — connection lifecycle, frame throughput and drops by cause, bus fan-out size and latency, replay volume, Redis command latency and errors, rate-limit decisions, and scheduler outcomes. Served on a separate listener so the websocket port exposes nothing extra. No metric is labelled by topic, userKey or job ID, so client input cannot inflate cardinality.
- **Structured logging** — `log/slog` JSON output with automatic redaction of any field whose key ends in `password`, `token`, `authorization`, or `secret`.

## Requirements

- Go 1.22+
- Redis 6+ (anything with `EVAL` and `SET NX EX`)

## Quickstart

```bash
git clone https://github.com/damiensmith1/go-ws-server.git
cd go-ws-server
cp .env.example .env       # fill in secrets, see Configuration below
go build ./...
./go-ws-server
```

The server listens on `WEBSOCKET_PORT` (default `8080`).

### Docker

```bash
docker build -t go-ws-server .
docker run -d \
  --name go-ws-server \
  -p 8080:8080 \
  --env-file .env \
  go-ws-server
```

For TLS, mount your cert directory and point `TLS_KEY_PATH` / `TLS_CERT_PATH` at the mounted paths.

## Configuration

All config is read from environment variables. See [`.env.example`](./.env.example) for the full list. Key ones:

| Variable                       | Default | Description                                                       |
| ------------------------------ | ------- | ----------------------------------------------------------------- |
| `REDIS_HOST` / `REDIS_PORT`    | —       | Redis location.                                                   |
| `REDIS_ADDRS`                  | —       | Comma-separated addresses; overrides `REDIS_HOST`/`REDIS_PORT`. Several addresses select a Cluster client. |
| `REDIS_MASTER_NAME`            | —       | Set with `REDIS_ADDRS` to use Sentinel failover.                  |
| `REDIS_PASSWORD`               | —       | Redis auth.                                                       |
| `WEBSOCKET_PORT`               | `8080`  | Listen port.                                                      |
| `ALLOWED_ORIGINS`              | —       | Comma-separated `Origin` allowlist for upgrades. Empty keeps the same-origin default; `*` allows all (dev only). |
| `METRICS_ADDR`                 | —       | Listen address for Prometheus `/metrics`, e.g. `:9090`. Empty disables it. |
| `READINESS_TIMEOUT_MS`         | `2000`  | Timeout for the Redis ping behind `/readyz`.                      |
| `WEBSOCKET_TIMEOUT`            | `300000` | Idle timeout in ms.                                              |
| `MAX_PAYLOAD_BYTES`            | `65536` | Hard limit on inbound WS frames; oversize frames close the conn.  |
| `MAX_BUFFERED_BYTES`           | `1048576` | Per-socket outbound buffer threshold; messages drop above this. |
| `TLS_KEY_PATH` / `TLS_CERT_PATH` | —     | Set both to run as `wss://`.                                      |
| `AUTH_JWT_SECRET`              | —       | Required for auth. When unset the server runs **insecure**.       |
| `AUTH_JWT_AUDIENCE`            | —       | Optional JWT `aud` claim to enforce.                              |
| `AUTH_JWT_ISSUER`              | —       | Optional JWT `iss` claim to enforce.                              |
| `STREAM_MAX_LENGTH`            | `1000`  | Per-topic replay history cap (`XADD MAXLEN ~`, approximate).      |
| `STREAM_TTL_SECONDS`           | —       | If set, periodically `XTRIM MINID` entries older than this.       |
| `RATE_LIMIT_MESSAGES_PER_SEC`  | `50`    | Per-userKey inbound message limit.                                |
| `RATE_LIMIT_JOBS_PER_MIN`      | `30`    | Per-userKey scheduleJob limit.                                    |
| `SCHEDULER_ALLOWED_HOSTS`      | —       | Comma-sep allow-list for scheduler URLs. Unset = any public host. |
| `LOG_LEVEL`                    | `info`  | slog level: `debug` `info` `warn` `error`.                        |
| `INSTANCE_ID`                  | random  | Identifies this instance for distributed job claims.              |

## Health endpoints

| Path | Port | Meaning |
| --- | --- | --- |
| `/healthz` | `WEBSOCKET_PORT` | Liveness. Always `200 ok` while the process is running. Use it to decide whether to restart the container. |
| `/readyz`  | `WEBSOCKET_PORT` | Readiness. Pings Redis and returns `503 redis unavailable` when it does not answer within `READINESS_TIMEOUT_MS`. Use it to decide whether to route traffic. |
| `/metrics` | `METRICS_ADDR`   | Prometheus exposition. Separate listener, not exposed to websocket clients. |

The distinction matters: every operation this server performs — fan-out,
replay, presence, locks, scheduling — is a Redis round trip. An instance
that has lost Redis still accepts sockets and still answers `/healthz`,
so without `/readyz` an orchestrator will keep routing clients to a
black hole.

## Authentication

When `AUTH_JWT_SECRET` is set, every connection must present an HS256 JWT whose `sub` claim equals the desired `userKey`. The token is read from either:

- `Authorization: Bearer <token>` header, **or**
- `?token=<token>` query string

The JWT verifier pins the algorithm to HS256 to avoid algorithm-confusion attacks.

```go
// Issue a token (server-side, e.g. on login)
import "github.com/golang-jwt/jwt/v5"

token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
    "sub": "alice",
    "exp": time.Now().Add(time.Hour).Unix(),
})
signed, _ := token.SignedString([]byte(os.Getenv("AUTH_JWT_SECRET")))
```

If `AUTH_JWT_SECRET` is **not** set, the server logs a loud warning at startup and accepts any `userKey` from the query string. This is intended for local development only.

To plug in a custom auth scheme (mTLS, opaque session tokens, header validation, etc.) implement the `auth.Verifier` interface and pass it into the server in `main.go`.

## Connecting

```
ws://localhost:8080/ws?userKey=alice&keepAlive=true                 # insecure mode
ws://localhost:8080/ws?token=<jwt>&keepAlive=true                   # JWT mode
```

`keepAlive=true` enables server-driven ping/pong. The server pings every half-`WEBSOCKET_TIMEOUT` and resets the idle timer on each pong.

## Message Protocol

All messages are JSON. The server replies with `{"type":"success", "reqID": "...", "message": "..."}` on success or `{"type":"error", "reqID": "...", "message": "..."}` on failure. The optional `reqID` round-trips so clients can correlate replies.

### Subscribe / Unsubscribe

```json
{ "type": "subscribe",   "topic": "yourTopicName" }
{ "type": "unsubscribe", "topic": "yourTopicName" }
```

`subscribe` accepts an optional `since` cursor for replay. Pass either an ISO timestamp, a numeric millisecond timestamp, or a stream ID returned by a previous publish (`<ms>-<seq>`):

```json
{ "type": "subscribe", "topic": "chat", "since": "2026-05-01T12:00:00Z" }
{ "type": "subscribe", "topic": "chat", "since": "1714589342175-0" }
```

The server replays every missed message **before** going live, dedupes against any concurrent live publishes, and tags replayed messages with `"replay": true`. `since` is **exclusive** — a message at exactly that cursor is not redelivered.

If the requested `since` is older than the oldest entry currently in the stream (i.e. the stream has been trimmed past that point), the server emits one more frame:

```json
{ "type": "replayTruncated", "topic": "chat", "oldestAvailable": "1714589300000-0" }
```

This tells the client: replay is starting from `oldestAvailable`, and earlier messages must come from elsewhere — typically a longer-retention event log. Clients that only need recent gaps can ignore the signal.

### Publish

```json
{
  "type": "publish",
  "topic": "yourTopicName",
  "data": { "key": "value" }
}
```

Subscribers (across all instances) receive:

```json
{ "type": "publish", "topic": "yourTopicName", "data": { "key": "value" }, "streamId": "1714589342175-0" }
```

`streamId` is the cursor a client should remember for reconnect — pass it back as `since` to resume from exactly where it left off.

### Lock / Unlock / Renew

```json
{ "type": "lockTopic",   "topic": "X", "lockType": "publish" }
{ "type": "unlockTopic", "topic": "X", "lockType": "publish" }
{ "type": "renewLock",   "topic": "X", "lockType": "publish" }
```

`lockType` is `"publish"` or `"subscribe"`. Locks have a 5-minute TTL; renew before they expire.

### Broadcast

```json
{ "type": "broadcast", "data": { "key": "value" } }
```

Fans out to every socket on every instance that's connected with the same `userKey`. Other sockets receive `{"type":"message","data":{...}}`. No reply is sent to the sender.

### Schedule a Job

```json
{
  "type": "scheduleJob",
  "jobData": {
    "jobId": "uniqueJobId",
    "executeAt": "2026-12-31T23:59:59.000Z",
    "interval": 10000,
    "validUntil": "2027-01-31T23:59:59.000Z",
    "apiEndpoint": "https://api.example.com/endpoint",
    "method": "POST",
    "headers": { "Authorization": "Bearer ..." },
    "payload": { "key": "value" },
    "retryPolicy": { "maxRetries": 3, "retryDelay": 1000 }
  }
}
```

| Field         | Description                                       |
| ------------- | ------------------------------------------------- |
| `jobId`       | Unique identifier (rejected if it already exists) |
| `executeAt`   | First execution time (ISO 8601)                   |
| `interval`    | Repeat interval in ms (omit for one-shot)         |
| `validUntil`  | Stop firing after this time                       |
| `apiEndpoint` | URL the scheduler calls (subject to SSRF policy)  |
| `method`      | `GET` `POST` `PUT` `DELETE`                       |
| `headers`     | Headers to include on the request                 |
| `payload`     | Request body                                      |
| `retryPolicy` | `{ maxRetries, retryDelay }` (optional)           |

### Remove a Job

```json
{ "type": "removeJob", "jobId": "uniqueJobId" }
```

## Scaling

The server is stateless. Run as many instances as you like behind any TCP/HTTP load balancer that supports WebSocket upgrade. The shared Redis is the only coordination point:

- Topic subscriptions and locks live in Redis.
- `publish` messages are written to a per-topic Redis Stream (durable history) and announced via Redis Pub/Sub (live fan-out).
- Late subscribers `XRANGE` from their `since` cursor; the server dedupes the brief window where stream and pub/sub overlap.
- Scheduled jobs are claimed atomically (`SET NX EX`), so any instance can run any job exactly once.

## Security notes

- **Origin checks**: `gorilla/websocket`'s default `CheckOrigin` is same-origin. Browser clients on a different origin must go through a reverse proxy or you must override the upgrader's `CheckOrigin` in `main.go`. Don't blindly accept any origin in production.
- **JWT algorithm**: HS256 is pinned via `WithValidMethods` to defend against algorithm-confusion attacks. Other algorithms require modifying `internal/auth/auth.go`.
- **Insecure mode** (no `AUTH_JWT_SECRET`) is intended for local development only. The startup log is loud about this on purpose.
- **Logging redaction** scrubs values whose key name ends in `password`, `token`, `authorization`, or `secret`. It is applied at the slog handler level, so it covers any logging call. For nested structs, implement `slog.LogValuer` so fields can be walked.
- **SSRF**: outbound scheduler URLs are double-checked: pre-flight DNS validation *and* a `net.Dialer.Control` hook that inspects the actual IP being dialed. The dial-time check is the only defense that survives DNS rebinding.

## Testing

```bash
go test -race ./...                              # unit tests, ~5s
go test -tags=integration -timeout=180s -v .     # full stack, needs Docker
```

Unit tests use [miniredis](https://github.com/alicebob/miniredis) — they're fast and have no external dependencies. The integration suite (`integration_test.go`, `//go:build integration`) boots a real Redis 7 container via [testcontainers-go](https://golang.testcontainers.org/), starts the server in-process on a random port, and drives multi-client scenarios:

- **Pub/sub fan-out** across three subscribers and a publisher.
- **Subscribe replay** — late subscriber gets the backlog via `since=0-0`, frames marked `replay:true` and in order.
- **Replay truncation** — subscribing with a `since` cursor older than the stream's oldest entry emits a `replayTruncated` frame with `oldestAvailable`.
- **Broadcast scoping** — fans out to every socket of a given userKey only; never crosses to other users.
- **Topic publish lock** — a third-party publisher gets `error` while the lock is held; succeeds after unlock.
- **JWT auth path** — valid token connects, valid via `Authorization: Bearer` header, missing/wrong-secret/expired/userKey-mismatch tokens all rejected with HTTP 401.
- **Rate limiting** — with `RATE_LIMIT_MESSAGES_PER_SEC=3` and 12 rapid publishes, exactly 3 succeed and 9 are rejected with `Rate limit exceeded`.
- **Multi-instance** — two server instances against the same Redis: a topic publish on instance B reaches a subscriber on instance A; broadcasts fan out across instance boundaries for the same userKey.
- **Idle timeout** — a connection without `keepAlive` is closed by the server after `WEBSOCKET_TIMEOUT`.
- **Keepalive** — with `keepAlive=true`, the server's pings + the client's auto-pongs hold the socket open well past the idle timeout.
- **Scheduler + SSRF** — a job pointed at a private-IP target is queued, the scheduler ticks past `executeAt`, and the dial-time SSRF guard refuses to connect (0 hits on the local target). Proves both halves of the pipeline.

You'll need Docker running. The first run pulls `redis:7-alpine` (~30 MB). Full suite runs in about 20 seconds.

## Smoke testing

A tiny interactive client lives in `cmd/wsprobe`. It connects to a running server and lets you type one JSON message per line:

```bash
go run ./cmd/wsprobe -url ws://localhost:8080/ws -userKey alice
> {"type":"subscribe","topic":"chat"}
> {"type":"publish","topic":"chat","data":{"hi":1}}
```

Use `-token "$JWT"` instead of `-userKey` when running with `AUTH_JWT_SECRET`, and `-keepAlive` to request server-driven ping/pong.

## Development

```bash
go test ./...        # unit tests
go test -race ./...  # with the race detector
go vet ./...         # static checks
go run .             # local server (needs Redis)
go run ./cmd/wsprobe # interactive client for smoke tests
```

## License

[MIT](./LICENSE)
