# go-ws-server

A Redis-backed WebSocket server in Go: pub/sub topics with replay, topic
locking, per-user broadcast, and a distributed scheduler for delayed and
recurring HTTP calls.

Every instance is stateless. Redis is the only coordination point, so you
scale by adding processes behind any load balancer that speaks WebSocket
upgrade.

It is usable two ways: as a **standalone server** driven by environment
variables, or as a **Go package** embedded in your own binary, with
pluggable authentication, per-topic authorization, message routing,
custom protocol verbs and middleware.

```bash
go run .                                  # standalone
import ".../go-ws-server/wsserver"        # embedded
```

## Contents

| | |
| --- | --- |
| [Quickstart](#quickstart) | Run it locally or in Docker |
| [Message protocol](#message-protocol) | Every wire verb, with examples |
| [Authentication](#authentication) | Who is this? JWT, custom verifiers, token expiry |
| [Authorization](#authorization) | May they do this, here? Per-topic policy |
| [Guards and limits](#guards-and-limits) | Rate limiting, eviction, payload caps, SSRF |
| [Scaling](#scaling) | How it distributes, and measured numbers |
| [Observability](#observability) | Metrics, dashboard, health endpoints, log correlation |
| [Embedding](#embedding) | Public packages and every extension point |
| [Configuration](#configuration) | Full environment variable reference |
| [Testing](#testing) | Unit, integration, load, and smoke |

## Features

**Messaging**

- **Pub/sub topics with replay** — subscribe, unsubscribe, publish. Messages route across all instances via Redis Pub/Sub; a bounded per-topic Redis Stream lets late subscribers replay what they missed by timestamp or stream ID.
- **Acknowledgements** — clients ack a stream ID and the server stores the cursor per user and topic, so `subscribe` with `since: "ack"` resumes where they left off without the client remembering a position across restarts.
- **Delivery reports** — opt in per publish to learn how many subscribers a message reached, was dropped for, and was filtered from.
- **Topic locking** — atomic publish or subscribe locks with TTL and renewal.
- **Per-user broadcast** — fan out to every socket sharing a `userKey`, on any instance.
- **Presence** — query who is subscribed to a topic, list your own subscriptions, and optionally publish connect/disconnect events to a topic.
- **Job scheduler** — HTTP calls at a future time, with optional interval, validity window and retry policy. Distributed via Redis sorted sets and atomic per-job claims.
- **Protocol versioning** — frames may declare a `version`; unsupported versions are rejected with the supported range.

**Security**

- **Pluggable authentication** — HS256 JWT by default, algorithm-pinned, with optional audience and issuer. Any `auth.Verifier` can replace it.
- **Per-topic authorization** — gate subscribe, publish and lock by topic. Declarative glob rules from configuration, or any `authz.Authorizer` in code.
- **Token expiry enforced for the life of the socket** — a credential that expires closes its connection rather than lasting until idle timeout.
- **Origin allowlist** — configurable `Origin` checking for browser upgrades.
- **SSRF protection** — outbound scheduler requests are validated *and* their TCP connect is gated, blocking private, loopback, link-local, CGNAT and multicast targets even under DNS rebinding or HTTP redirects.
- **Log redaction** — any field whose key ends in `password`, `token`, `authorization` or `secret` is scrubbed at the handler level.

**Operations**

- **Prometheus metrics** on a separate listener, with no client-controllable label values.
- **Liveness and readiness endpoints** — `/readyz` pings Redis, so an instance that has lost it stops advertising as routable.
- **Graceful shutdown** — queued frames are flushed before exit, within a configurable bound.
- **Log correlation** — every line from a connection carries `connID`, `userKey` and, per frame, `reqID`.
- **Grafana dashboard** covering every exported metric.
- **Load and fan-out measurement tools** with published baseline numbers.

**Extensibility**

- **`Judge` hook** — replace exact-topic fan-out with a per-message routing decision.
- **Custom verbs and middleware** — extend the protocol without forking dispatch.
- **Connection lifecycle hooks** — for per-connection state held outside the server.
- **Redis topology support** — single, Cluster or Sentinel via one setting.

## Requirements

- Go 1.25+
- Redis 6+ (anything with `EVAL`, `SET NX EX` and Streams)

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

## Connecting

```
ws://localhost:8080/ws?userKey=alice&keepAlive=true                 # insecure mode
ws://localhost:8080/ws?token=<jwt>&keepAlive=true                   # JWT mode
```

`keepAlive=true` enables server-driven ping/pong. The server pings every half-`WEBSOCKET_TIMEOUT` and resets the idle timer on each pong.

## Message Protocol

Every inbound frame may carry a `version`:

```json
{ "type": "subscribe", "topic": "chat", "version": 1 }
```

Omitting it means the current version, so clients written before
versioning keep working. A frame naming a version this server does not
implement is rejected with an error stating the supported range, rather
than having its fields silently misread — which is the migration path a
future wire change needs.

Current version: **1**. Supported: **1 to 1**.

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

### Presence

Ask who is subscribed to a topic. Gated as a read of that topic: the
membership list is as sensitive as the messages, so a client that may not
subscribe may not enumerate subscribers either.

```json
{ "type": "presence", "topic": "chat", "reqID": "r1" }
```

```json
{ "type": "presence", "reqID": "r1", "topic": "chat",
  "subscribers": ["alice", "bob"], "count": 2 }
```

Subscribers are userKeys, deduplicated across sockets — a user with three
connections to the topic appears once.

### Delivery reports

A publisher normally learns nothing about what happened to its message.
Set `report` on a publish to find out:

```json
{ "type": "publish", "topic": "chat", "data": {"x":1}, "report": true }
```

```json
{ "type": "deliveryReport", "topic": "chat", "streamId": "1699999999999-0",
  "instance": "7f3a…", "delivered": 812, "dropped": 3, "filtered": 0 }
```

`dropped` counts subscribers whose send queue refused the frame;
`filtered` counts subscribers a `bus.Judge` excluded. They are separate
because they mean opposite things — a drop is a problem, a filter is the
routing policy working.

**You get one report per instance, not one total.** Each instance reports
only what it fanned out, which is why the frame carries `instance`.
Aggregating would mean knowing how many instances are live and waiting
for all of them, turning every reported publish into a distributed
barrier; partial reports that arrive promptly are more useful than a
total that may never come.

Off by default: a report costs an extra pub/sub message per instance that
fans the message out. A `Subscriber` that predates this feature is
counted as delivered, since there is no way for it to say otherwise and
guessing "dropped" would make reports wrong for every existing
implementation.

### Acknowledgements

Delivery is fire-and-forget by default. A client that wants at-least-once
delivery acknowledges what it has processed:

```json
{ "type": "ack", "topic": "chat", "streamId": "1699999999999-0" }
```

The `streamId` is the one carried on every delivered `publish` frame. The
server stores the cursor per userKey and topic, and a later subscribe can
resume from it:

```json
{ "type": "subscribe", "topic": "chat", "since": "ack" }
```

This is the difference from plain `since`: the client does not have to
remember a position across restarts, and a user's reading position
survives the socket that established it.

Two properties worth knowing:

**Cursors only move forward.** Redelivery means a client can legitimately
re-ack something it has already acked, and an out-of-order ack must not
rewind the cursor and replay everything after it again. Stream IDs are
compared numerically, not lexicographically — `"10-0"` sorts before
`"9-0"` as a string, which would treat a newer cursor as older.

**With nothing acked, `since: "ack"` behaves like a plain subscribe.** It
does not replay the whole stream, so a first-time subscriber is not
flooded with history it never asked for.

Cursors expire after `ACK_CURSOR_TTL_SECONDS` (default 7 days). Set it
above your longest expected client absence: a returning client whose
cursor has expired silently resumes from the top of the stream.

### Presence events

Set `PRESENCE_TOPIC` and the server publishes to it when a userKey
becomes present or absent:

```json
{ "event": "connected",    "userKey": "alice", "at": "2026-09-21T10:04:01Z" }
{ "event": "disconnected", "userKey": "alice", "at": "2026-09-21T10:41:12Z" }
```

Fired on the **first** and **last** socket only, so a user with three
tabs open produces one `connected` and one `disconnected`, not three of
each.

Off by default: publishing user activity to a topic others may subscribe
to should be a deliberate choice. If you enable it, cover the topic in
`AUTHZ_RULES` — otherwise any authenticated client can watch everyone
come and go. Publishing failures are logged and swallowed, since a
presence feed must never be able to fail a connection or a disconnection.

### List subscriptions

Ask which topics this client is subscribed to. Ungated: it reveals only
what the caller already did. Useful after a reconnect to reconcile
client-side state against the server's.

```json
{ "type": "listSubscriptions", "reqID": "r2" }
```

```json
{ "type": "subscriptions", "reqID": "r2", "topics": ["chat", "alerts"] }
```

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

To plug in a custom auth scheme (mTLS, opaque session tokens, header validation) implement `auth.Verifier` and pass it as `wsserver.Options.Verifier`. Set `Result.ExpiresAt` if your credential expires and the server will close the socket at that point; leave it zero if it does not.

### Token expiry

`exp` is enforced for the life of the socket, not just at the handshake.
Verification runs once, at upgrade; without further action a token that
expires — or is revoked — keeps its connection until the idle timeout,
and indefinitely if the client keeps pinging.

When the verifier reports a deadline, the server closes the socket at
that moment with close code **1008 (policy violation)** and reason
`token expired`, which a client can tell apart from an idle close or a
restart and use as its cue to re-authenticate and reconnect. A token
with no `exp` claim gets no deadline. Closes are counted under
`ws_connections_closed_total{reason="token_expired"}`.

A custom `auth.Verifier` opts in by setting `Result.ExpiresAt`; leaving
it zero preserves the previous behaviour.

## Authorization

Authentication answers "who is this?" once, at the upgrade.
Authorization answers "may this identity do this, *here*?" on every frame
that names a topic. Without the second, any client that can connect can
read and write every topic on the server.

Three actions are gated: `subscribe` (reading a topic, including replay),
`publish` (writing to it), and `lock` (taking, renewing or releasing a
topic lock — at least as powerful as the action it locks).
`unsubscribe` is deliberately ungated: dropping your own subscription
only ever removes access, and gating it would let a policy change trap a
client inside a topic it can no longer read.

### Declarative rules

Set `AUTHZ_RULES` to a JSON object mapping each action to glob patterns:

```json
{
  "subscribe": ["tenant.{userKey}.*", "announcements.*"],
  "publish":   ["tenant.{userKey}.*"],
  "lock":      ["tenant.{userKey}.*"]
}
```

- `*` matches exactly one dot-separated segment, so `orders.*` matches
  `orders.created` but not `orders.eu.created`.
- `{userKey}` is replaced by the caller's verified identity. The
  substitution is escaped, so a userKey containing `*` cannot widen its
  own pattern into other tenants' topics.
- **An action you omit denies every topic for that action.** A policy
  that forgets `lock` disables locking rather than leaving it open.
- An unknown action name or an invalid pattern is rejected at startup,
  not silently ignored.

Leaving `AUTHZ_RULES` unset preserves the previous behaviour — every
authenticated client may act on every topic — and logs a warning at
startup, the same way an unset `AUTH_JWT_SECRET` does.

### Custom policy

For anything rules cannot express, implement `authz.Authorizer`:

```go
type Authorizer interface {
    Authorize(ctx context.Context, req authz.Request) error
}
```

`req` carries the `UserKey`, the `Action`, the `Topic` and the
credential's `Claims`, so a policy can read roles, tenants or scopes
without re-parsing the token per frame.

Return `nil` to allow, `authz.ErrDenied` (or an error wrapping it) to
refuse, and **any other error when you could not reach a verdict**. That
distinction is load-bearing: a denial is reported to the client as
`Not authorized to <action> topic <topic>.`, while a failure to decide
fails closed but reports `Internal error`, so an outage in your policy
store is not mistaken for a permission change. The two are counted
separately as `authz_decisions_total{action,decision}` with `decision` of
`allowed`, `denied` or `error`.

Implementations are called on the hot path of every topic-bearing frame,
possibly concurrently, so they must be safe for concurrent use and must
not block unboundedly.
## Guards and limits

Everything a client can push on is bounded, and every bound is configurable.

| Guard | Setting | Behaviour |
| --- | --- | --- |
| Inbound message rate | `RATE_LIMIT_MESSAGES_PER_SEC` | Sliding window per userKey in Redis. Over-limit frames are rejected, not queued. |
| Job scheduling rate | `RATE_LIMIT_JOBS_PER_MIN` | Separate bucket, same mechanism. |
| Inbound frame size | `MAX_PAYLOAD_BYTES` | Oversize frames close the connection. |
| Outbound backlog | `MAX_BUFFERED_BYTES` | Per socket. Fan-out messages drop above it rather than blocking the bus. |
| Stalled consumers | `MAX_CONSECUTIVE_DROPS` | A connection that drops this many messages back to back is closed with 1013. Any successful send resets the run, so an occasional drop under burst is not punished. `0` disables. |
| Idle connections | `WEBSOCKET_TIMEOUT` | Closed unless the client requested `keepAlive`. |
| Credential lifetime | JWT `exp` | Enforced for the life of the socket, not just at the handshake. |
| Replay history | `STREAM_MAX_LENGTH`, `STREAM_TTL_SECONDS` | Bounds per-topic stream growth. |
| Ack cursors | `ACK_CURSOR_TTL_SECONDS` | Expires stored positions so keys do not accumulate. |
| Shutdown | `SHUTDOWN_TIMEOUT_MS` | Bounds the flush of queued frames on exit. |
| Browser origins | `ALLOWED_ORIGINS` | Empty keeps gorilla's same-origin default; `*` allows all. |
| Scheduler targets | `SCHEDULER_ALLOWED_HOSTS` | Host allowlist on top of the SSRF guard. |

**Backpressure reaches the publisher.** Under a burst that outruns the
delivery path, the server queues rather than drops, and the publisher's
own write rate slows. In measurement, 1.5M deliveries completed with zero
loss while the publisher self-throttled from 8,627 to 673 messages/second.

### Security notes

- **Origin checks**: set `ALLOWED_ORIGINS` to a comma-separated allowlist for browser clients. Empty keeps gorilla's same-origin default. `*` accepts any origin and is for development only.
- **JWT algorithm**: HS256 is pinned via `WithValidMethods` to defend against algorithm-confusion attacks. Other algorithms require modifying `internal/auth/auth.go`.
- **Insecure mode** (no `AUTH_JWT_SECRET`) is intended for local development only. The startup log is loud about this on purpose.
- **Logging redaction** scrubs values whose key name ends in `password`, `token`, `authorization`, or `secret`. It is applied at the slog handler level, so it covers any logging call. For nested structs, implement `slog.LogValuer` so fields can be walked.
- **SSRF**: outbound scheduler URLs are double-checked: pre-flight DNS validation *and* a `net.Dialer.Control` hook that inspects the actual IP being dialed. The dial-time check is the only defense that survives DNS rebinding.

**Metric cardinality**: no metric is labelled by topic, userKey or job ID.
Frame types are bounded to verbs the server knows — built-in or registered
through `handler.Registry` — so a client sending junk `type` values cannot
mint unbounded time series.

## Scaling

The server is stateless. Run as many instances as you like behind any TCP/HTTP load balancer that supports WebSocket upgrade. The shared Redis is the only coordination point:

- Topic subscriptions and locks live in Redis.
- `publish` messages are written to a per-topic Redis Stream (durable history) and announced via Redis Pub/Sub (live fan-out).
- Late subscribers `XRANGE` from their `since` cursor; the server dedupes the brief window where stream and pub/sub overlap.
- Scheduled jobs are claimed atomically (`SET NX EX`), so any instance can run any job exactly once.

### Redis topology

`REDIS_ADDRS` selects it: one address is a plain client, several are a
Cluster client, and any number with `REDIS_MASTER_NAME` is a Sentinel
failover client.

Note that Redis broadcasts plain `PUBLISH` to every node, so in Cluster
mode the streams and keys shard but the pub/sub path does not.

### Measured

Single instance, M2, Redis on loopback:

| Scenario | Result |
| --- | --- |
| 500 subscribers, 200 publishes/s | 250,000 deliveries, 0 lost, p50 **2.5ms** |
| 500 subscribers, unthrottled burst | 1,000,000 deliveries, 0 lost, p50 2.75s |
| Cross-instance discard cost | **190–350ns** per irrelevant message |
| Redis egress | `message size × instance count` per publish |

Two tools produce these. `cmd/wsload` drives one instance and reports
delivery latency, throughput and loss. `cmd/wsfanout` runs several
instances against one Redis and measures what they cost each other.

Full method, numbers and the scalability trade-offs they settle:
[docs/load-testing.md](docs/load-testing.md).

## Observability

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

### Metrics

Connection lifecycle and close reasons, frame throughput and drops by
cause, bus fan-out size and latency, judge outcomes, replay volume,
authorization decisions, rate-limit decisions, Redis command latency and
errors, and scheduler outcomes.

An importable Grafana dashboard covering every exported metric lives at
[`deploy/grafana/go-ws-server.json`](deploy/grafana/go-ws-server.json),
with scrape config and panel notes in
[`deploy/grafana/README.md`](deploy/grafana/README.md).

### Log correlation

Every log line written while serving a connection carries `connID` and
`userKey`; lines written while handling a frame that supplied a `reqID`
carry that too.

```json
{"level":"INFO","msg":"websocket client connected","connID":"6f2c…","userKey":"alice"}
{"level":"ERROR","msg":"message handling failed","connID":"6f2c…","userKey":"alice","reqID":"r7","type":"publish"}
```

`connID` is per socket, not per user, which is the case that matters: a
user with several tabs open cannot be told apart by `userKey` and
timestamp alone. It is also the identifier a `bus.Judge` addresses a
subscriber by, so a routing decision and the logs for the connection it
applied to line up.

## Embedding

The command in the repository root is a thin wrapper over package
`wsserver`. Import it to run the server inside another Go program, or to
supply policy in code rather than through environment variables:

```go
import "github.com/damiensmith1/go-ws-server/wsserver"

srv, err := wsserver.New(wsserver.Options{
    ListenAddr: ":8080",
    RedisAddrs: []string{"localhost:6379"},
    Verifier:   myVerifier,   // auth.Verifier
    Authorizer: myAuthorizer, // authz.Authorizer
})
if err != nil {
    return err
}
return srv.Run(ctx)
```

`Options` is plain data with working zero values, so set only what differs
from the defaults. To keep environment configuration and override a piece
of it:

```go
opts, err := wsserver.OptionsFromEnv()
opts.Authorizer = myAuthorizer
```

Some things cannot be expressed as configuration and are supplied in code.
Leaving any of them nil falls back to the same default the command uses:

| Option | Replaces |
| --- | --- |
| `Verifier` | How upgrades are authenticated |
| `Authorizer` | Per-topic access policy |
| `Judge` + `Candidates` | Exact-topic fan-out, with a per-message routing decision |
| `Registry` | Adds protocol verbs |
| `Middleware` | Wraps every inbound frame |
| `OnConnect` / `OnDisconnect` | Connection lifecycle notifications |
| `Metrics` | The collector set to record into |

`srv.Bus()` publishes from inside the host process without a websocket
round trip, and `srv.Metrics().Registry()` exposes the collectors on your
own HTTP server instead of a second listener.

### Custom verbs and middleware

`handler.Registry` adds verbs without forking the dispatch switch:

```go
reg := handler.NewRegistry()
reg.Register("typing", func(ctx context.Context, req *handler.Request, rw handler.Responder) (string, error) {
    // req carries UserKey, ConnID, Claims and the decoded Envelope.
    return "ok", nil
})

opts.Registry = reg
```

Returning a non-empty string sends a success frame carrying it, tagged
with the request's `reqID`. Returning an error sends an error frame with
the error's message — **error text is client-facing**, so keep internal
detail out of it. Returning `"", nil` sends nothing, which is what a
handler that already wrote its own frames through the `Responder` wants.

The registry is consulted **before** the built-in verbs, so a deployment
can override `publish` or `subscribe`. That is deliberate but sharp:
overriding a built-in means taking on its authorization and locking too.

Built-in validation knows the built-in verbs' required fields and nothing
about yours, so it is skipped for registered verbs — your handler
validates its own frame. The protocol version check still applies.

`handler.Middleware` wraps every frame, built-in verbs included:

```go
opts.Middleware = []handler.Middleware{
    func(next handler.Handler) handler.Handler {
        return func(ctx context.Context, req *handler.Request, rw handler.Responder) (string, error) {
            start := time.Now()
            reply, err := next(ctx, req, rw)
            log.Info("frame", "type", req.Type(), "took", time.Since(start))
            return reply, err
        }
    },
}
```

The first element is outermost. A middleware that returns without calling
`next` has rejected the frame, and its error reaches the client like any
other.

Verbs reach a metric label, so the label set stays bounded to what the
server knows: built-ins plus whatever you register. Anything else is
counted as `unknown` rather than minting a time series per junk string.

### Connection lifecycle

`OnConnect` and `OnDisconnect` report every socket opening and closing,
for per-connection state you keep outside the server:

```go
opts.OnDisconnect = func(ctx context.Context, c handler.Conn) {
    myStore.drop(c.ConnID) // c also carries UserKey and Claims
}
```

`c.ConnID` is the same identifier that appears as `connID` in the logs,
that `handler.Request` carries, and that a `bus.Judge` addresses a
subscriber by — so state created on connect can be torn down on
disconnect against one stable key.

Distinct from `PRESENCE_TOPIC`, which reports only the **first and last**
socket for a userKey. That is the right granularity for "is this user
online"; it is the wrong one for per-connection state, since a user with
three sockets produces one presence event and three lifecycle calls.

Both run synchronously on the connection's goroutine, so they must not
block: a slow `OnConnect` delays the client's first frame and a slow
`OnDisconnect` delays cleanup. One Redis round trip is fine; anything
longer belongs in a goroutine.

### Custom routing: the `Judge` hook

By default a topic's subscribers all receive every message published to
it. A `bus.Judge` replaces that with a per-message decision — semantic
matching, per-recipient filtering, sampling, anything that has to look at
the payload:

```go
type Judge interface {
    Judge(ctx context.Context, msg bus.Message, candidates []bus.Candidate) ([]bus.Decision, error)
}
```

Two properties of the interface are deliberate.

**It is called once, at publish**, inside `PublishTopic`, and the
resulting recipient list is written into the message. Judging during
fan-out instead would break three things: every instance receives every
publish, so a non-deterministic judge would deliver to a subscriber on one
instance and not another; replay reads from a Redis stream, so a decision
not stored there is lost on reconnect; and fan-out is at-least-once, so a
recomputed decision can differ between delivery attempts for the same
message. Deciding once and carrying the result fixes all three.

**It receives every candidate at once**, not one call per subscriber.
That is the shape a remote classifier wants — one request carrying the
message and all the predicates. Measured against a real classifier, going
from 1 to 100 candidates in a single call left latency flat (193ms to
224ms) while per-subscriber calls would have been 100 sequential round
trips.

A failing Judge does not drop the message. `JudgeFailurePolicy` defaults
to `bus.DeliverAll`, falling back to exact-topic match, because a broker
that silently stops delivering when its classifier is unavailable is a
worse failure than one that briefly over-delivers. Use `bus.DeliverNone`
only when over-delivery is the more serious fault. Either way the failure
is counted under `bus_judge_calls_total{outcome}`.

Supply `Candidates` alongside it — a `bus.CandidateSource` returning the
**cluster-wide** subscribers for a topic, since the decision is recorded
for every instance, not just the publishing one. Subscribers are addressed
by `bus.Identified`; a `Subscriber` that does not implement it is never
filtered, so existing implementations keep working unchanged.

### Public packages

| Package | For |
| --- | --- |
| `wsserver` | Constructing and running the server. |
| `auth`     | Implementing `Verifier` to authenticate upgrades. |
| `authz`    | Implementing `Authorizer` to gate per-topic access. |
| `bus`      | `Subscriber` / `BroadcastTarget`, the `Judge` hook, and publishing in-process. |
| `handler`  | Custom verbs, middleware, and connection lifecycle hooks. |
| `protocol` | Encoding and decoding wire frames. |
| `metrics`  | The collector set and its registry. |

Everything under `internal/` is private and may change without notice.

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
| `AUTHZ_RULES`                  | —       | Per-topic authorization policy as JSON. Empty allows every topic to every authenticated client. |
| `PRESENCE_TOPIC`               | —       | Topic for connect/disconnect events. Empty disables the feed.     |
| `ACK_CURSOR_TTL_SECONDS`       | `604800` | Expiry for stored ack cursors (7 days).                          |
| `SHUTDOWN_TIMEOUT_MS`          | `10000` | Bound on graceful shutdown, including flushing queued frames.     |
| `WEBSOCKET_TIMEOUT`            | `300000` | Idle timeout in ms.                                              |
| `MAX_PAYLOAD_BYTES`            | `65536` | Hard limit on inbound WS frames; oversize frames close the conn.  |
| `MAX_BUFFERED_BYTES`           | `1048576` | Per-socket outbound buffer threshold; messages drop above this. |
| `MAX_CONSECUTIVE_DROPS`        | `100`   | Evict a connection after this many back-to-back drops. `0` disables eviction. |
| `ENABLE_COMPRESSION`           | `false` | Negotiate `permessage-deflate` on upgrade.                        |
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

### Load and fan-out

```bash
go run ./cmd/wsload   -url ws://localhost:8080/ws -subscribers 500 -messages 2000
go run ./cmd/wsfanout -redis localhost:6379 -instances 8 -payload 1024
```

See [docs/load-testing.md](docs/load-testing.md).

### Benchmarks

```bash
go test ./bus/ -run XXX -bench . -benchtime=2s
```

Fan-out by subscriber count, lock contention across topics, frame
encoding, and subscription churn.

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
