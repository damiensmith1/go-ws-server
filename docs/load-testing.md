# Load testing

`cmd/wsload` is a load generator for this server. It opens N subscriber
connections to one topic, publishes M messages from a separate
connection, and reports end-to-end delivery latency, throughput and loss.

```bash
go run ./cmd/wsload -url ws://localhost:8080/ws -subscribers 500 -messages 2000
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `-url` | `ws://localhost:8080/ws` | Endpoint. |
| `-subscribers` | `100` | Subscriber connections. |
| `-messages` | `1000` | Messages to publish. |
| `-rate` | `0` | Publishes per second; `0` is as fast as possible. |
| `-payload` | `256` | Payload size in bytes. |
| `-topic` | `loadtest` | Topic to use. |
| `-settle` | `3s` | Wait for stragglers after the last publish. |
| `-token` | — | Bearer token, if the server requires auth. |

Latency is measured publish-to-receipt, through Redis and back out via
fan-out, so it covers the whole path rather than any one component.

The tool reads the publisher's own replies and reports rejections
separately from loss. Without that distinction a rate-limited run looks
like message loss, which is a different problem with a different fix —
the first run of this tool reported 90% "loss" that was entirely the
default `RATE_LIMIT_MESSAGES_PER_SEC=50` rejecting the publisher.

## Results

Single instance, Apple M2, Go 1.25, Redis 8 on loopback, server and load
generator on the same machine. These are a **baseline for regressions**,
not a capacity model: one box with no network between the parts flatters
latency and understates the cost of real fan-out across instances.

### Sustained, within the rate limit

200 publishes/second, 256-byte payloads, `-rate 200`:

| Subscribers | Deliveries | Lost | Delivery rate | p50 | p90 | p99 | max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 100 | 50,000 | 0 | 19,566/s | 2.07ms | 3.97ms | 11.0ms | 50.6ms |
| 500 | 250,000 | 0 | 99,789/s | 2.48ms | 4.00ms | 27.6ms | 32.7ms |

Latency is flat in subscriber count at a fixed publish rate: 5x the
subscribers moves p50 from 2.07ms to 2.48ms. Fan-out cost per subscriber
is small enough that it does not show at this scale.

### Burst, unthrottled

2,000 messages published as fast as the connection allows, 500
subscribers, 256-byte payloads:

| Deliveries | Lost | Publish wall time | Delivery rate | p50 | p99 | max |
| --- | --- | --- | --- | --- | --- | --- |
| 1,000,000 | 0 | 232ms | 4,313,340/s | 2.75s | 5.19s | 5.24s |

**Nothing is lost; latency is what degrades.** 8,627 publishes/second
against a delivery path that drains at roughly 4.3M deliveries/second
builds a queue, and the queue is paid for in latency rather than drops.
That is the intended behaviour, and it is worth knowing which way this
server fails: it will make you wait before it makes you lose data.

With 1KB payloads and the default 1MB per-socket buffer, 3,000 messages
to 500 subscribers delivered all 1,500,000 with no loss, and the
publisher's own write rate self-throttled from 8,627/s to 673/s. The
backpressure reaches the publisher before the drop thresholds are hit.

### What did not happen

No drops and no slow-consumer evictions in any run above
(`ws_frames_dropped_total` and
`ws_connections_closed_total{reason="slow_consumer"}` stayed at zero).
The drop path needs a consumer that genuinely stops reading, not merely
a fast publisher — a well-behaved client that reads in a loop keeps up
or applies backpressure first.

## What this says about the scalability backlog

- **Sharding `bus.mu` by topic hash is not justified yet.** The parallel
  fan-out benchmark is *faster* per operation than the serial one at the
  same shape (970ns against 1.23µs), so the global mutex is not the
  bottleneck at these numbers.
- **The `PSUBSCRIBE ws:topic:*` ceiling is untested here**, because it
  only bites with multiple instances: every instance receives every
  message for every topic regardless of local subscribers. A single-box
  run cannot see it. Measuring it needs several instances and a topic set
  much larger than any one instance's subscriptions.

## Reproducing

```bash
redis-server --port 6399 --save '' --appendonly no --daemonize yes

REDIS_HOST=127.0.0.1 REDIS_PORT=6399 \
WEBSOCKET_PORT=8099 METRICS_ADDR=:9099 \
RATE_LIMIT_MESSAGES_PER_SEC=100000 LOG_LEVEL=error \
go run .

go run ./cmd/wsload -url ws://localhost:8099/ws \
  -subscribers 500 -messages 2000 -settle 8s
```

Raise `RATE_LIMIT_MESSAGES_PER_SEC`, or pass `-rate` to stay under it;
otherwise you are measuring the rate limiter.
