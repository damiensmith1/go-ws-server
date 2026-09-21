# Load testing

Two tools. `cmd/wsload` measures one instance under load; `cmd/wsfanout`
measures what multiple instances cost each other. They answer different
questions and the second one is further down this page.

## Single instance

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

# Cross-instance fan-out

`cmd/wsfanout` measures the cost of the `PSUBSCRIBE ws:topic:*` design:
every instance receives every message for every topic, including topics
it has no subscribers for, which it decodes, locks for, finds nobody for,
and discards.

```bash
go run ./cmd/wsfanout -redis localhost:6379 -instances 8 -topics 32 -messages 800 -payload 1024
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `-redis` | `localhost:6379` | Redis shared by every instance. |
| `-instances` | `4` | Working instances; one extra idle instance is always added. |
| `-topics` | `40` | Topics, partitioned evenly across instances. |
| `-messages` | `500` | Messages published, round-robin across topics. |
| `-payload` | `0` | Extra bytes per message. **Egress scales with this.** |
| `-subscribers-per-topic` | `1` | Subscriber connections per topic. |

Instances run in one process. The headline result is a ratio, and a ratio
is unaffected by sharing a CPU, so co-locating them removes a pile of
orchestration without costing anything that matters.

One instance is deliberately given **no subscriptions**. Every message it
handles is therefore pure waste, so its mean handling time measures the
cost of a discard directly, with no arithmetic needed to separate it from
useful work.

## Results

### The waste factor is arithmetic

| Instances | Handled | Useful | Waste factor |
| --- | --- | --- | --- |
| 2 | 2,400 | 800 | 3.0x |
| 4 | 4,000 | 800 | 5.0x |
| 8 | 7,200 | 800 | 9.0x |
| 16 | 13,600 | 800 | 17.0x |

Exactly instance count plus one (the idle instance). This needed no
measurement — it follows from every instance subscribing to every topic —
but it confirms the harness is measuring what it claims to.

### Server CPU is not the problem

Discarding one irrelevant message costs **190–350ns**, consistent with
the fan-out benchmark: an unwanted message decodes, takes the lock, finds
an empty subscriber set and returns before the expensive `EncodePublish`.

At 100,000 messages/second that is **under 3% of a core per instance**.
Server CPU is not the constraint at any rate worth discussing.

### Redis egress is the problem

Redis delivers every `PUBLISH` once per subscribed instance, so its
network egress is `message size x instance count` per publish, wanted or
not. Measured via `total_net_output_bytes`:

| Payload | Bytes out per message per instance | 1 Gbps saturates at |
| --- | --- | --- |
| ~10 B (`{"seq":N}`) | 120 B | `msgs/sec x instances ≈ 1,000,000` |
| 1 KB | 1,158 B | `msgs/sec x instances ≈ 108,000` |

That second row is the one that matters. **With 1 KB messages, ten
instances at ~10,800 messages/second saturate a 1 Gbps Redis link** —
and none of that traffic is doing useful work beyond the first instance.

## What this says about S2 and S4

Interest-based routing (S2) and sharded pub/sub (S4) are **one change**,
not two: Redis sharded pub/sub has no pattern variant, so `SSUBSCRIBE`
forces per-channel subscription, which *is* interest-based routing.

Whether to build it is a single calculation:

```
your publish rate  x  instance count  x  message size  x  8
```

Compare against your Redis link. An order of magnitude below it, and
S2/S4 buys nothing worth the change. Close to it, and **Redis egress —
not server CPU — is the wall**, which is exactly what S2/S4 removes.

Run `wsfanout` with your real payload size before deciding. The answer
moves by an order of magnitude between a 10-byte and a 1 KB message.

### Decision: not built

Measured and closed rather than left open. The change is large — it
replaces pattern subscription with per-topic subscription and brings
three new failure modes with it:

- **Subscribe races.** A client subscribes to a topic this instance has
  never seen; the `SSUBSCRIBE` must land before the first publish or the
  message is missed, with no pattern subscription as a safety net.
  Wants a confirm-then-ack sequence on subscribe.
- **Unsubscribe churn.** The last local subscriber leaves and the
  instance must `SUNSUBSCRIBE`, but a new subscriber may arrive
  immediately. Wants a grace period.
- **Replay interaction.** The live-vs-replay buffering in `handleTopic`
  assumes the instance is already receiving live messages when replay
  begins. Per-topic subscription changes when that holds.

Against that: the numbers above show server CPU is a non-issue, and
Redis egress only becomes a wall at message rates and instance counts
far beyond what this server is deployed at. Building it would be paying
a large complexity cost for headroom nothing is consuming.

The measurement is reproducible (`cmd/wsfanout`), so this is a decision
that can be revisited against real numbers rather than re-argued.

## What this says about the scalability backlog

- **Sharding `bus.mu` by topic hash is not justified yet.** The parallel
  fan-out benchmark is *faster* per operation than the serial one at the
  same shape (970ns against 1.23µs), so the global mutex is not the
  bottleneck at these numbers.
- **The `PSUBSCRIBE ws:topic:*` ceiling** is measured by `cmd/wsfanout`,
  documented above, and the decision is recorded there: not built.
  Server CPU is irrelevant; Redis egress is the wall, and it sits far
  beyond this server's scale.
- **Sharding `bus.mu` by topic hash** is likewise not worth doing. The
  parallel fan-out benchmark runs *faster* per operation than the serial
  one at the same shape (970ns against 1.23us), so the global mutex is
  not the bottleneck. If it were, the parallel case would be slower.

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
