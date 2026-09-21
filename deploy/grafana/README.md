# Grafana dashboard

`go-ws-server.json` is an importable dashboard covering every metric the
server exports.

## Import

Grafana → Dashboards → New → Import → Upload JSON file, then pick your
Prometheus datasource. The `job` variable filters to one or more scrape
jobs; it defaults to all.

## Prometheus scrape config

`METRICS_ADDR` is a separate listener from the websocket port, on purpose
— runtime internals should not be reachable by clients. Scrape that port:

```yaml
scrape_configs:
  - job_name: go-ws-server
    static_configs:
      - targets: ["go-ws-server:9090"]
```

## What the panels are for

The dashboard is organised around the questions you actually ask during
an incident.

**Overview** — active connections, publish rate, delivery rate, drop
rate. Sustained non-zero drops are the single most useful early signal
that clients are not keeping up.

**Connections** — upgrades split by outcome, so a rise in
`rejected_auth` with no deploy points at expiring tokens rather than a
bad build. Closes split by reason, because `token_expired` (credential
churn), `slow_consumer` (eviction), `idle_timeout` (normal) and
`read_error` (clients disappearing) have nothing to do with each other
and a single "closes" number hides all of it.

**Fan-out** — delivery duration against subscribers per message. Read
them together: duration that grows faster than fan-out width means lock
contention, not more work.

**Replay** — truncations mean a client asked for history older than
`STREAM_MAX_LENGTH` retained. A persistent rate means the stream is too
short for your clients' reconnect window.

**Policy** — authorization, rate limiting and routing. `denied` and
`error` are deliberately separate series in both the authz and judge
panels: a denial is a client or policy problem, while an error means the
component could not reach a verdict at all, which is an outage in
whatever backs it. A dashboard that summed them would hide the outage
behind normal denial traffic.

**Scheduler and Redis** — queue depth, execution outcomes, claims lost
(normal in a multi-instance deployment; two instances raced and one
backed off), and Redis errors, which exclude `redis.Nil` and so should
sit flat at zero.

## Regenerating

The dashboard is checked in as JSON rather than generated at build time,
so it can be imported without running anything. If you add a metric, add
a panel — the metric names are in `metrics/metrics.go`.
