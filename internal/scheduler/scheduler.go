// Package scheduler runs the distributed delayed/recurring HTTP-callout
// scheduler.
//
// All instances participate. They watch the same Redis sorted-set
// (jobScheduler) for due jobs; each instance attempts an atomic SETNX
// claim on a job before executing, so a given fire happens exactly once
// across the cluster. Jobs with intervals are re-enqueued after execution.
//
// The HTTP client used to deliver callouts is wrapped in two SSRF defenses:
//   - A pre-flight URL/DNS check rejects obvious targets (private hosts,
//     bad allow-list match, non-http schemes).
//   - A custom Dialer.Control hook rejects the actual IP being dialed,
//     surviving DNS rebinding and redirect attacks.
package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/damiensmith1/go-ws-server/internal/metrics"
	"github.com/damiensmith1/go-ws-server/internal/protocol"
	"github.com/damiensmith1/go-ws-server/internal/redisx"
	"github.com/damiensmith1/go-ws-server/internal/ssrf"
)

// Config configures the scheduler instance.
type Config struct {
	InstanceID  string
	Guard       *ssrf.Guard
	HTTPTimeout time.Duration // per-request HTTP timeout. 10s if zero.

	// Metrics is optional. A nil value gets a private collector set, so
	// call sites never need a nil check.
	Metrics *metrics.Metrics
}

// Scheduler is the distributed job runner. Construct with New, start with
// Run. Wake() can be called when a new job is added to nudge the loop.
type Scheduler struct {
	cfg    Config
	rdb    redis.UniversalClient
	log    *slog.Logger
	client *http.Client
	m      *metrics.Metrics

	mu     sync.Mutex
	wakeCh chan struct{}
}

// New builds a Scheduler. The HTTP client uses a Dialer.Control SSRF check
// and refuses redirects (matching the TS `maxRedirects: 0`).
func New(rdb redis.UniversalClient, cfg Config, log *slog.Logger) *Scheduler {
	if log == nil {
		log = slog.Default()
	}
	if cfg.HTTPTimeout == 0 {
		cfg.HTTPTimeout = 10 * time.Second
	}
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.New()
	}

	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
		Control:   ssrf.DialControl(),
	}
	transport := &http.Transport{
		DialContext:           dialer.DialContext,
		MaxIdleConns:          50,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	client := &http.Client{
		Timeout:   cfg.HTTPTimeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	return &Scheduler{
		m:      cfg.Metrics,
		cfg:    cfg,
		rdb:    rdb,
		log:    log,
		client: client,
		wakeCh: make(chan struct{}, 1),
	}
}

// Wake nudges the scheduler to recompute the next-due time. Non-blocking;
// safe to call from any goroutine after a job is added or removed.
func (s *Scheduler) Wake() {
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

// Run is the scheduler tick loop. It blocks until ctx is cancelled. The
// loop sleeps until the next due-at, runs every ready job, and sleeps
// again. When the queue is empty it waits indefinitely for Wake().
func (s *Scheduler) Run(ctx context.Context) error {
	s.log.Info("scheduler started")
	defer s.log.Info("scheduler stopped")

	for {
		nextMs, ok, err := redisx.NextDueAt(ctx, s.rdb)
		if err != nil {
			s.log.Error("nextDueAt failed", "err", err.Error())
			// On error, back off briefly and retry rather than burn CPU.
			if !sleepCtx(ctx, time.Second) {
				return nil
			}
			continue
		}

		// Refresh the depth gauge here rather than in tick(): the loop
		// parks for up to an hour on an empty queue, and this runs on
		// every wake. An empty queue needs no extra round trip.
		if !ok {
			s.m.SchedulerQueueDepth.Set(0)
		} else if n, err := redisx.JobCount(ctx, s.rdb); err == nil {
			s.m.SchedulerQueueDepth.Set(float64(n))
		}

		var wait time.Duration
		if !ok {
			wait = time.Hour // effectively forever; Wake() will short-circuit
		} else {
			wait = time.Until(time.UnixMilli(nextMs))
			if wait < 0 {
				wait = 0
			}
		}

		if wait > 0 {
			if !waitForWakeOrTimer(ctx, s.wakeCh, wait) {
				return nil
			}
			continue
		}

		s.tick(ctx)
	}
}

func waitForWakeOrTimer(ctx context.Context, wake <-chan struct{}, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-wake:
		return true
	case <-t.C:
		return true
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (s *Scheduler) tick(ctx context.Context) {
	now := time.Now()
	due, err := redisx.ListDueJobs(ctx, s.rdb, now.UnixMilli())
	if err != nil {
		s.log.Error("listDueJobs failed", "err", err.Error())
		return
	}
	for _, d := range due {
		s.processDue(ctx, d, now)
	}
}

func (s *Scheduler) processDue(ctx context.Context, d redisx.DueJobsResult, now time.Time) {
	jd := d.Job.JobData
	if jd == nil {
		// Malformed entry; remove and move on.
		_, _ = redisx.RemoveJobMember(ctx, s.rdb, d.RawMember)
		return
	}

	if jd.ValidUntil != "" {
		if vu, _, err := protocol.ParseValidUntil(jd.ValidUntil); err == nil && vu.Before(now) {
			s.log.Info("job expired (validUntil); removing", "jobId", jd.JobID)
			_, _ = redisx.RemoveJobMember(ctx, s.rdb, d.RawMember)
			return
		}
	}

	claimed, err := redisx.ClaimJob(ctx, s.rdb, jd.JobID, s.cfg.InstanceID, 30*time.Second)
	if err != nil {
		s.log.Error("claim job failed", "jobId", jd.JobID, "err", err.Error())
		return
	}
	if !claimed {
		s.m.SchedulerClaimLost.Inc()
		s.log.Debug("job already claimed; skipping", "jobId", jd.JobID)
		return
	}

	removed, err := redisx.RemoveJobMember(ctx, s.rdb, d.RawMember)
	if err != nil {
		s.log.Error("remove job failed", "jobId", jd.JobID, "err", err.Error())
		return
	}
	if !removed {
		// Another instance got it after our claim window. Bail.
		return
	}

	execStart := time.Now()
	execErr := s.executeJob(ctx, jd)
	s.m.SchedulerDuration.Observe(time.Since(execStart).Seconds())
	if execErr != nil {
		s.m.SchedulerExecuted.WithLabelValues("failure").Inc()
		s.log.Error("job execution failed", "jobId", jd.JobID, "err", execErr.Error())
	} else {
		s.m.SchedulerExecuted.WithLabelValues("success").Inc()
	}

	// Reschedule if it has an interval and it's still within validUntil.
	if jd.Interval > 0 {
		nextMs := now.UnixMilli() + jd.Interval
		stillValid := true
		if jd.ValidUntil != "" {
			if vu, _, err := protocol.ParseValidUntil(jd.ValidUntil); err == nil && vu.UnixMilli() < nextMs {
				stillValid = false
			}
		}
		if stillValid {
			jd.ExecuteAt = time.UnixMilli(nextMs).UTC().Format(time.RFC3339Nano)
			updated := redisx.StoredJob{JobID: jd.JobID, JobData: jd}
			if err := redisx.AddRescheduledJob(ctx, s.rdb, updated, nextMs); err != nil {
				s.log.Error("reschedule job failed", "jobId", jd.JobID, "err", err.Error())
			} else {
				s.log.Debug("job rescheduled", "jobId", jd.JobID, "nextExecutionTime", nextMs)
			}
		}
	}
}

func (s *Scheduler) executeJob(ctx context.Context, jd *protocol.JobData) error {
	if s.cfg.Guard != nil {
		if err := s.cfg.Guard.Validate(ctx, jd.APIEndpoint); err != nil {
			s.log.Error("job endpoint blocked by SSRF policy",
				"jobId", jd.JobID,
				"apiEndpoint", jd.APIEndpoint,
				"err", err.Error(),
			)
			return err
		}
	}

	maxAttempts := 1
	retryDelay := time.Second
	if jd.RetryPolicy != nil {
		if jd.RetryPolicy.MaxRetries > 0 {
			maxAttempts = jd.RetryPolicy.MaxRetries
		}
		if jd.RetryPolicy.RetryDelay > 0 {
			retryDelay = time.Duration(jd.RetryPolicy.RetryDelay) * time.Millisecond
		}
	}

	s.log.Info("executing job", "jobId", jd.JobID)

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := s.doRequest(ctx, jd); err != nil {
			lastErr = err
			s.log.Warn("job attempt failed",
				"jobId", jd.JobID,
				"attempt", attempt,
				"err", err.Error(),
			)
			if attempt < maxAttempts {
				if !sleepCtx(ctx, retryDelay) {
					return ctx.Err()
				}
				continue
			}
			s.log.Error("job exhausted retries", "jobId", jd.JobID, "attempts", maxAttempts)
			return lastErr
		}
		s.log.Info("job executed successfully", "jobId", jd.JobID)
		return nil
	}
	return lastErr
}

func (s *Scheduler) doRequest(ctx context.Context, jd *protocol.JobData) error {
	var body []byte
	if len(jd.Payload) > 0 {
		body = []byte(jd.Payload)
	}
	var bodyReader *bytes.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, jd.Method, jd.APIEndpoint, readerOrNil(bodyReader))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	for k, v := range jd.Headers {
		req.Header.Set(k, v)
	}
	if body != nil && req.Header.Get("Content-Type") == "" {
		// JSON payload by default; matches axios defaults.
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()

	// 1xx / 2xx are success. Treat anything else as failure for retry.
	if resp.StatusCode >= 400 {
		return fmt.Errorf("non-success status %d", resp.StatusCode)
	}
	return nil
}

func readerOrNil(r *bytes.Reader) *bytes.Reader {
	if r == nil {
		return nil
	}
	return r
}

// MarshalJobForLog returns a JSON-marshalable view of a JobData stripped
// of headers — used in error responses where we don't want to leak auth
// material if a client requests an echo.
func MarshalJobForLog(jd *protocol.JobData) ([]byte, error) {
	if jd == nil {
		return nil, errors.New("nil job")
	}
	clone := *jd
	clone.Headers = nil
	return json.Marshal(clone)
}

// ResumeOnStartup decides whether to start ticking immediately. Returns
// the count of pending jobs.
func ResumeOnStartup(ctx context.Context, rdb redis.UniversalClient, log *slog.Logger) (int64, error) {
	n, err := redisx.JobCount(ctx, rdb)
	if err != nil {
		return 0, err
	}
	if n > 0 {
		log.Info("resuming scheduler", "jobCount", n)
	} else {
		log.Info("no pending jobs; scheduler will start when one is added")
	}
	return n, nil
}
