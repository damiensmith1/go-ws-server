package bus

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"

	"github.com/damiensmith1/go-ws-server/metrics"
)

// idSub is a fakeSub that can be addressed by a Judge.
type idSub struct {
	fakeSub
	id string
}

func (s *idSub) SubscriberID() string { return s.id }

func candidates(ids ...string) CandidateSource {
	return CandidateSourceFunc(func(context.Context, string) ([]Candidate, error) {
		out := make([]Candidate, 0, len(ids))
		for _, id := range ids {
			out = append(out, Candidate{ID: id, Criteria: "interest of " + id})
		}
		return out, nil
	})
}

func deliverTo(ids ...string) Judge {
	want := make(map[string]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}
	return JudgeFunc(func(_ context.Context, _ Message, cs []Candidate) ([]Decision, error) {
		out := make([]Decision, 0, len(cs))
		for _, c := range cs {
			out = append(out, Decision{ID: c.ID, Deliver: want[c.ID]})
		}
		return out, nil
	})
}

func newJudgingBus(t *testing.T, m *metrics.Metrics, cfg Config) *Bus {
	t.Helper()
	cfg.Metrics = m
	return New(nil, nil, nil, cfg, quietLogger())
}

func TestJudgeResolvesRecipients(t *testing.T) {
	m := metrics.New()
	b := newJudgingBus(t, m, Config{
		Candidates: candidates("a", "b", "c"),
		Judge:      deliverTo("a", "c"),
	})

	got, err := b.judge(context.Background(), "t", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("judge: %v", err)
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "c" {
		t.Fatalf("recipients = %v, want [a c]", got)
	}
	if n := testutil.ToFloat64(m.JudgeDecisions.WithLabelValues("ok")); n != 1 {
		t.Fatalf("ok counter = %v, want 1", n)
	}
}

// nil and empty are different on the wire and the difference decides
// whether a message reaches everyone or nobody.
func TestJudgeNilVersusEmpty(t *testing.T) {
	t.Run("no judge configured yields nil (deliver to all)", func(t *testing.T) {
		b := newJudgingBus(t, metrics.New(), Config{})
		got, err := b.judge(context.Background(), "t", json.RawMessage(`{}`))
		if err != nil || got != nil {
			t.Fatalf("got (%v, %v), want (nil, nil)", got, err)
		}
	})

	t.Run("a judge with no candidates yields nil, not starvation", func(t *testing.T) {
		b := newJudgingBus(t, metrics.New(), Config{
			Candidates: candidates(),
			Judge:      deliverTo(),
		})
		got, err := b.judge(context.Background(), "t", json.RawMessage(`{}`))
		if err != nil {
			t.Fatalf("judge: %v", err)
		}
		if got != nil {
			t.Fatalf("recipients = %v, want nil; a topic with no registered criteria must not be starved", got)
		}
	})

	t.Run("a judge that selects nobody yields empty, not nil", func(t *testing.T) {
		b := newJudgingBus(t, metrics.New(), Config{
			Candidates: candidates("a", "b"),
			Judge:      deliverTo(),
		})
		got, err := b.judge(context.Background(), "t", json.RawMessage(`{}`))
		if err != nil {
			t.Fatalf("judge: %v", err)
		}
		if got == nil {
			t.Fatal("recipients = nil, want an empty slice; nil would deliver to everyone the Judge just excluded")
		}
		if len(got) != 0 {
			t.Fatalf("recipients = %v, want empty", got)
		}
	})
}

func TestJudgeFailurePolicy(t *testing.T) {
	boom := JudgeFunc(func(context.Context, Message, []Candidate) ([]Decision, error) {
		return nil, errors.New("classifier unavailable")
	})

	// A broker that silently stops delivering when its classifier dies is
	// a worse failure than one that briefly over-delivers.
	t.Run("DeliverAll falls back to exact-topic match", func(t *testing.T) {
		m := metrics.New()
		b := newJudgingBus(t, m, Config{Candidates: candidates("a"), Judge: boom})

		got, err := b.judge(context.Background(), "t", json.RawMessage(`{}`))
		if err != nil {
			t.Fatalf("a Judge failure must not fail the publish: %v", err)
		}
		if got != nil {
			t.Fatalf("recipients = %v, want nil so everyone still receives", got)
		}
		if n := testutil.ToFloat64(m.JudgeDecisions.WithLabelValues("error")); n != 1 {
			t.Fatalf("error counter = %v, want 1; a failing Judge must stay visible", n)
		}
	})

	t.Run("DeliverNone drops the message", func(t *testing.T) {
		b := newJudgingBus(t, metrics.New(), Config{
			Candidates:         candidates("a"),
			Judge:              boom,
			JudgeFailurePolicy: DeliverNone,
		})
		got, err := b.judge(context.Background(), "t", json.RawMessage(`{}`))
		if err != nil {
			t.Fatalf("judge: %v", err)
		}
		if got == nil || len(got) != 0 {
			t.Fatalf("recipients = %v, want a non-nil empty slice", got)
		}
	})

	// A Judge is on the publish path, so a hung one would stall the
	// publishing client, not just its own goroutine.
	t.Run("a hung judge times out and is counted separately", func(t *testing.T) {
		m := metrics.New()
		hang := JudgeFunc(func(ctx context.Context, _ Message, _ []Candidate) ([]Decision, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})
		b := newJudgingBus(t, m, Config{
			Candidates:   candidates("a"),
			Judge:        hang,
			JudgeTimeout: 20 * time.Millisecond,
		})

		start := time.Now()
		if _, err := b.judge(context.Background(), "t", json.RawMessage(`{}`)); err != nil {
			t.Fatalf("judge: %v", err)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("judge blocked for %s; the timeout did not apply", elapsed)
		}
		if n := testutil.ToFloat64(m.JudgeDecisions.WithLabelValues("timeout")); n != 1 {
			t.Fatalf("timeout counter = %v, want 1", n)
		}
	})

	t.Run("a failing CandidateSource follows the same policy", func(t *testing.T) {
		b := newJudgingBus(t, metrics.New(), Config{
			Candidates: CandidateSourceFunc(func(context.Context, string) ([]Candidate, error) {
				return nil, errors.New("redis down")
			}),
			Judge: deliverTo("a"),
		})
		got, err := b.judge(context.Background(), "t", json.RawMessage(`{}`))
		if err != nil {
			t.Fatalf("a candidate lookup failure must not fail the publish: %v", err)
		}
		if got != nil {
			t.Fatalf("recipients = %v, want nil under DeliverAll", got)
		}
	})
}

// Matching by ID rather than by position means a Judge may reorder or
// coalesce, but a response that does not correspond to the question is a
// bug worth surfacing rather than misrouting on.
func TestResolveRejectsIncoherentDecisions(t *testing.T) {
	cs := []Candidate{{ID: "a"}, {ID: "b"}}

	t.Run("order does not matter", func(t *testing.T) {
		got, err := resolve(cs, []Decision{{ID: "b", Deliver: true}, {ID: "a", Deliver: true}})
		if err != nil || len(got) != 2 {
			t.Fatalf("got (%v, %v), want both delivered", got, err)
		}
	})

	t.Run("a candidate with no decision is not delivered to", func(t *testing.T) {
		got, err := resolve(cs, []Decision{{ID: "a", Deliver: true}})
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if len(got) != 1 || got[0] != "a" {
			t.Fatalf("recipients = %v, want [a]", got)
		}
	})

	t.Run("an unknown subscriber is an error", func(t *testing.T) {
		if _, err := resolve(cs, []Decision{{ID: "z", Deliver: true}}); err == nil {
			t.Fatal("want an error; delivering to an unasked-for subscriber is a routing bug")
		}
	})

	t.Run("a duplicate decision is an error", func(t *testing.T) {
		if _, err := resolve(cs, []Decision{{ID: "a", Deliver: true}, {ID: "a", Deliver: false}}); err == nil {
			t.Fatal("want an error; two verdicts for one subscriber is ambiguous")
		}
	})
}

// A Subscriber that predates the Judge interface has no ID. Filtering it
// out would silently drop its messages the moment a Judge is configured.
func TestUnidentifiedSubscribersAreNeverFiltered(t *testing.T) {
	allowed := map[string]struct{}{"a": {}}

	if !allowedSubscriber(allowed, &fakeSub{}) {
		t.Fatal("an unidentifiable subscriber was filtered out")
	}
	if !allowedSubscriber(nil, &idSub{id: "zzz"}) {
		t.Fatal("a nil allowlist must mean deliver to everyone")
	}
	if !allowedSubscriber(allowed, &idSub{id: "a"}) {
		t.Fatal("a listed subscriber was filtered out")
	}
	if allowedSubscriber(allowed, &idSub{id: "b"}) {
		t.Fatal("an unlisted subscriber was delivered to")
	}
}

// End to end: the decision made at publish time must survive the round
// trip through Redis and actually filter the fan-out. This is the whole
// point of putting Recipients in the payload rather than deciding during
// delivery.
func TestJudgedPublishFiltersFanout(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)

	pub := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	sub := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { pub.Close(); sub.Close() })

	m := metrics.New()
	b := New(pub, sub, fakeBroadcast{}, Config{
		StreamMaxLength: 100,
		Metrics:         m,
		Candidates:      candidates("wanted", "unwanted"),
		Judge:           deliverTo("wanted"),
	}, quietLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = b.Run(ctx) }()

	wanted := &idSub{id: "wanted"}
	unwanted := &idSub{id: "unwanted"}
	legacy := &fakeSub{} // no SubscriberID: predates the Judge interface
	for _, s := range []Subscriber{wanted, unwanted, legacy} {
		b.AddLocalSubscription("alerts", s)
	}

	// Give the PSUBSCRIBE a beat to land, as the other Run-based test does.
	time.Sleep(50 * time.Millisecond)

	if _, err := b.PublishTopic(ctx, "alerts", json.RawMessage(`{"sev":"warn"}`)); err != nil {
		t.Fatalf("PublishTopic: %v", err)
	}

	waitFor(t, 2*time.Second, func() bool { return len(wanted.Snapshot()) > 0 })
	// Give any wrongly-routed frame the same chance to arrive, so the
	// negative assertions below are not just winning a race.
	time.Sleep(50 * time.Millisecond)

	if got := len(wanted.Snapshot()); got != 1 {
		t.Fatalf("selected subscriber got %d messages, want 1", got)
	}
	if got := len(unwanted.Snapshot()); got != 0 {
		t.Fatalf("excluded subscriber got %d messages, want 0", got)
	}
	if got := len(legacy.Snapshot()); got != 1 {
		t.Fatalf("unidentifiable subscriber got %d messages, want 1: it must not be silently filtered", got)
	}
	if n := testutil.ToFloat64(m.FanoutFiltered); n != 1 {
		t.Fatalf("filtered counter = %v, want 1", n)
	}
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", within)
}
