package bus

import (
	"context"
	"encoding/json"
	"fmt"
)

// Judge decides which subscribers of a topic should receive a message.
//
// Routing is otherwise exact-topic match: every subscriber of a topic
// gets every message published to it. A Judge replaces that with a
// per-message decision — semantic matching, per-recipient filtering,
// sampling, anything that needs to look at the payload.
//
// # Called once, at publish
//
// Judge runs on the publishing instance, inside PublishTopic, before the
// message reaches the stream. It is deliberately not called during
// fan-out, and the difference is not an optimisation:
//
//   - Consistency. Every server instance receives every publish. Judging
//     during fan-out would run the Judge once per instance over the same
//     message, and any non-determinism — a model, a clock, a remote
//     policy service — would deliver the message to a subscriber on one
//     instance and not on another.
//   - Replay. Messages are replayed from a Redis stream when a subscriber
//     reconnects with `since`. A decision made at delivery time is not in
//     the stream, so replay would either re-run the Judge against a stale
//     world or skip filtering entirely.
//   - Redelivery. Fan-out is at-least-once. A decision recomputed per
//     delivery attempt can differ between attempts for the same message.
//
// Deciding once and carrying the result with the message makes all three
// fall out for free. The cost is that a Judge must be able to see every
// candidate subscriber cluster-wide, not just the ones local to this
// instance, which is why Candidates comes from shared state rather than
// from the local subscriber map.
//
// # Batching
//
// Judge receives every candidate at once rather than being called per
// subscriber. This is the shape a remote classifier wants: one request
// carrying the message and all the predicates, against one round trip per
// subscriber. Implementations that call out to a network service should
// exploit it.
//
// # Failure
//
// Returning an error does not drop the message. The bus applies
// Config.JudgeFailurePolicy — by default DeliverAll, because a broker
// that silently stops delivering when its classifier is unavailable is a
// worse failure than one that briefly over-delivers. Choose DeliverNone
// only when over-delivery is the more serious fault, such as when the
// Judge enforces confidentiality rather than relevance.
//
// Implementations must be safe for concurrent use and should respect ctx.
type Judge interface {
	Judge(ctx context.Context, msg Message, candidates []Candidate) ([]Decision, error)
}

// JudgeFunc adapts a plain function to Judge.
type JudgeFunc func(ctx context.Context, msg Message, candidates []Candidate) ([]Decision, error)

func (f JudgeFunc) Judge(ctx context.Context, msg Message, candidates []Candidate) ([]Decision, error) {
	return f(ctx, msg, candidates)
}

// Message is the message being routed.
type Message struct {
	Topic string
	Data  json.RawMessage
}

// Candidate is one subscriber being considered for delivery.
type Candidate struct {
	// ID identifies the subscriber across the cluster. It is what a
	// Decision refers back to and what travels in the published payload,
	// so it must be stable for the life of the subscription.
	ID string

	// Criteria is the subscriber's stated interest, opaque to the bus.
	// Where it comes from is the Candidates provider's business: a
	// subscription option, a stored profile, a per-connection filter.
	Criteria string
}

// Decision is a Judge's verdict for one candidate.
type Decision struct {
	// ID must match the Candidate this decides. Decisions naming an
	// unknown ID are ignored; candidates with no Decision are not
	// delivered to. Matching by ID rather than by position means a Judge
	// may reorder, coalesce or omit entries without silently misrouting.
	ID string

	// Deliver is the verdict.
	Deliver bool

	// Score is an optional confidence or relevance figure, carried only
	// for logging and threshold tuning. The bus does not interpret it —
	// applying a threshold is the Judge's job, so that the policy lives in
	// one place.
	Score float64
}

// CandidateSource supplies the subscribers a Judge must consider for a
// topic. It has to see the whole cluster, not one instance's local
// subscribers, or the decision recorded in the message would be wrong for
// every other instance.
type CandidateSource interface {
	Candidates(ctx context.Context, topic string) ([]Candidate, error)
}

// CandidateSourceFunc adapts a plain function to CandidateSource.
type CandidateSourceFunc func(ctx context.Context, topic string) ([]Candidate, error)

func (f CandidateSourceFunc) Candidates(ctx context.Context, topic string) ([]Candidate, error) {
	return f(ctx, topic)
}

// FailurePolicy is what the bus does when a Judge returns an error.
type FailurePolicy int

const (
	// DeliverAll ignores the failure and routes by exact-topic match, as
	// if no Judge were configured. This is the default.
	DeliverAll FailurePolicy = iota

	// DeliverNone drops the message for every subscriber. Appropriate only
	// when over-delivery is worse than non-delivery.
	DeliverNone
)

func (p FailurePolicy) String() string {
	switch p {
	case DeliverNone:
		return "deliver_none"
	default:
		return "deliver_all"
	}
}

// Identified is implemented by subscribers a Judge can address
// individually. A Subscriber that does not implement it is never
// filtered: it receives every message on the topics it subscribes to,
// which keeps existing Subscriber implementations working unchanged.
type Identified interface {
	Subscriber

	// SubscriberID returns the same identifier the CandidateSource uses
	// for this subscriber.
	SubscriberID() string
}

// resolve turns a Judge's decisions into the set of subscriber IDs to
// deliver to, rejecting a response that does not correspond to what was
// asked.
func resolve(candidates []Candidate, decisions []Decision) ([]string, error) {
	known := make(map[string]struct{}, len(candidates))
	for _, c := range candidates {
		known[c.ID] = struct{}{}
	}
	out := make([]string, 0, len(decisions))
	seen := make(map[string]struct{}, len(decisions))
	for _, d := range decisions {
		if _, ok := known[d.ID]; !ok {
			return nil, fmt.Errorf("judge returned a decision for unknown subscriber %q", d.ID)
		}
		if _, dup := seen[d.ID]; dup {
			return nil, fmt.Errorf("judge returned two decisions for subscriber %q", d.ID)
		}
		seen[d.ID] = struct{}{}
		if d.Deliver {
			out = append(out, d.ID)
		}
	}
	return out, nil
}
