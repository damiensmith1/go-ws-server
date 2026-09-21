// Package authz decides whether an already-authenticated client may act on
// a particular topic.
//
// This is the second half of access control and is deliberately separate
// from package auth. auth answers "who is this?" once, at the HTTP
// upgrade. authz answers "may this identity do this, here?" on every
// frame that names a topic. Collapsing the two would mean either
// re-verifying credentials per message or, as before this package existed,
// letting any authenticated client read and write every topic on the
// server.
//
// Three actions are gated:
//
//   - ActionSubscribe — reading a topic, including replay from its stream.
//   - ActionPublish   — writing to a topic.
//   - ActionLock      — taking, renewing or releasing a topic lock, which
//     denies the action to everyone else and so is at least as powerful as
//     the action it locks.
//
// The zero value of the package is permissive: a nil Authorizer allows
// everything, which is the behaviour the server had before and keeps this
// from being a breaking change for existing deployments.
package authz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
)

// Action is the operation being attempted on a topic.
type Action string

const (
	ActionSubscribe Action = "subscribe"
	ActionPublish   Action = "publish"
	ActionLock      Action = "lock"
)

// Request is one authorization question.
type Request struct {
	// UserKey is the verified identity from auth.Result.
	UserKey string

	// Action is what the client is trying to do.
	Action Action

	// Topic is the topic being acted on.
	Topic string

	// Claims are the credential's claims, when the verifier supplied any.
	// An Authorizer that needs roles, tenant IDs or scopes reads them
	// here; it must not re-parse the token itself.
	Claims map[string]any
}

// ErrDenied reports that the request was understood and refused.
//
// It is distinct from an arbitrary error on purpose. A denial is a normal
// outcome the client caused and should be told about plainly. An error
// that is not ErrDenied means the Authorizer could not reach a verdict —
// its policy store was unreachable, say — and must not be reported to the
// client as a denial, because doing so would turn an outage into a silent,
// confusing permission change. The caller fails closed either way, but
// logs and reports the two differently.
var ErrDenied = errors.New("not authorized for topic")

// Authorizer decides whether a request is permitted.
//
// Implementations must return nil to allow, ErrDenied (or an error
// wrapping it) to refuse, and any other error when no verdict could be
// reached. They are called on the hot path of every topic-bearing frame
// and may be called concurrently from many goroutines, so they must be
// safe for concurrent use and should not perform unbounded blocking work.
type Authorizer interface {
	Authorize(ctx context.Context, req Request) error
}

// AuthorizerFunc adapts a plain function to Authorizer.
type AuthorizerFunc func(ctx context.Context, req Request) error

func (f AuthorizerFunc) Authorize(ctx context.Context, req Request) error { return f(ctx, req) }

// AllowAll permits every request. It is the default, and it is what the
// server did before this package existed.
func AllowAll() Authorizer {
	return AuthorizerFunc(func(context.Context, Request) error { return nil })
}

// DenyAll refuses every request. Useful as a base to compose against, and
// as the safe choice in tests.
func DenyAll() Authorizer {
	return AuthorizerFunc(func(_ context.Context, req Request) error {
		return fmt.Errorf("%w: %s on %q", ErrDenied, req.Action, req.Topic)
	})
}

// Rules is a declarative per-action topic allowlist.
//
// Each action maps to a list of glob patterns, matched with path.Match
// semantics over "." separators — so "orders.*" matches "orders.created"
// but not "orders.eu.created", and "orders.**" is not special. An action
// with no patterns denies every topic for that action; this is why the
// zero Rules value denies everything rather than accidentally allowing it.
//
// The token "{userKey}" is replaced by the requesting identity before
// matching, which expresses the common per-tenant case directly:
//
//	Rules{
//	    ActionSubscribe: {"tenant.{userKey}.*", "announcements.*"},
//	    ActionPublish:   {"tenant.{userKey}.*"},
//	    ActionLock:      {"tenant.{userKey}.*"},
//	}
//
// A userKey containing "*" or "?" could otherwise turn its own
// substitution into a wildcard and match other tenants' topics, so
// substituted identities are escaped.
type Rules map[Action][]string

// Authorize implements Authorizer.
func (r Rules) Authorize(_ context.Context, req Request) error {
	for _, pattern := range r[req.Action] {
		if matchTopic(expand(pattern, req.UserKey), req.Topic) {
			return nil
		}
	}
	return fmt.Errorf("%w: %s on %q", ErrDenied, req.Action, req.Topic)
}

// expand substitutes {userKey}, escaping the identity so it cannot smuggle
// glob metacharacters into the pattern.
func expand(pattern, userKey string) string {
	if !strings.Contains(pattern, "{userKey}") {
		return pattern
	}
	return strings.ReplaceAll(pattern, "{userKey}", escapeGlob(userKey))
}

func escapeGlob(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case '*', '?', '[', ']', '\\':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// matchTopic applies path.Match after swapping "." for "/", so that glob
// wildcards stop at topic segment boundaries the way callers expect.
func matchTopic(pattern, topic string) bool {
	if pattern == "*" {
		return true
	}
	ok, err := path.Match(strings.ReplaceAll(pattern, ".", "/"), strings.ReplaceAll(topic, ".", "/"))
	return err == nil && ok
}

// Denied reports whether err is a refusal rather than a failure to decide.
func Denied(err error) bool { return errors.Is(err, ErrDenied) }

// ParseRules reads Rules from the JSON object used by the AUTHZ_RULES
// environment variable, for example:
//
//	{"subscribe":["tenant.{userKey}.*","announcements.*"],
//	 "publish":["tenant.{userKey}.*"],
//	 "lock":["tenant.{userKey}.*"]}
//
// An empty or whitespace-only input returns nil Rules and no error, which
// the caller reads as "no policy configured". An action absent from the
// object denies every topic for that action, so a typo in an action name
// fails closed rather than silently allowing everything; unknown action
// names are rejected outright so the typo is caught at startup instead.
func ParseRules(raw string) (Rules, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var parsed map[string][]string
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, fmt.Errorf("parse authz rules: %w", err)
	}
	rules := make(Rules, len(parsed))
	for action, patterns := range parsed {
		a := Action(action)
		switch a {
		case ActionSubscribe, ActionPublish, ActionLock:
		default:
			return nil, fmt.Errorf("parse authz rules: unknown action %q (want %q, %q or %q)",
				action, ActionSubscribe, ActionPublish, ActionLock)
		}
		for _, p := range patterns {
			if _, err := path.Match(strings.ReplaceAll(expand(p, "x"), ".", "/"), ""); err != nil {
				return nil, fmt.Errorf("parse authz rules: bad pattern %q for %s: %w", p, action, err)
			}
		}
		rules[a] = patterns
	}
	return rules, nil
}
