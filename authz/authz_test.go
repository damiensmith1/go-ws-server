package authz

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestRulesAuthorize(t *testing.T) {
	rules := Rules{
		ActionSubscribe: {"tenant.{userKey}.*", "announcements.*"},
		ActionPublish:   {"tenant.{userKey}.*"},
	}

	tests := []struct {
		name    string
		userKey string
		action  Action
		topic   string
		allow   bool
	}{
		{"own tenant subscribe", "acme", ActionSubscribe, "tenant.acme.orders", true},
		{"own tenant publish", "acme", ActionPublish, "tenant.acme.orders", true},
		{"shared topic subscribe", "acme", ActionSubscribe, "announcements.global", true},

		{"another tenant is denied", "acme", ActionSubscribe, "tenant.globex.orders", false},
		{"shared topic is read-only", "acme", ActionPublish, "announcements.global", false},

		// Actions absent from Rules deny everything: a policy that forgot
		// to mention locking must not leave locking wide open.
		{"unlisted action denies", "acme", ActionLock, "tenant.acme.orders", false},

		// path.Match stops at separators, so a single star is one segment.
		{"wildcard does not cross segments", "acme", ActionSubscribe, "tenant.acme.orders.eu", false},
		{"prefix is not enough", "acme", ActionSubscribe, "tenant.acmecorp.orders", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := rules.Authorize(context.Background(), Request{
				UserKey: tc.userKey, Action: tc.action, Topic: tc.topic,
			})
			if tc.allow && err != nil {
				t.Fatalf("want allowed, got %v", err)
			}
			if !tc.allow {
				if err == nil {
					t.Fatal("want denied, got allowed")
				}
				if !Denied(err) {
					t.Fatalf("denial must satisfy Denied(); got %v", err)
				}
			}
		})
	}
}

// A userKey is attacker-controlled in the insecure verifier and only as
// trustworthy as the issuer elsewhere. If substitution let it carry glob
// metacharacters, "*" as a userKey would match every tenant's topics.
func TestRulesEscapesUserKeyWildcards(t *testing.T) {
	rules := Rules{ActionSubscribe: {"tenant.{userKey}.*"}}

	for _, userKey := range []string{"*", "?", "[a-z]*"} {
		t.Run(userKey, func(t *testing.T) {
			err := rules.Authorize(context.Background(), Request{
				UserKey: userKey, Action: ActionSubscribe, Topic: "tenant.victim.orders",
			})
			if err == nil {
				t.Fatalf("userKey %q escaped its namespace and matched another tenant", userKey)
			}
		})
	}

	// The escaping must not break the ordinary case it protects.
	if err := rules.Authorize(context.Background(), Request{
		UserKey: "*", Action: ActionSubscribe, Topic: "tenant.*.orders",
	}); err != nil {
		t.Fatalf("a literal userKey should still match its own literal topic: %v", err)
	}
}

func TestZeroRulesDenyEverything(t *testing.T) {
	var rules Rules
	for _, a := range []Action{ActionSubscribe, ActionPublish, ActionLock} {
		if err := rules.Authorize(context.Background(), Request{UserKey: "u", Action: a, Topic: "t"}); err == nil {
			t.Fatalf("zero Rules allowed %s; it must fail closed", a)
		}
	}
}

func TestAllowAllAndDenyAll(t *testing.T) {
	if err := AllowAll().Authorize(context.Background(), Request{}); err != nil {
		t.Fatalf("AllowAll denied: %v", err)
	}
	err := DenyAll().Authorize(context.Background(), Request{Action: ActionPublish, Topic: "t"})
	if !Denied(err) {
		t.Fatalf("DenyAll must return a denial, got %v", err)
	}
}

// Denied must distinguish a refusal from a failure to reach a verdict;
// the caller reports the two differently.
func TestDeniedRejectsUnrelatedErrors(t *testing.T) {
	if Denied(errors.New("policy store unreachable")) {
		t.Fatal("an infrastructure error was misread as a denial")
	}
	if !Denied(errors.Join(errors.New("context"), ErrDenied)) {
		t.Fatal("a wrapped ErrDenied must still read as a denial")
	}
}

func TestParseRules(t *testing.T) {
	t.Run("empty means no policy", func(t *testing.T) {
		rules, err := ParseRules("   ")
		if err != nil || rules != nil {
			t.Fatalf("got (%v, %v), want (nil, nil)", rules, err)
		}
	})

	t.Run("parses actions and patterns", func(t *testing.T) {
		rules, err := ParseRules(`{"subscribe":["a.*"],"publish":["b.*"],"lock":["c.*"]}`)
		if err != nil {
			t.Fatalf("ParseRules: %v", err)
		}
		if got := len(rules); got != 3 {
			t.Fatalf("got %d actions, want 3", got)
		}
		if err := rules.Authorize(context.Background(), Request{UserKey: "u", Action: ActionPublish, Topic: "b.x"}); err != nil {
			t.Fatalf("parsed rule did not apply: %v", err)
		}
	})

	// A misspelled action would otherwise be silently dropped, and since a
	// missing action denies everything, the operator would see a total
	// outage for that verb with no clue why. Fail at startup instead.
	t.Run("unknown action is rejected", func(t *testing.T) {
		if _, err := ParseRules(`{"subscribed":["a.*"]}`); err == nil {
			t.Fatal("want an error naming the bad action")
		}
	})

	t.Run("malformed json is rejected", func(t *testing.T) {
		if _, err := ParseRules(`{"subscribe":`); err == nil {
			t.Fatal("want a parse error")
		}
	})

	// An unterminated character class makes path.Match return
	// ErrBadPattern, which means it never matches anything. Caught at
	// startup, that is a config error; caught at runtime it is a silent
	// deny-all for that action.
	t.Run("bad glob pattern is rejected", func(t *testing.T) {
		_, err := ParseRules(`{"subscribe":["a.[abc"]}`)
		if err == nil {
			t.Fatal("want an error naming the bad pattern")
		}
		if !strings.Contains(err.Error(), "a.[abc") {
			t.Fatalf("error should name the offending pattern, got %v", err)
		}
	})
}
