package handler

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/damiensmith1/go-ws-server/protocol"
)

type nopResponder struct{ sent [][]byte }

func (r *nopResponder) Send(f []byte)                       { r.sent = append(r.sent, f) }
func (r *nopResponder) SendNow(_ context.Context, f []byte) { r.sent = append(r.sent, f) }

// The first middleware in the list must be the outermost, because that is
// the order someone reading a list top to bottom expects.
func TestChainOrder(t *testing.T) {
	var order []string

	mark := func(name string) Middleware {
		return func(next Handler) Handler {
			return func(ctx context.Context, req *Request, rw Responder) (string, error) {
				order = append(order, name+" in")
				reply, err := next(ctx, req, rw)
				order = append(order, name+" out")
				return reply, err
			}
		}
	}

	h := Chain(func(context.Context, *Request, Responder) (string, error) {
		order = append(order, "handler")
		return "ok", nil
	}, mark("first"), mark("second"))

	reply, err := h(context.Background(), &Request{}, &nopResponder{})
	if err != nil || reply != "ok" {
		t.Fatalf("got (%q, %v)", reply, err)
	}

	want := []string{"first in", "second in", "handler", "second out", "first out"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("order = %v, want %v", order, want)
	}
}

func TestChainHandlesNilAndEmpty(t *testing.T) {
	base := func(context.Context, *Request, Responder) (string, error) { return "ok", nil }

	if h := Chain(base); h == nil {
		t.Fatal("Chain with no middleware returned nil")
	}
	// A nil entry must be skipped rather than panic: middleware lists are
	// often built conditionally.
	h := Chain(base, nil, nil)
	if reply, err := h(context.Background(), &Request{}, &nopResponder{}); err != nil || reply != "ok" {
		t.Fatalf("nil middleware broke the chain: (%q, %v)", reply, err)
	}
}

// A middleware that does not call next has rejected the frame.
func TestMiddlewareCanReject(t *testing.T) {
	var reached bool
	h := Chain(func(context.Context, *Request, Responder) (string, error) {
		reached = true
		return "ok", nil
	}, func(Handler) Handler {
		return func(context.Context, *Request, Responder) (string, error) {
			return "", errors.New("rejected by policy")
		}
	})

	if _, err := h(context.Background(), &Request{}, &nopResponder{}); err == nil {
		t.Fatal("want the rejection to surface")
	}
	if reached {
		t.Fatal("the handler ran despite the middleware rejecting the frame")
	}
}

func TestRegistry(t *testing.T) {
	r := NewRegistry()
	h := func(context.Context, *Request, Responder) (string, error) { return "custom", nil }

	t.Run("lookup finds a registered verb", func(t *testing.T) {
		r.Register("myVerb", h)
		if _, ok := r.Lookup("myVerb"); !ok {
			t.Fatal("registered verb not found")
		}
	})

	t.Run("lookup misses an unregistered verb", func(t *testing.T) {
		if _, ok := r.Lookup("nope"); ok {
			t.Fatal("unregistered verb was found")
		}
	})

	// Nil is the zero configuration and must be safe: the server calls
	// Lookup on every frame whether or not a registry was supplied.
	t.Run("a nil registry is usable", func(t *testing.T) {
		var nilReg *Registry
		if _, ok := nilReg.Lookup("anything"); ok {
			t.Fatal("nil registry claimed to have a handler")
		}
		if v := nilReg.Verbs(); v != nil {
			t.Fatalf("nil registry returned verbs %v", v)
		}
		nilReg.Register("x", h) // must not panic
	})

	t.Run("registering nonsense is ignored", func(t *testing.T) {
		r2 := NewRegistry()
		r2.Register("", h)
		r2.Register("v", nil)
		if len(r2.Verbs()) != 0 {
			t.Fatalf("verbs = %v, want none", r2.Verbs())
		}
	})

	t.Run("Use registers several verbs", func(t *testing.T) {
		r2 := NewRegistry()
		r2.Use(h, "a", "b", "c")
		if len(r2.Verbs()) != 3 {
			t.Fatalf("verbs = %v, want 3", r2.Verbs())
		}
	})
}

func TestRequestAccessors(t *testing.T) {
	req := &Request{Envelope: &protocol.Envelope{Type: "publish", Topic: "chat", ReqID: "r1"}}
	if req.Type() != "publish" || req.Topic() != "chat" || req.ReqID() != "r1" {
		t.Fatalf("accessors = %q %q %q", req.Type(), req.Topic(), req.ReqID())
	}

	// A Request with no envelope must not panic; middleware may construct
	// one before the frame is decoded.
	empty := &Request{}
	if empty.Type() != "" || empty.Topic() != "" || empty.ReqID() != "" {
		t.Fatal("accessors on an envelope-less Request should return empty strings")
	}
}
