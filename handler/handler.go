// Package handler is the extension surface for inbound frames.
//
// The server's built-in verbs — subscribe, publish, locks, jobs — are
// fixed. This package lets an embedder wrap them with middleware and add
// verbs of its own, without forking the dispatch switch.
//
// The types here deliberately do not mention the server's connection
// type. A Handler sees an identity, the decoded frame and a way to write
// frames back, which is everything a verb needs and nothing that ties it
// to how connections are implemented. That is what keeps this package
// importable from outside the module while the connection internals stay
// free to change.
package handler

import (
	"context"

	"github.com/damiensmith1/go-ws-server/protocol"
)

// Request is one inbound frame together with who sent it.
type Request struct {
	// UserKey is the verified identity from auth.Result.
	UserKey string

	// ConnID identifies the socket. It is the same value that appears as
	// connID in the logs and that a bus.Judge addresses a subscriber by,
	// so a handler, its log lines and its routing decisions correlate.
	ConnID string

	// Claims are the credential's claims, or nil.
	Claims map[string]any

	// Envelope is the decoded frame. It has already passed protocol
	// validation and the inbound rate limit.
	Envelope *protocol.Envelope
}

// ReqID is the client's correlation id for this frame, if it sent one.
func (r *Request) ReqID() string {
	if r.Envelope == nil {
		return ""
	}
	return r.Envelope.ReqID
}

// Topic is the frame's topic, if it has one.
func (r *Request) Topic() string {
	if r.Envelope == nil {
		return ""
	}
	return r.Envelope.Topic
}

// Type is the frame's verb.
func (r *Request) Type() string {
	if r.Envelope == nil {
		return ""
	}
	return r.Envelope.Type
}

// Responder writes frames back to the client that sent the request.
//
// Send is subject to the same buffering and drop policy as fan-out, so a
// handler must not rely on delivery. SendNow bypasses the queue for
// replies that must reach the client in order with respect to the
// request, which is what the built-in verbs use for their success and
// error frames.
type Responder interface {
	Send(frame []byte)
	SendNow(ctx context.Context, frame []byte)
}

// Handler runs one verb.
//
// Returning a non-empty reply makes the server send a success frame
// carrying it, tagged with the request's reqID. Returning an error makes
// it send an error frame with the error's message, so an error's text is
// client-facing — do not put internal detail in it. Returning "" and nil
// sends nothing, which is what a handler that has already written its own
// frames through the Responder should do.
type Handler func(ctx context.Context, req *Request, rw Responder) (reply string, err error)

// Middleware wraps a Handler.
//
// Middleware runs for every frame, including the built-in verbs, in the
// order registered: the first registered is the outermost, so it sees the
// request first and the response last. Use it for logging, tracing,
// metrics, validation or transformation.
//
// A middleware that returns without calling next has rejected the frame,
// and its error reaches the client like any other.
type Middleware func(next Handler) Handler

// Chain composes middleware around a Handler. The first element is the
// outermost wrapper.
func Chain(h Handler, mw ...Middleware) Handler {
	// Applied in reverse so that mw[0] ends up outermost, which is the
	// order people expect from a list they wrote top to bottom.
	for i := len(mw) - 1; i >= 0; i-- {
		if mw[i] != nil {
			h = mw[i](h)
		}
	}
	return h
}

// Registry maps verbs to handlers.
//
// It is consulted before the built-in switch, so a registered verb takes
// precedence and a deployment can override subscribe or publish without
// forking. That is deliberate but sharp: overriding a built-in means
// taking on its authorization and locking too.
type Registry struct {
	handlers map[string]Handler
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register adds a handler for a verb, replacing any previous one.
//
// A verb is client-supplied input that reaches a metric label, so the
// server bounds the label set to the verbs it knows. Registering a verb
// here is what adds it to that set; unregistered verbs continue to be
// counted as "unknown" rather than minting a new time series.
func (r *Registry) Register(verb string, h Handler) {
	if r == nil || h == nil || verb == "" {
		return
	}
	r.handlers[verb] = h
}

// Use is Register for several verbs sharing one handler.
func (r *Registry) Use(h Handler, verbs ...string) {
	for _, v := range verbs {
		r.Register(v, h)
	}
}

// Lookup returns the handler for a verb.
func (r *Registry) Lookup(verb string) (Handler, bool) {
	if r == nil {
		return nil, false
	}
	h, ok := r.handlers[verb]
	return h, ok
}

// Verbs returns every registered verb. The server uses it to bound the
// frame-type metric label.
func (r *Registry) Verbs() []string {
	if r == nil {
		return nil
	}
	out := make([]string, 0, len(r.handlers))
	for v := range r.handlers {
		out = append(out, v)
	}
	return out
}
