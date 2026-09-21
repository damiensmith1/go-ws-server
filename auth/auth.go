// Package auth verifies that an inbound WebSocket upgrade request is
// authorized to act as some userKey.
//
// Two built-in verifiers ship:
//
//   - JWTVerifier requires an HS256 JWT (algorithm pinned via WithValidMethods
//     to prevent algorithm-confusion attacks). The token is read from
//     "Authorization: Bearer <jwt>" or "?token=<jwt>". The userKey is taken
//     from the "sub" claim. Optional aud/iss are enforced when configured.
//
//   - InsecureVerifier accepts any client and reads userKey from "?userKey=".
//     This exists for parity with the reference TS server's dev mode and is
//     selected when AUTH_JWT_SECRET is unset. It logs a loud warning at
//     startup and is unsuitable for production.
//
// Custom schemes (mTLS, opaque sessions, header validation) can be plugged
// in by implementing the Verifier interface.
package auth

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Result is what a successful verification returns.
type Result struct {
	UserKey string

	// ExpiresAt is when the credential stops being valid, zero if it never
	// does. Verification happens once, at upgrade; without this the
	// connection layer has no way to know the token behind a long-lived
	// socket has since expired.
	ExpiresAt time.Time

	// Claims are the credential's claims, passed through for an
	// authz.Authorizer to read. Carrying them here is what lets a custom
	// policy use roles, tenants or scopes without re-parsing the token on
	// every frame. Nil when the verifier has no claims to offer.
	Claims map[string]any
}

// Verifier inspects an HTTP upgrade request and either authorizes it or
// returns an error.
type Verifier interface {
	Verify(r *http.Request) (*Result, error)
}

// VerifierFunc is a convenience adapter for stateless verifiers.
type VerifierFunc func(r *http.Request) (*Result, error)

func (f VerifierFunc) Verify(r *http.Request) (*Result, error) { return f(r) }

// ErrUnauthorized is returned for any rejection. The HTTP layer turns this
// into a 401 / WS close 1008. The wrapped reason is logged but not sent to
// the client.
var ErrUnauthorized = errors.New("unauthorized")

// JWTConfig holds the parameters for JWTVerifier.
type JWTConfig struct {
	Secret   string // HS256 shared secret. Required.
	Audience string // Optional `aud` claim to enforce.
	Issuer   string // Optional `iss` claim to enforce.
}

// NewJWT returns a Verifier that requires an HS256 JWT.
func NewJWT(cfg JWTConfig, log *slog.Logger) Verifier {
	if log == nil {
		log = slog.Default()
	}
	secret := []byte(cfg.Secret)
	parserOpts := []jwt.ParserOption{jwt.WithValidMethods([]string{"HS256"})}
	if cfg.Audience != "" {
		parserOpts = append(parserOpts, jwt.WithAudience(cfg.Audience))
	}
	if cfg.Issuer != "" {
		parserOpts = append(parserOpts, jwt.WithIssuer(cfg.Issuer))
	}
	parser := jwt.NewParser(parserOpts...)

	return VerifierFunc(func(r *http.Request) (*Result, error) {
		tok := extractToken(r)
		if tok == "" {
			return nil, ErrUnauthorized
		}
		claims := jwt.MapClaims{}
		if _, err := parser.ParseWithClaims(tok, claims, func(t *jwt.Token) (any, error) {
			return secret, nil
		}); err != nil {
			log.Warn("jwt verification failed", "err", err.Error())
			return nil, ErrUnauthorized
		}
		sub, _ := claims["sub"].(string)
		if sub == "" {
			if uk, ok := claims["userKey"].(string); ok {
				sub = uk
			}
		}
		if sub == "" {
			log.Warn("jwt missing sub/userKey claim")
			return nil, ErrUnauthorized
		}
		// If the client also sent ?userKey=, require it to match the token.
		if uk := r.URL.Query().Get("userKey"); uk != "" && uk != sub {
			log.Warn("userKey query does not match jwt sub", "sub", sub, "userKey", uk)
			return nil, ErrUnauthorized
		}
		res := &Result{UserKey: sub, Claims: map[string]any(claims)}
		// jwt/v5 has already rejected an expired token; what we want here
		// is the deadline so the socket can be closed when it arrives.
		if exp, err := claims.GetExpirationTime(); err == nil && exp != nil {
			res.ExpiresAt = exp.Time
		}
		return res, nil
	})
}

// NewInsecure returns a Verifier that accepts any ?userKey= and logs a
// warning at construction. This exists for local development only.
func NewInsecure(log *slog.Logger) Verifier {
	if log == nil {
		log = slog.Default()
	}
	log.Warn("AUTH_JWT_SECRET is not set — running in INSECURE mode where any client can claim any userKey via the connection query string. Do not use in production.")
	return VerifierFunc(func(r *http.Request) (*Result, error) {
		uk := r.URL.Query().Get("userKey")
		if uk == "" {
			return nil, ErrUnauthorized
		}
		return &Result{UserKey: uk}, nil
	})
}

func extractToken(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		if strings.HasPrefix(strings.ToLower(h), "bearer ") {
			return strings.TrimSpace(h[7:])
		}
	}
	return r.URL.Query().Get("token")
}
