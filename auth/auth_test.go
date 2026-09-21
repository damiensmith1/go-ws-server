package auth

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func sign(t *testing.T, secret string, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	s, err := tok.SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestJWTVerifier_Valid(t *testing.T) {
	v := NewJWT(JWTConfig{Secret: "shh"}, quietLogger())
	tok := sign(t, "shh", jwt.MapClaims{
		"sub": "alice",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	r := httptest.NewRequest("GET", "/?token="+tok, nil)
	res, err := v.Verify(r)
	if err != nil {
		t.Fatalf("expected ok, got %v", err)
	}
	if res.UserKey != "alice" {
		t.Fatalf("got userKey=%q, want alice", res.UserKey)
	}
}

func TestJWTVerifier_BearerHeader(t *testing.T) {
	v := NewJWT(JWTConfig{Secret: "shh"}, quietLogger())
	tok := sign(t, "shh", jwt.MapClaims{"sub": "alice", "exp": time.Now().Add(time.Hour).Unix()})
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer "+tok)
	res, err := v.Verify(r)
	if err != nil || res.UserKey != "alice" {
		t.Fatalf("bearer header failed: res=%v err=%v", res, err)
	}
}

func TestJWTVerifier_MissingToken(t *testing.T) {
	v := NewJWT(JWTConfig{Secret: "shh"}, quietLogger())
	r := httptest.NewRequest("GET", "/", nil)
	if _, err := v.Verify(r); err == nil {
		t.Fatal("expected error on missing token")
	}
}

func TestJWTVerifier_BadSignature(t *testing.T) {
	v := NewJWT(JWTConfig{Secret: "shh"}, quietLogger())
	tok := sign(t, "wrong-secret", jwt.MapClaims{"sub": "alice", "exp": time.Now().Add(time.Hour).Unix()})
	r := httptest.NewRequest("GET", "/?token="+tok, nil)
	if _, err := v.Verify(r); err == nil {
		t.Fatal("expected error on bad signature")
	}
}

func TestJWTVerifier_AudIssMatch(t *testing.T) {
	v := NewJWT(JWTConfig{Secret: "shh", Audience: "ws", Issuer: "id-svc"}, quietLogger())
	tok := sign(t, "shh", jwt.MapClaims{
		"sub": "alice", "aud": "ws", "iss": "id-svc",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	r := httptest.NewRequest("GET", "/?token="+tok, nil)
	if _, err := v.Verify(r); err != nil {
		t.Fatalf("expected ok, got %v", err)
	}

	// wrong audience → reject
	tok2 := sign(t, "shh", jwt.MapClaims{
		"sub": "alice", "aud": "different", "iss": "id-svc",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	r2 := httptest.NewRequest("GET", "/?token="+tok2, nil)
	if _, err := v.Verify(r2); err == nil {
		t.Fatal("expected error on aud mismatch")
	}
}

func TestJWTVerifier_UserKeyMismatch(t *testing.T) {
	v := NewJWT(JWTConfig{Secret: "shh"}, quietLogger())
	tok := sign(t, "shh", jwt.MapClaims{"sub": "alice", "exp": time.Now().Add(time.Hour).Unix()})
	r := httptest.NewRequest("GET", "/?token="+tok+"&userKey=bob", nil)
	if _, err := v.Verify(r); err == nil {
		t.Fatal("expected error on userKey ≠ sub")
	}
}

func TestJWTVerifier_AlgConfusionRefused(t *testing.T) {
	// Try to slip in an unsigned `none` token. Parser pinned to HS256 must reject.
	v := NewJWT(JWTConfig{Secret: "shh"}, quietLogger())
	tok := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{"sub": "alice"})
	signed, err := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "/?token="+signed, nil)
	if _, err := v.Verify(r); err == nil {
		t.Fatal("expected error on alg=none token")
	}
}

func TestInsecureVerifier(t *testing.T) {
	v := NewInsecure(quietLogger())
	r := httptest.NewRequest("GET", "/?userKey=alice", nil)
	res, err := v.Verify(r)
	if err != nil || res.UserKey != "alice" {
		t.Fatalf("expected alice, got res=%v err=%v", res, err)
	}

	r2 := httptest.NewRequest("GET", "/", nil)
	if _, err := v.Verify(r2); err == nil {
		t.Fatal("expected error on missing userKey")
	}
}

func TestJWTCarriesExpiry(t *testing.T) {
	const secret = "test-secret"
	v := NewJWT(JWTConfig{Secret: secret}, quietLogger())

	t.Run("exp is surfaced on Result", func(t *testing.T) {
		exp := time.Now().Add(42 * time.Minute).Truncate(time.Second)
		req := httptest.NewRequest("GET", "/ws", nil)
		req.Header.Set("Authorization", "Bearer "+sign(t, secret, jwt.MapClaims{"sub": "u1", "exp": exp.Unix()}))

		res, err := v.Verify(req)
		if err != nil {
			t.Fatalf("Verify: %v", err)
		}
		if !res.ExpiresAt.Equal(exp) {
			t.Fatalf("ExpiresAt = %v, want %v", res.ExpiresAt, exp)
		}
	})

	t.Run("a token without exp reports no deadline", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/ws", nil)
		req.Header.Set("Authorization", "Bearer "+sign(t, secret, jwt.MapClaims{"sub": "u1"}))

		res, err := v.Verify(req)
		if err != nil {
			t.Fatalf("Verify: %v", err)
		}
		if !res.ExpiresAt.IsZero() {
			t.Fatalf("ExpiresAt = %v, want zero so no watcher is started", res.ExpiresAt)
		}
	})
}
