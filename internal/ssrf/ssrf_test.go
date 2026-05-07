package ssrf

import (
	"context"
	"net"
	"strings"
	"testing"
)

func TestIsPublic(t *testing.T) {
	cases := []struct {
		ip       string
		isPublic bool
	}{
		{"8.8.8.8", true},
		{"1.1.1.1", true},
		{"127.0.0.1", false},
		{"10.0.0.1", false},
		{"172.16.0.1", false},
		{"192.168.1.1", false},
		{"169.254.1.1", false},
		{"100.64.0.1", false},
		{"224.0.0.1", false},
		{"::1", false},
		{"fc00::1", false},
		{"fe80::1", false},
		{"::ffff:127.0.0.1", false},
		{"2606:4700:4700::1111", true},
	}
	for _, c := range cases {
		ip := net.ParseIP(c.ip)
		if got := IsPublic(ip); got != c.isPublic {
			t.Errorf("IsPublic(%s) = %v, want %v", c.ip, got, c.isPublic)
		}
	}
}

func TestValidate_Schemes(t *testing.T) {
	g := &Guard{}
	for _, raw := range []string{
		"file:///etc/passwd",
		"ftp://example.com/",
		"gopher://example.com/",
	} {
		if err := g.Validate(context.Background(), raw); err == nil {
			t.Errorf("expected error for %s", raw)
		}
	}
}

func TestValidate_PrivateLiteralBlocked(t *testing.T) {
	g := &Guard{}
	for _, raw := range []string{
		"http://127.0.0.1/",
		"http://10.0.0.1/",
		"http://[::1]/",
		"http://169.254.169.254/latest/meta-data/", // EC2 metadata
	} {
		err := g.Validate(context.Background(), raw)
		if err == nil {
			t.Errorf("expected block for %s", raw)
		} else if !strings.Contains(err.Error(), "non-public") && !strings.Contains(err.Error(), "blocked") {
			t.Errorf("unexpected error for %s: %v", raw, err)
		}
	}
}

func TestValidate_AllowList(t *testing.T) {
	g := &Guard{AllowedHosts: []string{"example.com"}}
	// Subdomain match
	if err := g.Validate(context.Background(), "https://api.example.com/x"); err != nil {
		// DNS resolution may legitimately fail in test envs without network;
		// what we care about here is the allow-list step. If it's the
		// allow-list rejecting us, that's a bug.
		if strings.Contains(err.Error(), "not in SCHEDULER_ALLOWED_HOSTS") {
			t.Fatalf("subdomain should match: %v", err)
		}
	}
	// Off-list rejection
	err := g.Validate(context.Background(), "https://other.example.org/x")
	if err == nil || !strings.Contains(err.Error(), "not in SCHEDULER_ALLOWED_HOSTS") {
		t.Fatalf("expected allow-list rejection, got %v", err)
	}
}

func TestDialControl_BlocksPrivate(t *testing.T) {
	ctrl := DialControl()
	err := ctrl("tcp", "127.0.0.1:80", nil)
	if err == nil {
		t.Fatal("expected DialControl to block 127.0.0.1")
	}
	err = ctrl("tcp", "8.8.8.8:80", nil)
	if err != nil {
		t.Fatalf("expected DialControl to allow 8.8.8.8, got %v", err)
	}
}
