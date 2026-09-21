// Package ssrf implements outbound-URL guards for the scheduler's HTTP
// callouts. Two layers cooperate:
//
//   - Validate(ctx, rawURL) does a pre-flight check: the URL must be
//     http(s), match the optional allow-list, and resolve only to public
//     IPs. This catches obvious mistakes early and lets the server return a
//     clear error.
//
//   - Dialer returns a net.Dialer.Control hook for the HTTP transport.
//     At actual connect time it inspects the resolved IP being dialed and
//     refuses to connect to private/loopback/link-local/CGNAT/multicast/etc.
//     ranges. This is the airtight defense: it survives DNS rebinding
//     (where the second resolve returns a private IP), HTTP redirects, and
//     race conditions between the pre-flight resolve and the actual connect.
//
// Use BOTH. The pre-flight check by itself is exploitable.
package ssrf

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"syscall"
)

// ErrBlocked is returned for any URL or address rejected by the guard.
var ErrBlocked = errors.New("blocked by SSRF policy")

// Guard holds policy: an optional case-insensitive allow-list of hostnames
// (each matched as exact host or subdomain).
type Guard struct {
	AllowedHosts []string
}

// Validate runs the pre-flight check on rawURL. It does not connect.
//
// Rules:
//   - scheme must be http or https
//   - if AllowedHosts is non-empty, the URL host must equal one of them or
//     be a subdomain (e.g. "api.example.com" matches "example.com")
//   - every address that the hostname resolves to must be a public IP. A
//     hostname that resolves to a single private IP is blocked even if
//     others are public — fail-closed.
func (g *Guard) Validate(ctx context.Context, rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("%w: parse url: %v", ErrBlocked, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%w: only http(s) urls are allowed", ErrBlocked)
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return fmt.Errorf("%w: missing host", ErrBlocked)
	}
	if !g.hostAllowed(host) {
		return fmt.Errorf("%w: host %q not in SCHEDULER_ALLOWED_HOSTS", ErrBlocked, host)
	}

	var ips []net.IP
	if literal := net.ParseIP(host); literal != nil {
		ips = []net.IP{literal}
	} else {
		resolved, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return fmt.Errorf("%w: dns lookup: %v", ErrBlocked, err)
		}
		for _, a := range resolved {
			ips = append(ips, a.IP)
		}
	}
	if len(ips) == 0 {
		return fmt.Errorf("%w: no addresses for %q", ErrBlocked, host)
	}
	for _, ip := range ips {
		if !IsPublic(ip) {
			return fmt.Errorf("%w: %q resolves to non-public address %s", ErrBlocked, host, ip)
		}
	}
	return nil
}

// hostAllowed returns true if host matches the allow-list. An empty
// allow-list means "any public host" (the IP check still runs).
func (g *Guard) hostAllowed(host string) bool {
	if len(g.AllowedHosts) == 0 {
		return true
	}
	for _, h := range g.AllowedHosts {
		if host == h || strings.HasSuffix(host, "."+h) {
			return true
		}
	}
	return false
}

// DialControl returns a function suitable for net.Dialer.Control. It
// inspects the resolved address Go is about to connect to and returns an
// error if it points at a non-public range. This runs after Go's DNS
// resolution but before the TCP connect, so it sees the IP that's actually
// going on the wire.
func DialControl() func(network, address string, c syscall.RawConn) error {
	return func(network, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return fmt.Errorf("%w: split host:port: %v", ErrBlocked, err)
		}
		ip := net.ParseIP(host)
		if ip == nil {
			return fmt.Errorf("%w: dialer received non-IP host %q", ErrBlocked, host)
		}
		if !IsPublic(ip) {
			return fmt.Errorf("%w: dial target %s is non-public", ErrBlocked, ip)
		}
		return nil
	}
}

var blockedV4 = parseCIDRs([]string{
	"0.0.0.0/8",       // "this network"
	"10.0.0.0/8",      // RFC1918
	"100.64.0.0/10",   // CGNAT
	"127.0.0.0/8",     // loopback
	"169.254.0.0/16",  // link-local
	"172.16.0.0/12",   // RFC1918
	"192.0.0.0/24",    // IETF protocol assignments
	"192.0.2.0/24",    // TEST-NET-1
	"192.168.0.0/16",  // RFC1918
	"198.18.0.0/15",   // benchmarking
	"198.51.100.0/24", // TEST-NET-2
	"203.0.113.0/24",  // TEST-NET-3
	"224.0.0.0/4",     // multicast
	"240.0.0.0/4",     // reserved
	"255.255.255.255/32",
})

var blockedV6 = parseCIDRs([]string{
	"::/128",       // unspecified
	"::1/128",      // loopback
	"64:ff9b::/96", // NAT64
	"100::/64",     // discard
	"fc00::/7",     // unique local
	"fe80::/10",    // link-local
	"ff00::/8",     // multicast
})

func parseCIDRs(cidrs []string) []*net.IPNet {
	out := make([]*net.IPNet, 0, len(cidrs))
	for _, c := range cidrs {
		if _, n, err := net.ParseCIDR(c); err == nil {
			out = append(out, n)
		}
	}
	return out
}

// IsPublic returns true if ip is in routable, non-private, non-loopback
// space. IPv4-mapped IPv6 addresses (::ffff:1.2.3.4) are normalized and
// checked against the IPv4 list.
func IsPublic(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if v4 := ip.To4(); v4 != nil {
		for _, n := range blockedV4 {
			if n.Contains(v4) {
				return false
			}
		}
		return true
	}
	for _, n := range blockedV6 {
		if n.Contains(ip) {
			return false
		}
	}
	return true
}
