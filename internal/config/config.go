// Package config loads runtime configuration from environment variables.
//
// All values come from env. A `.env` file in the working directory is loaded
// on startup if present, but it does not override variables already set in
// the environment.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

type Config struct {
	RedisHost     string
	RedisPort     int
	RedisPassword string

	// RedisAddrs overrides RedisHost/RedisPort and selects the topology:
	// one address is a plain client, several are a Cluster client. Setting
	// RedisMasterName makes it a Sentinel failover client instead.
	RedisAddrs      []string
	RedisMasterName string

	WebSocketPort    int
	WebSocketTimeout time.Duration
	MaxPayloadBytes  int64
	MaxBufferedBytes int64

	// AllowedOrigins is the Origin allowlist for websocket upgrades. Empty
	// keeps gorilla's same-origin default.
	AllowedOrigins []string

	TLSKeyPath  string
	TLSCertPath string

	AuthJWTSecret   string
	AuthJWTAudience string
	AuthJWTIssuer   string

	StreamMaxLength int64
	StreamTTL       time.Duration

	RateLimitMessagesPerSec int
	RateLimitJobsPerMin     int

	SchedulerAllowedHosts []string

	LogLevel   string
	InstanceID string
}

// Load reads configuration from the environment, applying defaults that
// match the TypeScript reference implementation. Returns an error only on
// values that cannot be safely defaulted (bad numeric inputs, partial TLS
// pair, missing redis host).
func Load() (*Config, error) {
	// Best-effort .env load. Never override an existing env var.
	_ = godotenv.Load()

	cfg := &Config{
		RedisHost:     getenv("REDIS_HOST", ""),
		RedisPassword: os.Getenv("REDIS_PASSWORD"),

		RedisAddrs:      getcsv("REDIS_ADDRS"),
		RedisMasterName: os.Getenv("REDIS_MASTER_NAME"),

		AllowedOrigins: getcsv("ALLOWED_ORIGINS"),

		AuthJWTSecret:   os.Getenv("AUTH_JWT_SECRET"),
		AuthJWTAudience: os.Getenv("AUTH_JWT_AUDIENCE"),
		AuthJWTIssuer:   os.Getenv("AUTH_JWT_ISSUER"),

		TLSKeyPath:  os.Getenv("TLS_KEY_PATH"),
		TLSCertPath: os.Getenv("TLS_CERT_PATH"),

		LogLevel:   getenv("LOG_LEVEL", "info"),
		InstanceID: getenv("INSTANCE_ID", uuid.NewString()),
	}

	var err error
	if cfg.RedisPort, err = getintOr("REDIS_PORT", 6379); err != nil {
		return nil, err
	}
	if cfg.WebSocketPort, err = getintOr("WEBSOCKET_PORT", 8080); err != nil {
		return nil, err
	}
	timeoutMs, err := getint64Or("WEBSOCKET_TIMEOUT", 5*60*1000)
	if err != nil {
		return nil, err
	}
	cfg.WebSocketTimeout = time.Duration(timeoutMs) * time.Millisecond

	if cfg.MaxPayloadBytes, err = getint64Or("MAX_PAYLOAD_BYTES", 64*1024); err != nil {
		return nil, err
	}
	if cfg.MaxBufferedBytes, err = getint64Or("MAX_BUFFERED_BYTES", 1024*1024); err != nil {
		return nil, err
	}
	if cfg.StreamMaxLength, err = getint64Or("STREAM_MAX_LENGTH", 1000); err != nil {
		return nil, err
	}
	streamTTLSec, err := getint64Or("STREAM_TTL_SECONDS", 0)
	if err != nil {
		return nil, err
	}
	cfg.StreamTTL = time.Duration(streamTTLSec) * time.Second

	if cfg.RateLimitMessagesPerSec, err = getintOr("RATE_LIMIT_MESSAGES_PER_SEC", 50); err != nil {
		return nil, err
	}
	if cfg.RateLimitJobsPerMin, err = getintOr("RATE_LIMIT_JOBS_PER_MIN", 30); err != nil {
		return nil, err
	}

	if hosts := strings.TrimSpace(os.Getenv("SCHEDULER_ALLOWED_HOSTS")); hosts != "" {
		for _, h := range strings.Split(hosts, ",") {
			h = strings.ToLower(strings.TrimSpace(h))
			if h != "" {
				cfg.SchedulerAllowedHosts = append(cfg.SchedulerAllowedHosts, h)
			}
		}
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// RedisAddrList returns the addresses to dial. REDIS_ADDRS wins when set;
// otherwise the single REDIS_HOST:REDIS_PORT pair is used, so existing
// single-node deployments need no config change.
func (c *Config) RedisAddrList() []string {
	if len(c.RedisAddrs) > 0 {
		return c.RedisAddrs
	}
	return []string{net.JoinHostPort(c.RedisHost, strconv.Itoa(c.RedisPort))}
}

// TLSEnabled reports whether the server should listen with TLS.
func (c *Config) TLSEnabled() bool {
	return c.TLSKeyPath != "" && c.TLSCertPath != ""
}

func (c *Config) validate() error {
	if c.RedisHost == "" && len(c.RedisAddrs) == 0 {
		return errors.New("REDIS_HOST or REDIS_ADDRS is required")
	}
	// Both TLS paths or neither — partial config is almost certainly a mistake.
	if (c.TLSKeyPath == "") != (c.TLSCertPath == "") {
		return errors.New("TLS_KEY_PATH and TLS_CERT_PATH must both be set, or both be unset")
	}
	if c.WebSocketPort < 1 || c.WebSocketPort > 65535 {
		return fmt.Errorf("WEBSOCKET_PORT out of range: %d", c.WebSocketPort)
	}
	if c.MaxPayloadBytes <= 0 {
		return errors.New("MAX_PAYLOAD_BYTES must be positive")
	}
	if c.MaxBufferedBytes <= 0 {
		return errors.New("MAX_BUFFERED_BYTES must be positive")
	}
	return nil
}

// getcsv splits a comma-separated env var, trimming spaces and dropping
// empty entries. An unset or all-empty value returns nil.
func getcsv(key string) []string {
	var out []string
	for _, part := range strings.Split(os.Getenv(key), ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getintOr(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func getint64Or(key string, fallback int64) (int64, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}
