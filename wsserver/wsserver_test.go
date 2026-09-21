package wsserver

import (
	"testing"
	"time"

	"github.com/damiensmith1/go-ws-server/internal/config"
)

func TestOptionsToConfigDefaults(t *testing.T) {
	cfg, err := Options{}.toConfig()
	if err != nil {
		t.Fatalf("toConfig: %v", err)
	}

	// The zero Options must be usable, which means every "0 means unset"
	// field resolves to the same default the command uses. A zero that
	// leaked through would produce, say, a 0-byte payload limit that
	// rejects every frame.
	checks := []struct {
		name string
		got  any
		want any
	}{
		{"MaxPayloadBytes", cfg.MaxPayloadBytes, int64(64 * 1024)},
		{"MaxBufferedBytes", cfg.MaxBufferedBytes, int64(1024 * 1024)},
		{"StreamMaxLength", cfg.StreamMaxLength, int64(1000)},
		{"RateLimitMessagesPerSec", cfg.RateLimitMessagesPerSec, 50},
		{"RateLimitJobsPerMin", cfg.RateLimitJobsPerMin, 30},
		{"MaxConsecutiveDrops", cfg.MaxConsecutiveDrops, 100},
		{"ReadinessTimeout", cfg.ReadinessTimeout, config.DefaultReadinessTimeout},
		{"ShutdownTimeout", cfg.ShutdownTimeout, config.DefaultShutdownTimeout},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}

	if len(cfg.RedisAddrs) != 1 || cfg.RedisAddrs[0] != "localhost:6379" {
		t.Errorf("RedisAddrs = %v, want [localhost:6379]", cfg.RedisAddrs)
	}
	if cfg.WebSocketPort != 0 {
		t.Errorf("WebSocketPort = %d, want 0 so the OS picks a free port", cfg.WebSocketPort)
	}
	if cfg.InstanceID == "" {
		t.Error("InstanceID must be generated when not supplied")
	}
}

func TestOptionsToConfigPassesValuesThrough(t *testing.T) {
	opts := Options{
		ListenAddr:       "0.0.0.0:9999",
		MaxPayloadBytes:  1234,
		MessagesPerSec:   7,
		ShutdownTimeout:  3 * time.Second,
		ReadinessTimeout: 500 * time.Millisecond,
		InstanceID:       "fixed-id",
		RedisAddrs:       []string{"a:6379", "b:6379"},
	}
	cfg, err := opts.toConfig()
	if err != nil {
		t.Fatalf("toConfig: %v", err)
	}
	if cfg.WebSocketPort != 9999 {
		t.Errorf("WebSocketPort = %d, want 9999", cfg.WebSocketPort)
	}
	if cfg.MaxPayloadBytes != 1234 || cfg.RateLimitMessagesPerSec != 7 {
		t.Errorf("explicit limits were overwritten by defaults: %+v", cfg)
	}
	if cfg.ShutdownTimeout != 3*time.Second || cfg.ReadinessTimeout != 500*time.Millisecond {
		t.Errorf("explicit timeouts were overwritten by defaults: %+v", cfg)
	}
	if cfg.InstanceID != "fixed-id" {
		t.Errorf("InstanceID = %q, want the supplied value", cfg.InstanceID)
	}
	if len(cfg.RedisAddrs) != 2 {
		t.Errorf("RedisAddrs = %v, want both addresses preserved", cfg.RedisAddrs)
	}
}

// Zero has to mean "use the default", so there must be a separate way to
// ask for no eviction at all. Negative is that way.
func TestMaxConsecutiveDropsNegativeDisablesEviction(t *testing.T) {
	cfg, err := Options{MaxConsecutiveDrops: -1}.toConfig()
	if err != nil {
		t.Fatalf("toConfig: %v", err)
	}
	if cfg.MaxConsecutiveDrops != 0 {
		t.Fatalf("MaxConsecutiveDrops = %d, want 0 (disabled)", cfg.MaxConsecutiveDrops)
	}
}

func TestOptionsRejectsBadListenAddr(t *testing.T) {
	for _, addr := range []string{"not-an-address", ":not-a-port", "1.2.3.4"} {
		if _, err := (Options{ListenAddr: addr}).toConfig(); err == nil {
			t.Errorf("ListenAddr %q was accepted; a typo here should fail at New, not at listen time", addr)
		}
	}
}

// The environment and Options spell "no eviction" differently: 0 in the
// environment, negative in Options, because Options reserves 0 for
// "unset". Without translation, MAX_CONSECUTIVE_DROPS=0 would silently
// re-enable eviction at the default.
func TestFromEnvPreservesDisabledEviction(t *testing.T) {
	t.Setenv("REDIS_HOST", "localhost")
	t.Setenv("MAX_CONSECUTIVE_DROPS", "0")

	opts, err := OptionsFromEnv()
	if err != nil {
		t.Fatalf("OptionsFromEnv: %v", err)
	}
	cfg, err := opts.toConfig()
	if err != nil {
		t.Fatalf("toConfig: %v", err)
	}
	if cfg.MaxConsecutiveDrops != 0 {
		t.Fatalf("MaxConsecutiveDrops = %d, want 0; the environment asked for eviction to be off", cfg.MaxConsecutiveDrops)
	}
}

func TestFromEnvRoundTripsAnExplicitBudget(t *testing.T) {
	t.Setenv("REDIS_HOST", "localhost")
	t.Setenv("MAX_CONSECUTIVE_DROPS", "25")

	opts, err := OptionsFromEnv()
	if err != nil {
		t.Fatalf("OptionsFromEnv: %v", err)
	}
	cfg, err := opts.toConfig()
	if err != nil {
		t.Fatalf("toConfig: %v", err)
	}
	if cfg.MaxConsecutiveDrops != 25 {
		t.Fatalf("MaxConsecutiveDrops = %d, want 25", cfg.MaxConsecutiveDrops)
	}
}
