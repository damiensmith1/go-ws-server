package config

import (
	"os"
	"testing"
)

func TestRedisAddrList(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want []string
	}{
		{
			name: "falls back to host:port",
			cfg:  Config{RedisHost: "localhost", RedisPort: 6379},
			want: []string{"localhost:6379"},
		},
		{
			name: "addrs override host:port",
			cfg:  Config{RedisHost: "ignored", RedisPort: 6379, RedisAddrs: []string{"a:6379", "b:6379"}},
			want: []string{"a:6379", "b:6379"},
		},
		{
			name: "ipv6 host is bracketed",
			cfg:  Config{RedisHost: "::1", RedisPort: 6379},
			want: []string{"[::1]:6379"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.RedisAddrList()
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("got %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestGetcsv(t *testing.T) {
	t.Setenv("TEST_CSV", " a:1 , ,b:2,")
	got := getcsv("TEST_CSV")
	if len(got) != 2 || got[0] != "a:1" || got[1] != "b:2" {
		t.Fatalf("got %v, want [a:1 b:2]", got)
	}
	os.Unsetenv("TEST_CSV")
	if got := getcsv("TEST_CSV"); got != nil {
		t.Fatalf("unset var: got %v, want nil", got)
	}
}

func TestValidate_AcceptsAddrsWithoutHost(t *testing.T) {
	base := func() Config {
		return Config{
			RedisAddrs:       []string{"a:6379"},
			WebSocketPort:    8080,
			MaxPayloadBytes:  1024,
			MaxBufferedBytes: 1024,
		}
	}
	cfg := base()
	if err := cfg.validate(); err != nil {
		t.Fatalf("REDIS_ADDRS alone should validate: %v", err)
	}
	cfg = base()
	cfg.RedisAddrs = nil
	if err := cfg.validate(); err == nil {
		t.Fatal("neither REDIS_HOST nor REDIS_ADDRS should fail validation")
	}
}

func TestGetbool(t *testing.T) {
	tests := []struct {
		value string
		want  bool
	}{
		{"1", true}, {"true", true}, {"TRUE", true}, {"yes", true}, {"on", true},
		{"0", false}, {"false", false}, {"no", false}, {"off", false},
		// A typo must leave the feature off rather than silently on.
		{"ture", false}, {"enabled", false}, {"maybe", false},
	}
	for _, tc := range tests {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("SOME_FLAG", tc.value)
			if got := getbool("SOME_FLAG", false); got != tc.want {
				t.Fatalf("getbool(%q) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}

	t.Run("unset uses the fallback", func(t *testing.T) {
		if getbool("DEFINITELY_UNSET_FLAG", true) != true {
			t.Fatal("unset should return the fallback")
		}
		if getbool("DEFINITELY_UNSET_FLAG", false) != false {
			t.Fatal("unset should return the fallback")
		}
	})
}
