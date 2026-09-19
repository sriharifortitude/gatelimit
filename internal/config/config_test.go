package config

import (
	"strings"
	"testing"
	"time"
)

const valid = `{
  "listen": ":8080",
  "upstream": "http://127.0.0.1:4200",
  "store": "redis",
  "redis_addr_env": "REDIS_ADDR",
  "on_store_error": "open",
  "trusted_proxies": ["10.0.0.0/8", "127.0.0.1/32"],
  "rules": [
    {"name": "ingest", "path_prefix": "/api/events", "key": "api_key", "algorithm": "token_bucket", "rate": 100, "period": "1s", "burst": 500},
    {"name": "query", "path_prefix": "/api", "key": "api_key", "algorithm": "sliding_window", "rate": 600, "period": "1m"},
    {"name": "public", "path_prefix": "/", "key": "ip", "algorithm": "token_bucket", "rate": 10, "period": "1s"}
  ]
}`

func TestParseValid(t *testing.T) {
	c, err := Parse(strings.NewReader(valid))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Trusted()) != 2 || c.Trusted()[0].String() != "10.0.0.0/8" {
		t.Fatalf("trusted: %v", c.Trusted())
	}
	if c.Rules[0].PeriodDuration() != time.Second || c.Rules[1].PeriodDuration() != time.Minute {
		t.Fatalf("periods: %v %v", c.Rules[0].PeriodDuration(), c.Rules[1].PeriodDuration())
	}
	if c.Rules[2].Burst != 10 {
		t.Fatalf("burst should default to rate, got %d", c.Rules[2].Burst)
	}
}

func TestEveryErrorNamesItsField(t *testing.T) {
	cases := map[string]string{
		`{"listen": ":1", "upstream": "http://x", "store": "memory", "on_store_error": "open", "rules": [], "extra": 1}`:                                                                                                                                                                             `unknown field "extra"`,
		`{"listen": ":1", "upstream": "x", "store": "memory", "on_store_error": "open", "rules": [{"name":"a","path_prefix":"/","key":"ip","algorithm":"token_bucket","rate":1,"period":"1s"}]}`:                                                                                                     "upstream: must be an absolute http(s) URL",
		`{"listen": ":1", "upstream": "http://x", "store": "redis", "on_store_error": "open", "rules": [{"name":"a","path_prefix":"/","key":"ip","algorithm":"token_bucket","rate":1,"period":"1s"}]}`:                                                                                               `redis_addr_env: required with store "redis"`,
		`{"listen": ":1", "upstream": "http://x", "store": "memory", "on_store_error": "maybe", "rules": [{"name":"a","path_prefix":"/","key":"ip","algorithm":"token_bucket","rate":1,"period":"1s"}]}`:                                                                                             `on_store_error: must be "open" or "closed"`,
		`{"listen": ":1", "upstream": "http://x", "store": "memory", "on_store_error": "open", "trusted_proxies": ["10.0.0.1"], "rules": [{"name":"a","path_prefix":"/","key":"ip","algorithm":"token_bucket","rate":1,"period":"1s"}]}`:                                                             `trusted_proxies: "10.0.0.1" is not a CIDR`,
		`{"listen": ":1", "upstream": "http://x", "store": "memory", "on_store_error": "open", "rules": []}`:                                                                                                                                                                                         "rules: at least one rule is required",
		`{"listen": ":1", "upstream": "http://x", "store": "memory", "on_store_error": "open", "rules": [{"name":"a","path_prefix":"api","key":"ip","algorithm":"token_bucket","rate":1,"period":"1s"}]}`:                                                                                            "rules[0].path_prefix: must start with /",
		`{"listen": ":1", "upstream": "http://x", "store": "memory", "on_store_error": "open", "rules": [{"name":"a","path_prefix":"/","key":"user","algorithm":"token_bucket","rate":1,"period":"1s"}]}`:                                                                                            `rules[0].key: must be "api_key" or "ip"`,
		`{"listen": ":1", "upstream": "http://x", "store": "memory", "on_store_error": "open", "rules": [{"name":"a","path_prefix":"/","key":"ip","algorithm":"leaky","rate":1,"period":"1s"}]}`:                                                                                                     `rules[0].algorithm: must be "token_bucket" or "sliding_window"`,
		`{"listen": ":1", "upstream": "http://x", "store": "memory", "on_store_error": "open", "rules": [{"name":"a","path_prefix":"/","key":"ip","algorithm":"token_bucket","rate":0,"period":"1s"}]}`:                                                                                              "rules[0].rate: must be positive",
		`{"listen": ":1", "upstream": "http://x", "store": "memory", "on_store_error": "open", "rules": [{"name":"a","path_prefix":"/","key":"ip","algorithm":"token_bucket","rate":1,"period":"soon"}]}`:                                                                                            "rules[0].period: must be a positive duration",
		`{"listen": ":1", "upstream": "http://x", "store": "memory", "on_store_error": "open", "rules": [{"name":"a","path_prefix":"/","key":"ip","algorithm":"sliding_window","rate":5,"period":"1s","burst":9}]}`:                                                                                  "rules[0].burst: not used by sliding_window",
		`{"listen": ":1", "upstream": "http://x", "store": "memory", "on_store_error": "open", "rules": [{"name":"a","path_prefix":"/","key":"ip","algorithm":"token_bucket","rate":1,"period":"1s"},{"name":"a","path_prefix":"/x","key":"ip","algorithm":"token_bucket","rate":1,"period":"1s"}]}`: `rules[1].name: "a" is used twice`,
	}
	for input, want := range cases {
		_, err := Parse(strings.NewReader(input))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("want error containing %q, got %v", want, err)
		}
	}
}

func TestSeveralErrorsAreReportedTogether(t *testing.T) {
	_, err := Parse(strings.NewReader(`{"listen": "", "upstream": "", "store": "x", "on_store_error": "x", "rules": []}`))
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{"listen: required", "upstream:", "store:", "on_store_error:", "rules:"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in %v", want, err)
		}
	}
}
