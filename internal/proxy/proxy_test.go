package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sriharifortitude/gatelimit/internal/config"
	"github.com/sriharifortitude/gatelimit/internal/limiter"
)

var t0 = time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)

// A store that fails on demand, to exercise the policies.
type flaky struct {
	inner limiter.Limiter
	down  bool
}

func (f *flaky) Allow(ctx context.Context, key string, l limiter.Limit, now time.Time) (limiter.Decision, error) {
	if f.down {
		return limiter.Decision{}, fmt.Errorf("%w: connection refused", limiter.ErrStore)
	}
	return f.inner.Allow(ctx, key, l, now)
}

func setup(t *testing.T, onStoreError string, withFallback bool) (*httptest.Server, *flaky, *time.Time) {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "upstream saw %s %s xff=%s", r.Method, r.URL.Path, r.Header.Get("X-Forwarded-For"))
	}))
	t.Cleanup(upstream.Close)

	cfg, err := config.Parse(strings.NewReader(fmt.Sprintf(`{
	  "listen": ":0", "upstream": %q, "store": "memory", "on_store_error": %q,
	  "trusted_proxies": ["127.0.0.1/32"],
	  "rules": [
	    {"name": "keyed", "path_prefix": "/api", "key": "api_key", "algorithm": "token_bucket", "rate": 1, "period": "1s", "burst": 2},
	    {"name": "public", "path_prefix": "/public", "key": "ip", "algorithm": "sliding_window", "rate": 3, "period": "1m"}
	  ]}`, upstream.URL, onStoreError)))
	if err != nil {
		t.Fatal(err)
	}
	primary := limiter.NewMemory(time.Minute)
	f := &flaky{inner: primary.TokenBucket()}
	stores := Stores{TokenBucket: f, SlidingWindow: primary.SlidingWindow()}
	if withFallback {
		fb := limiter.NewMemory(time.Minute)
		stores.FallbackTokenBucket = fb.TokenBucket()
		stores.FallbackSlidingWindow = fb.SlidingWindow()
	}
	now := t0
	h := New(cfg, stores, slog.New(slog.NewTextHandler(io.Discard, nil)), func() time.Time { return now })
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, f, &now
}

func get(t *testing.T, srv *httptest.Server, path string, headers ...string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest("GET", srv.URL+path, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	return res, string(body)
}

func TestKeyedRuleAdmitsBurstThenRefuses(t *testing.T) {
	srv, _, now := setup(t, "closed", false)
	for i, wantRemaining := range []string{"1", "0"} {
		res, body := get(t, srv, "/api/x", "X-API-Key", "abc")
		if res.StatusCode != 200 || !strings.Contains(body, "upstream saw GET /api/x") {
			t.Fatalf("request %d: %d %s", i, res.StatusCode, body)
		}
		if res.Header.Get("RateLimit-Limit") != "2" || res.Header.Get("RateLimit-Remaining") != wantRemaining {
			t.Fatalf("request %d headers: %v", i, res.Header)
		}
	}
	res, _ := get(t, srv, "/api/x", "X-API-Key", "abc")
	if res.StatusCode != 429 || res.Header.Get("Retry-After") != "1" || res.Header.Get("RateLimit-Remaining") != "0" {
		t.Fatalf("third: %d %v", res.StatusCode, res.Header)
	}
	// Reset: two tokens short at 1/s = 2 s.
	if res.Header.Get("RateLimit-Reset") != "2" {
		t.Fatalf("reset header: %v", res.Header)
	}
	// Another key is independent.
	if res, _ := get(t, srv, "/api/x", "Authorization", "Bearer other"); res.StatusCode != 200 {
		t.Fatalf("other key: %d", res.StatusCode)
	}
	// Time passes: one token back.
	*now = t0.Add(time.Second)
	if res, _ := get(t, srv, "/api/x", "X-API-Key", "abc"); res.StatusCode != 200 {
		t.Fatalf("after refill: %d", res.StatusCode)
	}
}

func TestKeyedRuleWithoutKeyIs401(t *testing.T) {
	srv, _, _ := setup(t, "closed", false)
	res, _ := get(t, srv, "/api/x")
	if res.StatusCode != 401 {
		t.Fatalf("got %d", res.StatusCode)
	}
}

func TestIPRuleUsesForwardedForBehindTrustedProxy(t *testing.T) {
	srv, _, _ := setup(t, "closed", false)
	// httptest clients arrive from 127.0.0.1, which the config trusts, so X-Forwarded-For is believed.
	for i := 0; i < 3; i++ {
		if res, _ := get(t, srv, "/public/a", "X-Forwarded-For", "198.51.100.1"); res.StatusCode != 200 {
			t.Fatalf("client 1 request %d: %d", i, res.StatusCode)
		}
	}
	if res, _ := get(t, srv, "/public/a", "X-Forwarded-For", "198.51.100.1"); res.StatusCode != 429 {
		t.Fatalf("client 1 fourth: %d", res.StatusCode)
	}
	if res, _ := get(t, srv, "/public/a", "X-Forwarded-For", "198.51.100.2"); res.StatusCode != 200 {
		t.Fatalf("client 2 is separate: %d", res.StatusCode)
	}
}

func TestUnmatchedPathIsProxiedWithoutLimits(t *testing.T) {
	srv, _, _ := setup(t, "closed", false)
	for i := 0; i < 10; i++ {
		res, body := get(t, srv, "/other")
		if res.StatusCode != 200 || res.Header.Get("RateLimit-Limit") != "" || !strings.Contains(body, "xff=127.0.0.1") {
			t.Fatalf("%d: %d %v %s", i, res.StatusCode, res.Header, body)
		}
	}
}

func TestStoreDownFailClosed(t *testing.T) {
	srv, f, _ := setup(t, "closed", true)
	f.down = true
	res, _ := get(t, srv, "/api/x", "X-API-Key", "abc")
	if res.StatusCode != 503 || res.Header.Get("Retry-After") != "1" {
		t.Fatalf("got %d %v", res.StatusCode, res.Header)
	}
}

func TestStoreDownFailOpenUsesFallbackLimits(t *testing.T) {
	srv, f, _ := setup(t, "open", true)
	f.down = true
	codes := []int{}
	for i := 0; i < 3; i++ {
		res, _ := get(t, srv, "/api/x", "X-API-Key", "abc")
		codes = append(codes, res.StatusCode)
	}
	if fmt.Sprint(codes) != "[200 200 429]" {
		t.Fatalf("fallback should still enforce burst 2: %v", codes)
	}
}

func TestStoreDownFailOpenWithoutFallbackIsUnlimited(t *testing.T) {
	srv, f, _ := setup(t, "open", false)
	f.down = true
	for i := 0; i < 5; i++ {
		if res, _ := get(t, srv, "/api/x", "X-API-Key", "abc"); res.StatusCode != 200 {
			t.Fatalf("%d: %d", i, res.StatusCode)
		}
	}
}

func TestHealthAndMetrics(t *testing.T) {
	srv, _, _ := setup(t, "closed", false)
	get(t, srv, "/api/x", "X-API-Key", "abc")
	get(t, srv, "/api/x", "X-API-Key", "abc")
	get(t, srv, "/api/x", "X-API-Key", "abc")
	if res, body := get(t, srv, "/healthz"); res.StatusCode != 200 || body != "ok\n" {
		t.Fatalf("healthz: %d %q", res.StatusCode, body)
	}
	_, body := get(t, srv, "/metrics")
	for _, want := range []string{`gatelimit_decisions_total{outcome="allowed",rule="keyed"} 2`, `gatelimit_decisions_total{outcome="denied",rule="keyed"} 1`, "gatelimit_decision_seconds_bucket"} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}
