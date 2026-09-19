// Package proxy is the HTTP front: match a rule, derive the key, ask the
// limiter, set the headers, forward or refuse.
package proxy

import (
	"context"
	"log/slog"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sriharifortitude/gatelimit/internal/config"
	"github.com/sriharifortitude/gatelimit/internal/keys"
	"github.com/sriharifortitude/gatelimit/internal/limiter"
)

// Stores provides a limiter per algorithm for the primary store and, when
// the primary is remote, for the in-process fallback.
type Stores struct {
	TokenBucket   limiter.Limiter
	SlidingWindow limiter.Limiter
	// Fallback is used when the primary returns a store error and the
	// policy is fail-open. Nil means fail-open lets requests through unlimited.
	FallbackTokenBucket   limiter.Limiter
	FallbackSlidingWindow limiter.Limiter
}

type Handler struct {
	cfg     *config.Config
	stores  Stores
	proxy   *httputil.ReverseProxy
	now     func() time.Time
	log     *slog.Logger
	metrics *metrics
	mux     *http.ServeMux
}

type metrics struct {
	decisions   *prometheus.CounterVec
	storeErrors *prometheus.CounterVec
	latency     prometheus.Histogram
}

// New builds the handler. `now` is injectable for tests.
func New(cfg *config.Config, stores Stores, log *slog.Logger, now func() time.Time) *Handler {
	target, _ := url.Parse(cfg.Upstream) // validated by config
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.SetXForwarded()
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log.Warn("upstream error", "path", r.URL.Path, "err", err)
			http.Error(w, "upstream unavailable", http.StatusBadGateway)
		},
	}
	m := &metrics{
		decisions:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gatelimit_decisions_total", Help: "Requests by rule and outcome."}, []string{"rule", "outcome"}),
		storeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gatelimit_store_errors_total", Help: "Limiter store failures by policy applied."}, []string{"policy"}),
		latency:     prometheus.NewHistogram(prometheus.HistogramOpts{Name: "gatelimit_decision_seconds", Help: "Time to reach a limit decision.", Buckets: prometheus.ExponentialBuckets(0.0001, 4, 8)}),
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(m.decisions, m.storeErrors, m.latency)

	h := &Handler{cfg: cfg, stores: stores, proxy: rp, now: now, log: log, metrics: m, mux: http.NewServeMux()}
	h.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	h.mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	h.mux.HandleFunc("/", h.limited)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

func (h *Handler) limited(w http.ResponseWriter, r *http.Request) {
	rule := h.match(r.URL.Path)
	if rule == nil {
		h.proxy.ServeHTTP(w, r)
		return
	}
	key := h.key(rule, r)
	if key == "" {
		// A rule keyed on API key, and no key was presented. The upstream
		// decides whether anonymous access exists; we only refuse to pool
		// every anonymous client into one bucket.
		http.Error(w, "missing API key", http.StatusUnauthorized)
		h.metrics.decisions.WithLabelValues(rule.Name, "no_key").Inc()
		return
	}
	limit := limiter.Limit{Rate: rule.Rate, Period: rule.PeriodDuration(), Burst: rule.Burst}
	ctx, cancel := context.WithTimeout(r.Context(), 250*time.Millisecond)
	defer cancel()

	started := time.Now()
	d, err := h.primary(rule).Allow(ctx, rule.Name+":"+key, limit, h.now())
	h.metrics.latency.Observe(time.Since(started).Seconds())

	if err != nil {
		if !limiter.IsStoreError(err) {
			h.log.Error("limiter", "rule", rule.Name, "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		h.metrics.storeErrors.WithLabelValues(h.cfg.OnStoreError).Inc()
		if h.cfg.OnStoreError == "closed" {
			h.log.Warn("store unavailable, failing closed", "rule", rule.Name, "err", err)
			w.Header().Set("Retry-After", "1")
			http.Error(w, "rate limiter unavailable", http.StatusServiceUnavailable)
			return
		}
		fb := h.fallback(rule)
		if fb == nil {
			h.log.Warn("store unavailable, failing open without a fallback", "rule", rule.Name, "err", err)
			h.metrics.decisions.WithLabelValues(rule.Name, "open").Inc()
			h.proxy.ServeHTTP(w, r)
			return
		}
		h.log.Warn("store unavailable, using in-process fallback", "rule", rule.Name, "err", err)
		d, _ = fb.Allow(context.Background(), rule.Name+":"+key, limit, h.now())
	}

	setHeaders(w.Header(), limit, d)
	if !d.Allowed {
		h.metrics.decisions.WithLabelValues(rule.Name, "denied").Inc()
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}
	h.metrics.decisions.WithLabelValues(rule.Name, "allowed").Inc()
	h.proxy.ServeHTTP(w, r)
}

// setHeaders writes the IETF draft-ietf-httpapi-ratelimit-headers fields and,
// on refusal, Retry-After. Values are whole seconds, rounded up, because a
// client that retries a millisecond early gets refused again.
func setHeaders(hd http.Header, l limiter.Limit, d limiter.Decision) {
	hd.Set("RateLimit-Limit", strconv.Itoa(l.Burst))
	hd.Set("RateLimit-Remaining", strconv.Itoa(d.Remaining))
	hd.Set("RateLimit-Reset", strconv.Itoa(ceilSeconds(d.Reset)))
	if !d.Allowed {
		hd.Set("Retry-After", strconv.Itoa(max(1, ceilSeconds(d.RetryAfter))))
	}
}

func ceilSeconds(d time.Duration) int {
	return int(math.Ceil(d.Seconds()))
}

func (h *Handler) match(path string) *config.Rule {
	for i := range h.cfg.Rules {
		if strings.HasPrefix(path, h.cfg.Rules[i].PathPrefix) {
			return &h.cfg.Rules[i]
		}
	}
	return nil
}

func (h *Handler) key(rule *config.Rule, r *http.Request) string {
	if rule.Key == "api_key" {
		return keys.APIKey(r)
	}
	return keys.ClientIP(r, h.cfg.Trusted())
}

func (h *Handler) primary(rule *config.Rule) limiter.Limiter {
	if rule.Algorithm == "sliding_window" {
		return h.stores.SlidingWindow
	}
	return h.stores.TokenBucket
}

func (h *Handler) fallback(rule *config.Rule) limiter.Limiter {
	if rule.Algorithm == "sliding_window" {
		return h.stores.FallbackSlidingWindow
	}
	return h.stores.FallbackTokenBucket
}
