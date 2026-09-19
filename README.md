# gatelimit

[![CI](https://github.com/sriharifortitude/gatelimit/actions/workflows/ci.yml/badge.svg)](https://github.com/sriharifortitude/gatelimit/actions/workflows/ci.yml)

A rate-limiting reverse proxy in Go. Put it in front of an API, give it
a JSON file of rules, and it enforces per-key or per-IP limits with
token buckets or sliding windows — in one process, or across many with
Redis — and tells clients exactly where they stand with the IETF
`RateLimit-*` headers.

    go install github.com/sriharifortitude/gatelimit/cmd/gatelimit@latest
    gatelimit -config gatelimit.json

```json
{
  "listen": ":8080",
  "upstream": "http://127.0.0.1:4200",
  "store": "redis",
  "redis_addr_env": "REDIS_ADDR",
  "on_store_error": "open",
  "trusted_proxies": ["10.0.0.0/8"],
  "rules": [
    {"name": "ingest", "path_prefix": "/api/events", "key": "api_key",
     "algorithm": "token_bucket", "rate": 50, "period": "1s", "burst": 200},
    {"name": "query", "path_prefix": "/api/query", "key": "api_key",
     "algorithm": "sliding_window", "rate": 300, "period": "1m"},
    {"name": "health", "path_prefix": "/api/health", "key": "ip",
     "algorithm": "token_bucket", "rate": 5, "period": "1s"}
  ]
}
```

    $ for i in $(seq 12); do curl -s -o /dev/null -w '%{http_code} ' localhost:8080/api/health & done; wait
    200 200 200 200 200 429 429 429 429 429 429 429

    $ curl -si localhost:8080/api/query -H 'authorization: Bearer eg_…' -d '…' | grep -i ratelimit
    RateLimit-Limit: 300
    RateLimit-Remaining: 298
    RateLimit-Reset: 56

## What it does

- **Rules** are tried in order; the first `path_prefix` that matches
  applies. Each names its key (`api_key` — `X-API-Key` or a bearer token,
  SHA-256'd before it touches Redis or a log — or `ip`), its algorithm and
  its limit.
- **Token bucket**: `rate` per `period`, bursts up to `burst`. Refills
  continuously (fractional tokens), so 5/s at 100 ms elapsed is half a
  token, not none. A clock that runs backwards mints nothing.
- **Sliding window**: at most `rate` in any trailing `period`, using the
  current fixed window plus the previous one weighted by overlap. O(1) per
  key, exact at boundaries, at most one window's approximation inside.
- **Stores**: `memory` for one instance; `redis` for many. The Redis
  paths are Lua scripts, so a decision is one atomic round trip and two
  proxy instances cannot both admit the last token. The integration test
  fires 100 concurrent requests through two clients and asserts exactly
  50 pass.
- **When Redis is down**: `on_store_error` is `closed` (503 with
  `Retry-After: 1`) or `open` (fall back to the in-process limiter, which
  still enforces the rule per instance). Either way it is counted in
  `gatelimit_store_errors_total` and logged once per request, and the
  proxy still starts if Redis is unreachable at boot.
- **Headers**: `RateLimit-Limit`, `RateLimit-Remaining`, `RateLimit-Reset`
  on every limited response; `Retry-After` on 429, whole seconds rounded
  up so a client that obeys it is not refused again.
- **`X-Forwarded-For`** is believed only from `trusted_proxies`, and then
  the rightmost address not belonging to a trusted proxy is the client —
  anything left of it was written by the client.
- **A keyed rule with no key is 401**, not "one shared bucket for
  everyone anonymous".
- `/healthz`, Prometheus `/metrics` (decisions by rule and outcome, store
  errors by policy, decision latency histogram), structured JSON logs,
  graceful shutdown.

## Checks

    go vet ./...
    staticcheck ./...
    go test ./...                      # memory store, config, keys, proxy: no services needed
    go test -race ./...                # CI runs this; needs cgo
    REDIS_ADDR=127.0.0.1:6379 go test ./internal/limiter/   # Redis scenarios + cross-instance exactness
    go test -bench . -run xxx ./internal/limiter/

Every limiter expectation is worked out by hand from the arithmetic
(refill fractions, retry-after seconds, window weights) and the Redis
tests replay the same scenarios so the two stores are held to the same
numbers. On this machine the in-memory token bucket decides in ~290 ns
with zero allocations.

## Design notes

1. [Two algorithms, two stores, one arithmetic](docs/adr/0001-shared-arithmetic.md)
2. [Fail-open with a local fallback](docs/adr/0002-store-error-policy.md)

## What it deliberately does not do

- **No TLS termination, no auth.** It sits behind whatever terminates TLS
  and in front of whatever authenticates. It hashes credentials; it does
  not check them.
- **Prefix matching only.** No method matching, no regex, no host
  matching. Each would be a few lines in `match`; none is here until
  someone needs it.
- **Sliding window is an approximation** (documented in ADR 1). If exact
  trailing-window counts matter, a sorted-set log per key is the
  replacement, at O(rate) memory per key.
- **In-process fallback is per instance.** With `open` and Redis down, N
  instances allow up to N× the limit between them. That is the trade
  named in ADR 2, and the metric that says it is happening.
- **No config reload.** Restart to change rules; the proxy shuts down
  gracefully. A SIGHUP reload is the obvious next feature.
- **No per-rule store or policy.** One store, one policy, for all rules.

## Licence

MIT.
