# 2. Store failure is a policy, and fail-open keeps a local limiter

Status: accepted — 2026-09-19

## Context

Redis will be unreachable at some point. A rate limiter that fails
closed turns a cache outage into an API outage; one that fails open
turns it into an unprotected API. Both are correct for someone.

## Decision

- `on_store_error` is required in the config — there is no default —
  because the person deploying must choose. `closed` answers 503 with
  `Retry-After: 1`. `open` lets requests through.
- Failing open does not mean unlimited. When the primary store is Redis,
  each proxy instance also holds an in-process limiter, and a store error
  hands the decision to it with the same rule. The limit then applies per
  instance rather than globally: N instances admit up to N× the limit
  between them, which is degraded, not absent.
- Only errors wrapped in `ErrStore` trigger the policy. Anything else
  from a limiter is a bug and is answered 500 so it is noticed.
- The proxy starts even if Redis is unreachable at boot, for the same
  reason: a proxy that refuses to start takes the upstream offline, which
  is the closed policy applied by accident.
- Every store error increments `gatelimit_store_errors_total{policy}` and
  logs at warn level with the rule and the error. The metric is what an
  alert should watch; the log is what the on-call reads next.

## Consequences

- A Redis outage under `open` is visible (metric, log), survivable
  (requests flow), and bounded (per-instance limits). Under `closed` it
  is visible and loud.
- The per-request Redis timeout is 250 ms via the request context, and
  the client is built with no retries. A slow Redis therefore costs a
  slow decision once, not a retry storm, and then the policy applies.
- The fallback store's state is not synchronised with Redis. When Redis
  returns, clients may briefly see a different `RateLimit-Remaining` than
  they would have; the numbers are consistent within each store, not
  across the switch. Making the switch seamless would need the fallback
  to replay into Redis, which was judged not worth the complexity for a
  window that is, by definition, an incident.
