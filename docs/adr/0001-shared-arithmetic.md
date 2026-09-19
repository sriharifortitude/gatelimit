# 1. Two algorithms, two stores, one arithmetic

Status: accepted — 2026-09-19

## Context

A limiter that runs in one process and a limiter that runs across many
have different storage but must give the same answer for the same
history, or a deployment that scales from one instance to three changes
its behaviour for reasons nobody can explain to a client reading the
`RateLimit-Remaining` header.

## Decision

- The decision arithmetic lives in two pure functions,
  `tokenBucketStep` and `slidingWindowStep`, that take state and a clock
  and return new state and a `Decision`. The memory store calls them
  under a mutex.
- The Redis store runs Lua scripts that mirror those functions line for
  line (the token-bucket script keeps tokens as a decimal string so no
  precision is lost), and then calls the same Go functions to produce the
  headers from the counts the script decided on.
- The clock is always supplied by the caller — never `time.Now()` in the
  store, never `TIME` in Redis — so tests drive it, and so both stores see
  the same `now` for the same request.
- The sliding window is the two-fixed-window approximation: the trailing
  count is the current window plus the previous window scaled by how much
  of it is still inside the trailing period.

## Consequences

- The test files for the two stores contain the same scenarios with the
  same hand-computed numbers: 200 ms retry-after at 5/s with an empty
  bucket, 3 s retry-after at 12:01:15 with 10 previous and 2 current.
  Divergence is a failing test, not a support ticket.
- The approximation can over-admit slightly when the previous window's
  traffic was concentrated at its end, and under-admit when it was at the
  start; the error is bounded by one window's count and is zero at window
  boundaries. For "300 per minute" that is the right trade against a
  sorted-set log per key. For "3 per day" it is not, and the README says
  so.
- Redis round trips carry the clock as a parameter, which means a proxy
  with a wrong clock enforces wrong limits. Instances are expected to run
  NTP; the Lua script clamps negative elapsed time to zero so a lagging
  instance cannot mint tokens, only fail to refill.
- Floating-point in the arithmetic is rounded to the millisecond with a
  microsecond of tolerance, because 0.3 × 60 is 18.000000000000004 and a
  header must say 18.
