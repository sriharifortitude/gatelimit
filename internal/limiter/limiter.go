// Package limiter decides whether a request identified by a key may pass.
//
// Two algorithms are provided. A token bucket admits bursts up to a size
// and refills at a rate; it is the right shape for "100 requests per
// second, bursts of 200". A sliding window bounds the count over any
// trailing window; it is the right shape for "1000 requests per hour" where
// a burst that large would be a problem in itself.
//
// Each algorithm has two stores: an in-process one, and Redis, so that
// several proxy instances enforce one limit. The Redis implementations run
// the read-modify-write as a Lua script, which is what makes the decision
// atomic across instances.
package limiter

import (
	"context"
	"errors"
	"math"
	"time"
)

// Limit is what a rule allows.
type Limit struct {
	// Requests admitted per Period at steady state.
	Rate int
	// Period over which Rate applies.
	Period time.Duration
	// Burst is the bucket size for token buckets; ignored by sliding windows.
	Burst int
}

// Decision is the outcome for one request.
type Decision struct {
	Allowed bool
	// Remaining requests before the limit bites, as a client would want to see it.
	Remaining int
	// RetryAfter is how long until the next request would be allowed; zero when Allowed.
	RetryAfter time.Duration
	// Reset is how long until the limit is fully replenished.
	Reset time.Duration
}

// Limiter answers for one algorithm over one store.
type Limiter interface {
	Allow(ctx context.Context, key string, limit Limit, now time.Time) (Decision, error)
}

// ErrStore is wrapped by store errors so the proxy can apply its fail-open or
// fail-closed policy to them and to nothing else.
var ErrStore = errors.New("limiter store unavailable")

// perSecond is the refill rate as tokens per second.
func perSecond(l Limit) float64 {
	return float64(l.Rate) / l.Period.Seconds()
}

// tokenBucketStep is the pure arithmetic of one token-bucket decision, shared
// by the memory store and mirrored exactly by the Redis Lua script. Tokens
// are a float so that a rate of 5 per second at 100 ms elapsed refills 0.5
// of a token, not zero.
func tokenBucketStep(tokens float64, last, now time.Time, l Limit, fresh bool) (nextTokens float64, d Decision) {
	burst := float64(l.Burst)
	rate := perSecond(l)
	if fresh {
		tokens = burst
	} else {
		elapsed := now.Sub(last).Seconds()
		if elapsed < 0 {
			elapsed = 0
		}
		tokens = math.Min(burst, tokens+elapsed*rate)
	}
	if tokens >= 1 {
		tokens--
		d.Allowed = true
	} else {
		d.RetryAfter = secondsToDuration((1 - tokens) / rate)
	}
	d.Remaining = int(math.Floor(tokens))
	d.Reset = secondsToDuration((burst - tokens) / rate)
	return tokens, d
}

// slidingWindowStep is the pure arithmetic of one sliding-window decision.
// The count over the trailing window is estimated as the current fixed
// window's count plus the previous window's count weighted by how much of
// the previous window still falls inside the trailing window. This is the
// standard approximation: exact at window boundaries, never more than one
// window's skew inside, and O(1) storage per key.
func slidingWindowStep(prev, cur int, windowStart time.Time, now time.Time, l Limit) (allowed bool, d Decision) {
	elapsed := now.Sub(windowStart)
	weight := 1 - elapsed.Seconds()/l.Period.Seconds()
	if weight < 0 {
		weight = 0
	}
	estimate := float64(prev)*weight + float64(cur)
	if estimate+1 <= float64(l.Rate) {
		d.Allowed = true
		estimate++
	} else {
		d.RetryAfter = retryAfterSliding(prev, cur, elapsed, l)
	}
	remaining := float64(l.Rate) - estimate
	if remaining < 0 {
		remaining = 0
	}
	d.Remaining = int(math.Floor(remaining))
	d.Reset = l.Period - elapsed
	if d.Reset < 0 {
		d.Reset = 0
	}
	return d.Allowed, d
}

// retryAfterSliding: how far into the future the weighted previous count has
// decayed enough for one more request. With no previous-window contribution
// the answer is the rest of the current window.
func retryAfterSliding(prev, cur int, elapsed time.Duration, l Limit) time.Duration {
	if prev == 0 || cur >= l.Rate {
		return l.Period - elapsed
	}
	// Need prev*weight + cur + 1 <= Rate  =>  weight <= (Rate - cur - 1)/prev
	target := (float64(l.Rate) - float64(cur) - 1) / float64(prev)
	if target < 0 {
		return l.Period - elapsed
	}
	// weight = 1 - t/Period  =>  t = (1 - target) * Period
	at := secondsToDuration((1 - target) * l.Period.Seconds())
	if at <= elapsed {
		return time.Millisecond
	}
	return at - elapsed
}

// secondsToDuration rounds up to the millisecond, with a nanosecond of
// tolerance so that 18.000000000000004 s is 18 s and not 18.001 s.
func secondsToDuration(s float64) time.Duration {
	if s <= 0 {
		return 0
	}
	return time.Duration(math.Ceil(s*1000-1e-6)) * time.Millisecond
}

// IsStoreError reports whether err came from a store being unavailable,
// as opposed to a limit being exceeded or a bad argument.
func IsStoreError(err error) bool {
	return errors.Is(err, ErrStore)
}
