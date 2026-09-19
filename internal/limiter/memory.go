package limiter

import (
	"context"
	"sync"
	"time"
)

// Memory keeps state in the process. One instance, one limit; also the
// fallback when Redis is unreachable and the policy is fail-open.
//
// A single mutex guards the map. At the request rates a rate limiter sees
// this is not the bottleneck -- the proxy hop is -- and sharding the map
// would buy contention headroom nobody has asked for. The benchmark in
// memory_test.go is what would justify changing that.
type Memory struct {
	mu      sync.Mutex
	buckets map[string]*bucketState
	windows map[string]*windowState
	// idle is how long a key may go unused before the sweeper drops it.
	idle time.Duration
}

type bucketState struct {
	tokens float64
	last   time.Time
}

type windowState struct {
	start time.Time
	prev  int
	cur   int
	last  time.Time
}

// NewMemory returns a store whose keys expire after idle. Call Sweep
// periodically (the proxy does) or memory grows with the number of distinct
// keys ever seen.
func NewMemory(idle time.Duration) *Memory {
	return &Memory{buckets: map[string]*bucketState{}, windows: map[string]*windowState{}, idle: idle}
}

// TokenBucket returns a Limiter using the token-bucket algorithm over this store.
func (m *Memory) TokenBucket() Limiter { return memoryTokenBucket{m} }

// SlidingWindow returns a Limiter using the sliding-window algorithm over this store.
func (m *Memory) SlidingWindow() Limiter { return memorySlidingWindow{m} }

type memoryTokenBucket struct{ m *Memory }

func (t memoryTokenBucket) Allow(_ context.Context, key string, l Limit, now time.Time) (Decision, error) {
	t.m.mu.Lock()
	defer t.m.mu.Unlock()
	s, ok := t.m.buckets[key]
	if !ok {
		s = &bucketState{}
		t.m.buckets[key] = s
	}
	tokens, d := tokenBucketStep(s.tokens, s.last, now, l, !ok)
	s.tokens, s.last = tokens, now
	return d, nil
}

type memorySlidingWindow struct{ m *Memory }

func (w memorySlidingWindow) Allow(_ context.Context, key string, l Limit, now time.Time) (Decision, error) {
	w.m.mu.Lock()
	defer w.m.mu.Unlock()
	s, ok := w.m.windows[key]
	if !ok {
		s = &windowState{start: now.Truncate(l.Period)}
		w.m.windows[key] = s
	}
	// Roll fixed windows forward. One step if the next window has begun;
	// a fresh start if more than a whole window has passed (prev is then 0).
	windowStart := now.Truncate(l.Period)
	switch {
	case windowStart.Equal(s.start):
	case windowStart.Sub(s.start) == l.Period:
		s.prev, s.cur, s.start = s.cur, 0, windowStart
	default:
		s.prev, s.cur, s.start = 0, 0, windowStart
	}
	allowed, d := slidingWindowStep(s.prev, s.cur, s.start, now, l)
	if allowed {
		s.cur++
	}
	s.last = now
	return d, nil
}

// Sweep drops keys not touched within the idle period. Returns how many were dropped.
func (m *Memory) Sweep(now time.Time) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	dropped := 0
	for k, s := range m.buckets {
		if now.Sub(s.last) > m.idle {
			delete(m.buckets, k)
			dropped++
		}
	}
	for k, s := range m.windows {
		if now.Sub(s.last) > m.idle {
			delete(m.windows, k)
			dropped++
		}
	}
	return dropped
}

// Len reports how many keys are held, for metrics and tests.
func (m *Memory) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.buckets) + len(m.windows)
}
