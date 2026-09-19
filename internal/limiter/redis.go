package limiter

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis stores state in Redis so every proxy instance enforces one limit.
// Both algorithms are Lua scripts: Redis runs a script without interleaving
// other commands, so the read-modify-write is atomic without WATCH/MULTI
// round trips. The scripts take the time from the caller, not from Redis,
// so that tests can drive the clock and so that the arithmetic is the same
// as the in-memory store's to the millisecond.
type Redis struct {
	client redis.Scripter
	prefix string
}

// NewRedis returns a store over client. Keys are prefixed so several
// gatelimit deployments can share an instance.
func NewRedis(client redis.Scripter, prefix string) *Redis {
	return &Redis{client: client, prefix: prefix}
}

// TokenBucket returns a Limiter using the token-bucket algorithm over Redis.
func (r *Redis) TokenBucket() Limiter { return redisTokenBucket{r} }

// SlidingWindow returns a Limiter using the sliding-window algorithm over Redis.
func (r *Redis) SlidingWindow() Limiter { return redisSlidingWindow{r} }

// Mirrors tokenBucketStep. Times are milliseconds since the epoch; tokens
// are stored as a decimal string so no precision is lost to Redis's
// integer handling.
var tokenBucketScript = redis.NewScript(`
local tokens = redis.call('HGET', KEYS[1], 't')
local last = redis.call('HGET', KEYS[1], 'ts')
local rate = tonumber(ARGV[1])      -- tokens per second
local burst = tonumber(ARGV[2])
local now = tonumber(ARGV[3])       -- ms
if tokens == false then
  tokens = burst
else
  tokens = tonumber(tokens)
  local elapsed = (now - tonumber(last)) / 1000
  if elapsed < 0 then elapsed = 0 end
  tokens = math.min(burst, tokens + elapsed * rate)
end
local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end
redis.call('HSET', KEYS[1], 't', string.format('%.9f', tokens), 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 1000)
return {allowed, string.format('%.9f', tokens)}
`)

type redisTokenBucket struct{ r *Redis }

func (t redisTokenBucket) Allow(ctx context.Context, key string, l Limit, now time.Time) (Decision, error) {
	rate := perSecond(l)
	res, err := tokenBucketScript.Run(ctx, t.r.client, []string{t.r.prefix + "tb:" + key},
		strconv.FormatFloat(rate, 'f', -1, 64), l.Burst, now.UnixMilli()).Slice()
	if err != nil {
		return Decision{}, fmt.Errorf("%w: %v", ErrStore, err)
	}
	allowed, tokens, err := parseTokenBucketReply(res)
	if err != nil {
		return Decision{}, fmt.Errorf("%w: %v", ErrStore, err)
	}
	d := Decision{Allowed: allowed, Remaining: int(math.Floor(tokens))}
	if !allowed {
		d.RetryAfter = secondsToDuration((1 - tokens) / rate)
	}
	d.Reset = secondsToDuration((float64(l.Burst) - tokens) / rate)
	return d, nil
}

func parseTokenBucketReply(res []any) (bool, float64, error) {
	if len(res) != 2 {
		return false, 0, fmt.Errorf("unexpected reply %v", res)
	}
	allowed, ok := res[0].(int64)
	if !ok {
		return false, 0, fmt.Errorf("unexpected reply %v", res)
	}
	text, ok := res[1].(string)
	if !ok {
		return false, 0, fmt.Errorf("unexpected reply %v", res)
	}
	tokens, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return false, 0, err
	}
	return allowed == 1, tokens, nil
}

// Two counters per key, one per fixed window, named by the window's start.
// The script increments the current window only when the request is
// admitted, mirroring the memory store, and returns the counts it decided
// on so the caller can compute the headers with the shared arithmetic.
var slidingWindowScript = redis.NewScript(`
local curKey = KEYS[1]
local prevKey = KEYS[2]
local limit = tonumber(ARGV[1])
local weight = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])       -- ms
local prev = tonumber(redis.call('GET', prevKey) or '0')
local cur = tonumber(redis.call('GET', curKey) or '0')
local estimate = prev * weight + cur
local allowed = 0
if estimate + 1 <= limit then
  allowed = 1
  cur = redis.call('INCR', curKey)
  redis.call('PEXPIRE', curKey, ttl)
end
return {allowed, prev, cur}
`)

type redisSlidingWindow struct{ r *Redis }

func (w redisSlidingWindow) Allow(ctx context.Context, key string, l Limit, now time.Time) (Decision, error) {
	windowStart := now.Truncate(l.Period)
	elapsed := now.Sub(windowStart)
	weight := 1 - elapsed.Seconds()/l.Period.Seconds()
	if weight < 0 {
		weight = 0
	}
	curKey := fmt.Sprintf("%ssw:%s:%d", w.r.prefix, key, windowStart.UnixMilli())
	prevKey := fmt.Sprintf("%ssw:%s:%d", w.r.prefix, key, windowStart.Add(-l.Period).UnixMilli())
	// Keep a window's counter for two periods: it is the "previous" window for the whole of the next one.
	res, err := slidingWindowScript.Run(ctx, w.r.client, []string{curKey, prevKey},
		l.Rate, strconv.FormatFloat(weight, 'f', -1, 64), (2 * l.Period).Milliseconds()).Slice()
	if err != nil {
		return Decision{}, fmt.Errorf("%w: %v", ErrStore, err)
	}
	if len(res) != 3 {
		return Decision{}, fmt.Errorf("%w: unexpected reply %v", ErrStore, res)
	}
	allowed, _ := res[0].(int64)
	prev, _ := res[1].(int64)
	cur, _ := res[2].(int64)
	// The script already incremented cur when it admitted; the shared
	// arithmetic expects the pre-admission count, so undo that for the
	// header computation.
	if allowed == 1 {
		cur--
	}
	_, d := slidingWindowStep(int(prev), int(cur), windowStart, now, l)
	return d, nil
}
