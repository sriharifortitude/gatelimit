package limiter

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Skipped unless REDIS_ADDR is set. The scenarios are the memory store's,
// with the same hand-computed expectations: the two stores must agree.

func redisStore(t *testing.T) *Redis {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis at %s: %v", addr, err)
	}
	prefix := fmt.Sprintf("gatelimit-test:%d:", time.Now().UnixNano())
	t.Cleanup(func() {
		keys, _ := client.Keys(context.Background(), prefix+"*").Result()
		if len(keys) > 0 {
			client.Del(context.Background(), keys...)
		}
		client.Close()
	})
	return NewRedis(client, prefix)
}

func TestRedisTokenBucketMatchesMemory(t *testing.T) {
	tb := redisStore(t).TokenBucket()
	l := Limit{Rate: 5, Period: time.Second, Burst: 3}
	ctx := context.Background()
	for i, want := range []int{2, 1, 0} {
		d, err := tb.Allow(ctx, "k", l, t0)
		if err != nil || !d.Allowed || d.Remaining != want {
			t.Fatalf("request %d: %+v %v", i, d, err)
		}
	}
	d, _ := tb.Allow(ctx, "k", l, t0)
	if d.Allowed || d.RetryAfter != 200*time.Millisecond || d.Reset != 600*time.Millisecond {
		t.Fatalf("fourth: %+v", d)
	}
	d, _ = tb.Allow(ctx, "k", l, t0.Add(300*time.Millisecond))
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("at +300ms: %+v", d)
	}
}

func TestRedisSlidingWindowMatchesMemory(t *testing.T) {
	sw := redisStore(t).SlidingWindow()
	l := Limit{Rate: 10, Period: time.Minute}
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if d, err := sw.Allow(ctx, "k", l, t0); err != nil || !d.Allowed {
			t.Fatalf("request %d: %+v %v", i, d, err)
		}
	}
	if d, _ := sw.Allow(ctx, "k", l, t0.Add(30*time.Second)); d.Allowed {
		t.Fatalf("11th must be denied: %+v", d)
	}
	d, _ := sw.Allow(ctx, "k", l, t0.Add(75*time.Second))
	if !d.Allowed || d.Remaining != 1 {
		t.Fatalf("at 12:01:15: %+v", d)
	}
	d, _ = sw.Allow(ctx, "k", l, t0.Add(75*time.Second))
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("second at 12:01:15: %+v", d)
	}
	d, _ = sw.Allow(ctx, "k", l, t0.Add(75*time.Second))
	if d.Allowed || d.RetryAfter != 3*time.Second || d.Reset != 45*time.Second {
		t.Fatalf("third at 12:01:15: %+v", d)
	}
}

// Two stores over two connections stand in for two proxy instances. Exactly
// burst requests pass in total, which is the property a single-instance
// limiter cannot give and the reason Redis is here.
func TestRedisTokenBucketExactAcrossInstances(t *testing.T) {
	a := redisStore(t)
	b := NewRedis(redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR")}), a.prefix)
	l := Limit{Rate: 1, Period: time.Second, Burst: 50}
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for g, store := range []*Redis{a, b, a, b} {
		wg.Add(1)
		go func(store *Redis, g int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				d, err := store.TokenBucket().Allow(context.Background(), "shared", l, t0)
				if err != nil {
					t.Errorf("goroutine %d: %v", g, err)
					return
				}
				if d.Allowed {
					mu.Lock()
					allowed++
					mu.Unlock()
				}
			}
		}(store, g)
	}
	wg.Wait()
	if allowed != 50 {
		t.Fatalf("allowed %d of 100, want exactly 50", allowed)
	}
}

func TestRedisUnavailableIsErrStore(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond, MaxRetries: -1})
	tb := NewRedis(client, "x:").TokenBucket()
	_, err := tb.Allow(context.Background(), "k", Limit{Rate: 1, Period: time.Second, Burst: 1}, t0)
	if err == nil || !IsStoreError(err) {
		t.Fatalf("want an ErrStore-wrapped error, got %v", err)
	}
}
