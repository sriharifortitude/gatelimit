package limiter

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

var t0 = time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)

// Every expectation below is worked out by hand from the arithmetic in
// limiter.go, not read back from the code's output.

func TestTokenBucketAdmitsBurstThenRefills(t *testing.T) {
	// 5 per second, burst 3: a fresh key has 3 tokens.
	l := Limit{Rate: 5, Period: time.Second, Burst: 3}
	tb := NewMemory(time.Minute).TokenBucket()
	ctx := context.Background()

	for i, wantRemaining := range []int{2, 1, 0} {
		d, err := tb.Allow(ctx, "k", l, t0)
		if err != nil || !d.Allowed || d.Remaining != wantRemaining {
			t.Fatalf("request %d: got %+v err %v, want allowed with remaining %d", i, d, err, wantRemaining)
		}
	}
	// Fourth request at the same instant: 0 tokens. One token takes 1/5 s = 200 ms.
	d, _ := tb.Allow(ctx, "k", l, t0)
	if d.Allowed || d.RetryAfter != 200*time.Millisecond {
		t.Fatalf("fourth: got %+v, want denied with RetryAfter 200ms", d)
	}
	// Reset: bucket is 3 tokens short at 5/s = 600 ms.
	if d.Reset != 600*time.Millisecond {
		t.Fatalf("reset: got %v, want 600ms", d.Reset)
	}
	// 100 ms later: 0.5 tokens, still denied, 100 ms to go.
	d, _ = tb.Allow(ctx, "k", l, t0.Add(100*time.Millisecond))
	if d.Allowed || d.RetryAfter != 100*time.Millisecond {
		t.Fatalf("at +100ms: got %+v, want denied with RetryAfter 100ms", d)
	}
	// 200 ms after that (300 ms total): 0.5 + 1.0 = 1.5 tokens; allowed, 0.5 left => remaining 0.
	d, _ = tb.Allow(ctx, "k", l, t0.Add(300*time.Millisecond))
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("at +300ms: got %+v, want allowed with remaining 0", d)
	}
	// A long idle refills to the burst, never beyond.
	d, _ = tb.Allow(ctx, "k", l, t0.Add(time.Hour))
	if !d.Allowed || d.Remaining != 2 {
		t.Fatalf("after an hour: got %+v, want remaining 2 (burst 3 minus this one)", d)
	}
}

func TestTokenBucketClockGoingBackwardsDoesNotMintTokens(t *testing.T) {
	l := Limit{Rate: 1, Period: time.Second, Burst: 1}
	tb := NewMemory(time.Minute).TokenBucket()
	ctx := context.Background()
	tb.Allow(ctx, "k", l, t0)
	d, _ := tb.Allow(ctx, "k", l, t0.Add(-time.Hour))
	if d.Allowed {
		t.Fatalf("a backwards clock must not refill: %+v", d)
	}
}

func TestSlidingWindowWeightsThePreviousWindow(t *testing.T) {
	// 10 per minute.
	l := Limit{Rate: 10, Period: time.Minute}
	sw := NewMemory(time.Hour).SlidingWindow()
	ctx := context.Background()

	// Minute 0: use all 10 at 12:00:00.
	for i := 0; i < 10; i++ {
		if d, _ := sw.Allow(ctx, "k", l, t0); !d.Allowed {
			t.Fatalf("request %d in first window should pass: %+v", i, d)
		}
	}
	if d, _ := sw.Allow(ctx, "k", l, t0.Add(30*time.Second)); d.Allowed {
		t.Fatalf("11th in the same window must be denied: %+v", d)
	}

	// 12:01:15 -- 15 s into the next window: weight of the previous window is 45/60 = 0.75.
	// Estimate = 10*0.75 + 0 = 7.5; 7.5 + 1 <= 10, allowed; remaining = floor(10 - 8.5) = 1.
	d, _ := sw.Allow(ctx, "k", l, t0.Add(75*time.Second))
	if !d.Allowed || d.Remaining != 1 {
		t.Fatalf("at 12:01:15: got %+v, want allowed with remaining 1", d)
	}
	// Second at the same instant: 7.5 + 1 = 8.5; 8.5 + 1 <= 10 allowed; remaining floor(10-9.5) = 0.
	d, _ = sw.Allow(ctx, "k", l, t0.Add(75*time.Second))
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("second at 12:01:15: got %+v, want allowed with remaining 0", d)
	}
	// Third: 7.5 + 2 = 9.5; 10.5 > 10 denied.
	// RetryAfter: need 10*w + 2 + 1 <= 10 => w <= 0.7 => t = 0.3 * 60 s = 18 s into the window; we are at 15 s => 3 s.
	d, _ = sw.Allow(ctx, "k", l, t0.Add(75*time.Second))
	if d.Allowed || d.RetryAfter != 3*time.Second {
		t.Fatalf("third at 12:01:15: got %+v, want denied with RetryAfter 3s", d)
	}
	// Reset is the rest of the current fixed window: 45 s.
	if d.Reset != 45*time.Second {
		t.Fatalf("reset: got %v, want 45s", d.Reset)
	}

	// Two whole windows later the history is gone: full allowance.
	d, _ = sw.Allow(ctx, "k", l, t0.Add(3*time.Minute))
	if !d.Allowed || d.Remaining != 9 {
		t.Fatalf("after two idle windows: got %+v, want remaining 9", d)
	}
}

func TestSweepDropsIdleKeys(t *testing.T) {
	m := NewMemory(time.Minute)
	l := Limit{Rate: 1, Period: time.Second, Burst: 1}
	m.TokenBucket().Allow(context.Background(), "old", l, t0)
	m.SlidingWindow().Allow(context.Background(), "fresh", l, t0.Add(59*time.Second))
	// At 12:01:30 "old" has been idle 90 s and "fresh" 31 s; only "old" is over the minute.
	if got := m.Sweep(t0.Add(90 * time.Second)); got != 1 {
		t.Fatalf("sweep dropped %d, want 1 (only the token-bucket key is idle for over a minute)", got)
	}
	if m.Len() != 1 {
		t.Fatalf("len %d, want 1", m.Len())
	}
}

// Under -race: 64 goroutines hammer one key; exactly burst requests may pass
// at a single instant, whatever the interleaving.
func TestTokenBucketConcurrentExactness(t *testing.T) {
	l := Limit{Rate: 1, Period: time.Second, Burst: 100}
	tb := NewMemory(time.Minute).TokenBucket()
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				d, _ := tb.Allow(context.Background(), "k", l, t0)
				if d.Allowed {
					mu.Lock()
					allowed++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	if allowed != 100 {
		t.Fatalf("allowed %d of 640 concurrent requests, want exactly 100", allowed)
	}
}

func BenchmarkMemoryTokenBucket(b *testing.B) {
	l := Limit{Rate: 1000, Period: time.Second, Burst: 1000}
	tb := NewMemory(time.Minute).TokenBucket()
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			tb.Allow(context.Background(), keys[i%len(keys)], l, time.Now())
			i++
		}
	})
}
