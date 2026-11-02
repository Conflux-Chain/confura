package rate

import (
	"context"
	"encoding/json"
	xrate "golang.org/x/time/rate"
	"math"
	"testing"
	"time"

	limiter "github.com/Conflux-Chain/go-conflux-util/rate"
)

func TestCreateDoesNotAcquireStrategyLock(t *testing.T) {
	r := &Registry{strategies: map[string]*Strategy{"default": {Name: "default", LimitOptions: map[string]interface{}{"rpc": TokenBucketOption{Rate: 1, Burst: 1}}}}}
	r.publishLimiterStrategies()
	// Reload holds mu while waiting for the dependency registry lock. The
	// dependency holds that lock when it invokes Create: Create must progress.
	r.mu.Lock()
	defer r.mu.Unlock()
	done := make(chan error, 1)
	go func() { _, err := r.Create(context.Background(), "rpc", "default"); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Create waits for reload lock")
	}
}

func TestFractionalTokenBucket(t *testing.T) {
	r := &Registry{}
	l, err := r.createWithOption(TokenBucketOption{Rate: 0.5, Burst: 1})
	if err != nil {
		t.Fatal(err)
	}
	bucket := l.(*limiter.TokenBucket)
	// Past times make this test independent of the old dependency's Delay()
	// wall-clock implementation as well as its corrected DelayFrom(now).
	now := time.Now().Add(-10 * time.Second)
	if err := bucket.LimitAt(now, 1); err != nil {
		t.Fatal(err)
	}
	if err := bucket.LimitAt(now.Add(2*time.Second), 1); err != nil {
		t.Fatalf("fractional rate did not refill: %v", err)
	}
}

func TestInvalidLimiterOptionsReturnErrors(t *testing.T) {
	r := &Registry{}
	for _, opt := range []interface{}{TokenBucketOption{Rate: 0, Burst: 1}, TokenBucketOption{Rate: -1, Burst: 1}, TokenBucketOption{Rate: 1, Burst: 0}, TokenBucketOption{Rate: xrate.Limit(math.NaN()), Burst: 1}, TokenBucketOption{Rate: 1e-20, Burst: 1}, FixedWindowOption{}, FixedWindowOption{Interval: -1, Quota: 1}} {
		if _, err := r.createWithOption(opt); err == nil {
			t.Errorf("accepted invalid option %+v", opt)
		}
	}
	var s Strategy
	if err := json.Unmarshal([]byte(`{"rpc":null}`), &s); err == nil {
		t.Fatal("accepted null rule")
	}
	if err := json.Unmarshal([]byte(`{"rpc":{"algo":"token_bucket","option":{"rate":0.5,"burst":1}}}`), &s); err != nil {
		t.Fatal(err)
	}
}
