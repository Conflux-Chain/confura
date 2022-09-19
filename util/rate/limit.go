package rate

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type Option struct {
	Rate  rate.Limit
	Burst int
}

func NewOption(r int, b int) Option {
	return Option{
		Rate:  rate.Limit(r),
		Burst: b,
	}
}

type visitor struct {
	limiter  *rate.Limiter // token bucket
	lastSeen time.Time     // used for GC when visitor inactive for a while
}

// VistingLimiter is used to limit visiting requests made by some entity of specific type
// using token bucket algorithm.
type VistingLimiter struct {
	Option

	// limit entity (eg., IP or API key etc.) => visitor
	visitors map[string]*visitor

	mu sync.Mutex
}

func NewVisitingLimiter(rate rate.Limit, burst int) *VistingLimiter {
	return &VistingLimiter{
		Option:   Option{rate, burst},
		visitors: make(map[string]*visitor),
	}
}

func (l *VistingLimiter) Allow(entity string, n int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	v, ok := l.visitors[entity]
	if !ok {
		v = &visitor{
			limiter: rate.NewLimiter(l.Rate, l.Burst),
		}
		l.visitors[entity] = v
	}

	v.lastSeen = time.Now()

	return v.limiter.AllowN(v.lastSeen, n)
}

func (l *VistingLimiter) GC(timeout time.Duration) {
	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	for entity, v := range l.visitors {
		if v.lastSeen.Add(timeout).Before(now) {
			delete(l.visitors, entity)
		}
	}
}

func (l *VistingLimiter) Update(option Option) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.Rate == option.Rate && l.Burst == option.Burst {
		return false
	}

	l.Option = option

	for _, visitor := range l.visitors {
		visitor.limiter.SetLimit(option.Rate)
		visitor.limiter.SetBurst(option.Burst)
	}

	return true
}
