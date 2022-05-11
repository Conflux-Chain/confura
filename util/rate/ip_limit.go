package rate

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type visitor struct {
	limiter  *rate.Limiter // token bucket
	lastSeen time.Time     // used for GC when visitor inactive for a while
}

// IpLimiter is used to limit requests from different users.
type IpLimiter struct {
	// ip => visitor
	visitors map[string]*visitor

	// uniform config for all visitors
	rate  rate.Limit
	burst int

	mu sync.Mutex
}

func NewIpLimiter(rate rate.Limit, burst int) *IpLimiter {
	return &IpLimiter{
		visitors: make(map[string]*visitor),
		rate:     rate,
		burst:    burst,
	}
}

func (l *IpLimiter) Allow(ip string, n int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	v, ok := l.visitors[ip]
	if !ok {
		v = &visitor{
			limiter: rate.NewLimiter(l.rate, l.burst),
		}
		l.visitors[ip] = v
	}

	v.lastSeen = time.Now()

	return v.limiter.AllowN(v.lastSeen, n)
}

func (l *IpLimiter) GC(timeout time.Duration) {
	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	for ip, v := range l.visitors {
		if v.lastSeen.Add(timeout).Before(now) {
			delete(l.visitors, ip)
		}
	}
}
