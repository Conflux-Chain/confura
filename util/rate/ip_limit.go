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

type visitor struct {
	limiter  *rate.Limiter // token bucket
	lastSeen time.Time     // used for GC when visitor inactive for a while
}

// IpLimiter is used to limit requests from different users.
type IpLimiter struct {
	Option

	// ip => visitor
	visitors map[string]*visitor

	mu sync.Mutex
}

func NewIpLimiter(rate rate.Limit, burst int) *IpLimiter {
	return &IpLimiter{
		Option:   Option{rate, burst},
		visitors: make(map[string]*visitor),
	}
}

func (l *IpLimiter) Allow(ip string, n int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	v, ok := l.visitors[ip]
	if !ok {
		v = &visitor{
			limiter: rate.NewLimiter(l.Rate, l.Burst),
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

func (l *IpLimiter) Update(option Option) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.Rate != option.Rate || l.Burst != option.Burst {
		for _, visitor := range l.visitors {
			visitor.limiter.SetLimit(option.Rate)
			visitor.limiter.SetBurst(option.Burst)
		}
	}
}
