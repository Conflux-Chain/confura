package rate

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type VisitContext struct {
	Ip       string     // visiter IP
	Key      string     // visiter key
	Status   *VipStatus // visiter VIP status
	Resource string     // visited resource (also used as limit rule)
}

type Limiter interface {
	Allow(vc *VisitContext, n int) bool
	GC(timeout time.Duration)
	Update(option Option) bool
}

// IpLimiter limiting by IP address
type IpLimiter struct {
	*visitLimiter
}

func NewIpLimiter(option Option) *IpLimiter {
	return &IpLimiter{
		visitLimiter: newVisitLimiter(option.Rate, option.Burst),
	}
}

func (l *IpLimiter) Allow(vc *VisitContext, n int) bool {
	return l.visitLimiter.Allow(vc.Ip, n)
}

// KeyLimiter limiting by limit key
type KeyLimiter struct {
	*visitLimiter
}

func NewKeyLimiter(option Option) *KeyLimiter {
	return &KeyLimiter{
		visitLimiter: newVisitLimiter(option.Rate, option.Burst),
	}
}

func (l *KeyLimiter) Allow(vc *VisitContext, n int) bool {
	return l.visitLimiter.Allow(vc.Key, n)
}

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

// visitLimiter is used to limit visit requests in terms of specific entity using
// token bucket algorithm.
type visitLimiter struct {
	Option

	// limit entity (eg., IP or limit key etc.) => visitor
	visitors map[string]*visitor

	mu sync.Mutex
}

func newVisitLimiter(rate rate.Limit, burst int) *visitLimiter {
	return &visitLimiter{
		Option:   Option{rate, burst},
		visitors: make(map[string]*visitor),
	}
}

func (l *visitLimiter) Allow(entity string, n int) bool {
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

func (l *visitLimiter) GC(timeout time.Duration) {
	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	for entity, v := range l.visitors {
		if v.lastSeen.Add(timeout).Before(now) {
			delete(l.visitors, entity)
		}
	}
}

func (l *visitLimiter) Update(option Option) bool {
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
