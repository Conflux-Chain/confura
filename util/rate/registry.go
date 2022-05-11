package rate

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

var DefaultRegistry = NewRegistry()

func init() {
	go DefaultRegistry.gcPeriodically(5*time.Minute, 3*time.Minute)
}

type Registry struct {
	limiters map[string]*IpLimiter
	mu       sync.Mutex
}

func NewRegistry() *Registry {
	return &Registry{
		limiters: make(map[string]*IpLimiter),
	}
}

func (m *Registry) GetOrRegister(name string, rate rate.Limit, burst int) *IpLimiter {
	m.mu.Lock()
	defer m.mu.Unlock()

	limiter, ok := m.limiters[name]
	if !ok {
		limiter = NewIpLimiter(rate, burst)
		m.limiters[name] = limiter
	}

	return limiter
}

func (m *Registry) GC(timeout time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, v := range m.limiters {
		v.GC(timeout)
	}
}

func (m *Registry) gcPeriodically(interval time.Duration, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		m.GC(timeout)
	}
}
