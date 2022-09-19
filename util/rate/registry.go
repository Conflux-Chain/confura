package rate

import (
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/sirupsen/logrus"
)

const (
	LimitKeyCacheSize = 5000
)

var (
	DefaultRegistryCfx = NewRegistry()
	DefaultRegistryEth = NewRegistry()
)

func init() {
	go DefaultRegistryCfx.gcPeriodically(5*time.Minute, 3*time.Minute)
	go DefaultRegistryEth.gcPeriodically(5*time.Minute, 3*time.Minute)
}

type Registry struct {
	mu sync.Mutex

	throttlers map[string]*Throttler // strategy => throttler
	strategies []*Strategy           // strategies ordered by priority descendingly

	// loader to retrieve keyset from store
	keyLoader func(key string) (*KeyInfo, error)
	// limit key cache: limit key => strategy ID (if =0 means missing)
	keyCache *lru.Cache
}

func NewRegistry() *Registry {
	cache, _ := lru.New(LimitKeyCacheSize)
	return &Registry{
		throttlers: make(map[string]*Throttler),
		keyCache:   cache,
	}
}

func (m *Registry) Get(vc *VistingContext) (*Throttler, bool) {
	strategies := m.matchStrategies(vc)
	if len(strategies) == 0 {
		logrus.WithField("vc", vc).Debug("No strategies matched for the visiting context")
		return nil, false
	}

	finalStrategy := m.pickStrategy(vc, strategies)
	if finalStrategy == nil {
		logrus.WithField("vc", vc).Debug("No strategy picked for the visiting context")
		return nil, false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	logrus.WithFields(logrus.Fields{
		"vc":       vc,
		"strategy": finalStrategy.Name,
	}).Debug("Throttler allocated for the visting context")

	t, ok := m.throttlers[finalStrategy.Name]
	return t, ok
}

func (m *Registry) GC(timeout time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, t := range m.throttlers {
		for _, v := range t.Limiters {
			v.GC(timeout)
		}
	}
}

func (m *Registry) gcPeriodically(interval time.Duration, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		m.GC(timeout)
	}
}

// matchStrategies gets all applicatable strategies for current visiting context
func (m *Registry) matchStrategies(vc *VistingContext) (res []*Strategy) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, s := range m.strategies {
		if s.applicable(vc) {
			res = append(res, s)
		}
	}

	return
}

// pickStrategy picks a final strategy among provided strategies for current visiting context
func (m *Registry) pickStrategy(vc *VistingContext, strategies []*Strategy) *Strategy {
	var sid uint32

	for _, s := range strategies {
		if !s.KeyMust() { // no key are necessary
			return s
		}

		if sid == 0 {
			// load from cache first
			if v, ok := m.keyCache.Get(vc.Key); ok {
				sid = v.(uint32)
				goto CHECK
			}

			// load from store
			k, err := m.keyLoader(vc.Key)
			if err != nil {
				logrus.WithField("limitKey", vc.Key).Error("Failed to load rate limit key")
				continue
			}

			if k != nil { // key found
				sid = k.SID
			}

			// add to cache
			m.keyCache.Add(vc.Key, sid)
		}

	CHECK:
		if sid > 0 && s.ID == sid { // strategy ID matched
			return s
		}
	}

	return nil
}
