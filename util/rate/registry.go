package rate

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura/util"
	"github.com/sirupsen/logrus"
)

const (
	DefaultStrategy       = "default"
	LimitKeyCacheSize     = 5000
	LimitKeyExpirationTTL = 75 * time.Second
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

	// available strategies: ID => *Strategy
	strategies map[uint32]*Strategy

	// limit key cache: limit key => *KeyInfo (nil if missing)
	keyCache *util.ExpirableLruCache
	// loader to retrieve keyset from store
	keyLoader func(key string) (*KeyInfo, error)

	// default IP limiter set
	defaultLimiterSet *IpLimiterSet
	// key based IP limiter sets: strategy ID => *KeyBasedIpLimiterSet
	kbIpLimiterSets map[uint32]*KeyBasedIpLimiterSet
	// key limiter sets: strategy ID => *KeyLimiterSet
	keyLimiterSets map[uint32]*KeyLimiterSet
}

func NewRegistry() *Registry {
	cache := util.NewExpirableLruCache(LimitKeyCacheSize, LimitKeyExpirationTTL)
	return &Registry{
		keyCache:        cache,
		strategies:      make(map[uint32]*Strategy),
		kbIpLimiterSets: make(map[uint32]*KeyBasedIpLimiterSet),
		keyLimiterSets:  make(map[uint32]*KeyLimiterSet),
	}
}

func (m *Registry) Get(vc *VisitContext) (Limiter, bool) {
	if len(vc.Key) == 0 { // no limit key provided?
		logrus.WithField("visitContext", vc).
			Debug("Use default limiter due to no limit key provided")

		return m.getDefaultLimiter(vc)
	}

	if vc.Status != nil && vc.Status.Tier != VipTierNone { // VIP access
		return m.getVipLimiter(vc)
	}

	ki, ok := m.loadKeyInfo(vc.Key)
	if !ok || ki == nil { // limit key not loaded or missing
		logrus.WithFields(logrus.Fields{
			"visitContext": vc,
			"keyInfo":      ki,
		}).Debug("Use default limiter due to limit key loaded failure or missing")

		return m.getDefaultLimiter(vc)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// load limiter set by key info from where limiter is looked up
	// for current visit context
	if ls, ok := m.getLimiterSetByKeyInfo(ki); ok {
		logrus.WithFields(logrus.Fields{
			"visitContext":   vc,
			"limiterSetType": fmt.Sprintf("%T", ls),
			"keyInfo":        ki,
		}).Debug("Use limiter from some strategy limiter set")

		return ls.Get(vc)
	}

	logrus.WithFields(logrus.Fields{
		"visitContext": vc,
		"keyInfo":      ki,
	}).Debug("Use default limiter due to not limiter set avaliable")

	return m.getDefaultLimiter(vc, true)
}

func (m *Registry) GC(timeout time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.defaultLimiterSet != nil {
		m.defaultLimiterSet.GC(timeout)
	}

	for _, ls := range m.kbIpLimiterSets {
		ls.GC(timeout)
	}

	for _, ls := range m.keyLimiterSets {
		ls.GC(timeout)
	}
}

// gcPeriodically peridically garbage collecting stale limiters
func (m *Registry) gcPeriodically(interval time.Duration, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		m.GC(timeout)
	}
}

func (m *Registry) getVipLimiter(vc *VisitContext) (Limiter, bool) {
	logger := logrus.WithFields(logrus.Fields{
		"visitContext": vc,
		"vipStatus":    vc.Status,
	})

	strategy := m.getVipStrategy(vc.Status.Tier)
	if strategy == nil {
		logger.Info("No VIP strategy available")
		return nil, false
	}

	logger = logger.WithField("strategy", strategy.Name)
	ki := &KeyInfo{
		SID: strategy.ID, Key: vc.Key, Type: LimitTypeByKey,
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// load limiter set by key info from where limiter is looked up
	// for current visit context
	if ls, ok := m.getLimiterSetByKeyInfo(ki); ok {
		logger.WithField("limiterSetType", fmt.Sprintf("%T", ls)).
			Debug("Use limiter from VIP strategy limiter set")
		return ls.Get(vc)
	}

	logger.Info("No VIP limiter available")
	return nil, false
}

func (m *Registry) getVipStrategy(tier VipTier) *Strategy {
	vipStrategy := GetVipStrategyByTier(tier)

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, s := range m.strategies {
		if strings.EqualFold(s.Name, vipStrategy) {
			return s
		}
	}

	return nil
}

// getLimiterSet gets limiter set for the specified strategy and limiter type
func (m *Registry) getLimiterSetByKeyInfo(ki *KeyInfo) (LimiterSet, bool) {
	if ki.Type == LimitTypeByIp {
		ls, ok := m.kbIpLimiterSets[ki.SID]
		return ls, ok
	}

	if ki.Type == LimitTypeByKey {
		ls, ok := m.keyLimiterSets[ki.SID]
		return ls, ok
	}

	return nil, false
}

// getDefaultLimiter returns the default limiter for current visit context
func (m *Registry) getDefaultLimiter(vc *VisitContext, lockfree ...bool) (Limiter, bool) {
	if len(lockfree) == 0 || !lockfree[0] {
		m.mu.Lock()
		defer m.mu.Unlock()
	}

	if m.defaultLimiterSet != nil {
		l, ok := m.defaultLimiterSet.Get(vc)
		return l, ok
	}

	return nil, false
}

// loadKeyInfo loads limit key info from cache or store
func (m *Registry) loadKeyInfo(key string) (*KeyInfo, bool) {
	// load from cache first
	if cv, ok := m.keyCache.Get(key); ok {
		// found in cache
		return cv.(*KeyInfo), true
	}

	if m.keyLoader == nil {
		return nil, false
	}

	// load from store if not found in cache
	ki, err := m.keyLoader(key)
	if err != nil {
		logrus.WithError(err).Error("Failed to load limit key info")
		return nil, false
	}

	// cache limit key
	m.keyCache.Add(key, ki)
	return ki, true
}

// Strategy Management

func (m *Registry) removeStrategy(s *Strategy) {
	if s.Name == DefaultStrategy {
		m.defaultLimiterSet = nil
		logrus.Info("Default IP limiter set unmounted")
	}

	delete(m.strategies, s.ID)
	delete(m.kbIpLimiterSets, s.ID)
	delete(m.keyLimiterSets, s.ID)
}

func (m *Registry) addStrategy(s *Strategy) {
	if s.Name == DefaultStrategy {
		m.defaultLimiterSet = NewIpLimiterSet(s)
		logrus.WithField("strategy", s).Info("Default IP limiter set mounted")
	}

	m.strategies[s.ID] = s
	m.kbIpLimiterSets[s.ID] = NewKeyBasedIpLimiterSet(s)
	m.keyLimiterSets[s.ID] = NewKeyLimiterSet(s)
}

func (m *Registry) updateStrategy(s *Strategy) {
	if s.Name == DefaultStrategy {
		m.defaultLimiterSet.Update(s)
		logrus.WithField("strategy", s).Info("Default IP limiter set updated")
	}

	m.strategies[s.ID] = s
	m.kbIpLimiterSets[s.ID].Update(s)
	m.keyLimiterSets[s.ID].Update(s)
}
