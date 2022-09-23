package rate

import (
	"time"

	"github.com/sirupsen/logrus"
)

type Config struct {
	Strategies map[uint32]*Strategy // limit strategies
}

type KeyInfo struct {
	SID  uint32 // bound strategy ID
	Key  string // limit key
	Type int    // limit type
}

type KeysetFilter struct {
	SIDs   []uint32 // strategy IDs
	KeySet []string // limit keyset
	Limit  int      // result limit size (<= 0 means none)
}

// KeysetLoader limit keyset loader
type KeysetLoader func(filter *KeysetFilter) ([]*KeyInfo, error)

func (m *Registry) AutoReload(interval time.Duration, reloader func() (*Config, error), kloader KeysetLoader) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// init registry key loader
	m.initKeyLoader(kloader)

	// warm up limit key cache for better performance
	m.warmUpKeyCache(kloader)

	// load immediately at first
	if rconf, err := reloader(); err == nil {
		m.reloadOnce(rconf)
	}

	// load periodically
	for range ticker.C {
		rconf, err := reloader()
		if err != nil {
			logrus.WithError(err).Error("Failed to load rate limit configs")
			continue
		}

		m.reloadOnce(rconf)
	}
}

func (m *Registry) reloadOnce(rconf *Config) {
	if rconf == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// refresh rate limit strategies
	m.refreshStrategies(rconf.Strategies)
}

func (m *Registry) refreshStrategies(strategies map[uint32]*Strategy) {
	// remove limiter sets
	for sid, strategy := range m.strategies {
		if _, ok := strategies[sid]; !ok {
			m.removeStrategy(strategy)
			logrus.WithField("strategy", strategy).Info("RateLimit strategy removed")
		}
	}

	// add or update limiter sets
	for sid, strategy := range strategies {
		s, ok := m.strategies[sid]
		if !ok { // add
			m.addStrategy(strategy)
			logrus.WithField("strategy", strategy).Info("RateLimit strategy added")
			continue
		}

		if s.MD5 != strategy.MD5 { // update
			m.updateStrategy(strategy)
			logrus.WithField("strategy", strategy).Info("RateLimit strategy updated")
		}
	}
}

func (m *Registry) initKeyLoader(kloader KeysetLoader) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.keyLoader = func(key string) (*KeyInfo, error) {
		kinfos, err := kloader(&KeysetFilter{KeySet: []string{key}})
		if err == nil && len(kinfos) > 0 {
			return kinfos[0], nil
		}

		return nil, err
	}
}

func (m *Registry) warmUpKeyCache(kloader KeysetLoader) {
	kis, err := kloader(&KeysetFilter{Limit: (LimitKeyCacheSize * 3 / 4)})
	if err != nil {
		logrus.WithError(err).Warn("Failed to load limit keyset to warm up cache")
		return
	}

	for i := range kis {
		m.keyCache.Add(kis[i].Key, kis[i])
	}

	logrus.WithField("totalKeys", len(kis)).Info("Limit keyset loaded to cache")
}
