package rate

import (
	"sort"
	"time"

	"github.com/sirupsen/logrus"
)

type Config struct {
	Strategies []*Strategy // limit strategies
}

type KeyInfo struct {
	SID uint32 // bound strategy ID
	Key string // limit key
}

type KeySetFilter struct {
	SIDs   []uint32 // strategy IDs
	KeySet []string // limit keyset
	Limit  int      // result limit size (<= 0 means none)
}

// KeySetLoader limit keyset loader
type KeySetLoader func(filter *KeySetFilter) ([]*KeyInfo, error)

func (m *Registry) AutoReload(interval time.Duration, reloader func() *Config, kloader KeySetLoader) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// init registry key loader
	m.keyLoader = func(key string) (*KeyInfo, error) {
		kinfos, err := kloader(&KeySetFilter{KeySet: []string{key}})
		if err == nil && len(kinfos) > 0 {
			return kinfos[0], nil
		}
		return nil, err
	}

	// TODO: warm up limit key cache for better performance

	// load immediately at first
	rconf := reloader()
	m.reloadOnce(rconf)

	// load periodically
	for range ticker.C {
		rconf := reloader()
		m.reloadOnce(rconf)

		// TODO: re-validate most recently used limit keys for refresh.
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

func (m *Registry) refreshStrategies(strategies []*Strategy) {
	// build strategy mapping
	strategyMap := make(map[string]*Strategy, len(strategies))
	for _, s := range strategies {
		strategyMap[s.Name] = s
	}

	// remove throttlers
	for name := range m.throttlers {
		if _, ok := strategyMap[name]; !ok {
			delete(m.throttlers, name)
			logrus.WithField("name", name).Info("RateLimit strategy removed")
		}
	}

	// add or update throttlers
	for name, strategy := range strategyMap {
		throttler, ok := m.throttlers[name]
		if !ok { // add
			m.throttlers[name] = NewThrottler(strategy)
			logrus.WithField("strategy", strategy).Info("RateLimit strategy added")
			continue
		}

		if throttler.Update(strategy) { // update
			logrus.WithField("strategy", strategy).Info("RateLimit strategy updated")
		}
	}

	// sort strategies by priority
	sort.SliceStable(strategies, func(i, j int) bool {
		return strategies[i].Priority > strategies[j].Priority
	})
	m.strategies = strategies
}
