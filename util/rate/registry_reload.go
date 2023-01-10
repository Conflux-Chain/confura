package rate

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/sirupsen/logrus"
)

type Config struct {
	Strategies map[uint32]*Strategy // limit strategies
}

func (m *Registry) AutoReload(interval time.Duration, reloader func() (*Config, error)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

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

func (m *Registry) reloadOnce(rc *Config) {
	if rc == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// remove strategy
	for sid, strategy := range m.id2Strategies {
		if _, ok := rc.Strategies[sid]; !ok {
			m.removeStrategy(strategy)
			logrus.WithField("strategy", strategy).Info("RateLimit strategy removed")
		}
	}

	// add or update strategy
	for sid, strategy := range rc.Strategies {
		s, ok := m.id2Strategies[sid]
		if !ok { // add
			m.addStrategy(strategy)
			logrus.WithField("strategy", strategy).Info("RateLimit strategy added")
			continue
		}

		if s.MD5 != strategy.MD5 { // update
			m.updateStrategy(s, strategy)
			logrus.WithField("strategy", strategy).Info("RateLimit strategy updated")
		}
	}
}

func (m *Registry) getVipStrategy(tier handlers.VipTier) (*Strategy, bool) {
	// assemble vip strategy name by tier
	vipStrategy := fmt.Sprintf("vip%d", tier)

	for _, s := range m.strategies {
		if strings.EqualFold(s.Name, vipStrategy) {
			return s, true
		}
	}

	return nil, false
}

func (m *Registry) removeStrategy(s *Strategy) {
	// remove all limiters under this strategy
	for resource := range s.LimitOptions {
		m.Remove(resource, s.Name)
		logrus.WithField("resource", resource).Info("RateLimit rule deleted")
	}

	delete(m.strategies, s.Name)
	delete(m.id2Strategies, s.ID)
}

func (m *Registry) addStrategy(s *Strategy) {
	m.strategies[s.Name] = s
	m.id2Strategies[s.ID] = s
}

func (m *Registry) updateStrategy(old, new *Strategy) {
	m.strategies[new.Name] = new
	m.id2Strategies[new.ID] = new

	if old.Name != new.Name {
		// strategy name changed? this shall be rare, but we also need to
		// delete all limiters with the groups of old strategy name.
		delete(m.strategies, old.Name)
		for resource := range old.LimitOptions {
			m.Remove(resource, old.Name)
			logrus.WithField("resource", resource).Info("RateLimit rule deleted")
		}

		return
	}

	// check the changes from old to new limit rule, and delete all old limiters
	// with resource whose limit rule has been altered.
	for resource, oldopt := range old.LimitOptions {
		newopt, ok := new.LimitOptions[resource]
		if !ok || !reflect.DeepEqual(oldopt, newopt) {
			m.Remove(resource, old.Name)
			logrus.WithField("resource", resource).Info("RateLimit rule deleted")
		}
	}
}
