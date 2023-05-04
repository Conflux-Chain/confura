package rate

import (
	"crypto/md5"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Conflux-Chain/confura/util/acl"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/sirupsen/logrus"
)

type Config struct {
	// checksums for modification detection
	CheckSums ConfigCheckSums

	Strategies map[uint32]*Strategy      // limit strategies
	AllowLists map[uint32]*acl.AllowList // allow lists
}

// ConfigCheckSums config md5 checksum
type ConfigCheckSums struct {
	Strategies map[uint32][md5.Size]byte
	AllowLists map[uint32][md5.Size]byte
}

func (m *Registry) AutoReload(interval time.Duration, reloader func() (*Config, error)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// last config finger prints
	var cs ConfigCheckSums

	// load immediately at first
	if rconf, err := reloader(); err == nil {
		m.reloadOnce(rconf, &cs)
		cs = rconf.CheckSums
	}

	// load periodically
	for range ticker.C {
		rconf, err := reloader()
		if err != nil {
			logrus.WithError(err).Error("Failed to load rate limit configs")
			continue
		}

		m.reloadOnce(rconf, &cs)
		cs = rconf.CheckSums
	}
}

func (m *Registry) reloadOnce(rc *Config, lastCs *ConfigCheckSums) {
	if rc != nil {
		m.reloadRateLimitStrategies(rc, lastCs)
		m.reloadAclAllowLists(rc, lastCs)
	}
}

func (m *Registry) reloadRateLimitStrategies(rc *Config, lastCs *ConfigCheckSums) {
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

		if lastCs.Strategies[sid] != rc.CheckSums.Strategies[sid] { // update
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
