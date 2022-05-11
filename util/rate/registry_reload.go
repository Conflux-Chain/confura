package rate

import (
	"time"

	"github.com/sirupsen/logrus"
)

func (m *Registry) AutoReload(interval time.Duration, reloader func() map[string]Option) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// load immediately at first
	all := reloader()
	m.reloadOnce(all)

	// load periodically
	for range ticker.C {
		all := reloader()
		m.reloadOnce(all)
	}
}

func (m *Registry) reloadOnce(all map[string]Option) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// remove limiters
	for name := range m.limiters {
		if _, ok := all[name]; !ok {
			delete(m.limiters, name)
			logrus.WithField("name", name).Info("IpRateLimiter removed")
		}
	}

	// add or update limiters
	for name, option := range all {
		if current, ok := m.limiters[name]; ok {
			if current.Update(option) {
				logrus.WithFields(logrus.Fields{
					"name":  name,
					"rate":  option.Rate,
					"burst": option.Burst,
				}).Info("IpRateLimiter updated")
			}
		} else {
			m.limiters[name] = NewIpLimiter(option.Rate, option.Burst)
			logrus.WithFields(logrus.Fields{
				"name":  name,
				"rate":  option.Rate,
				"burst": option.Burst,
			}).Info("IpRateLimiter added")
		}
	}
}
