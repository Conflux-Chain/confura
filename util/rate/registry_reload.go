package rate

import (
	"time"

	"github.com/sirupsen/logrus"
)

type Config struct {
	IpLimitOpts map[string]Option   // ip rate limit configs
	WhiteList   map[string]struct{} // white list
}

func (m *Registry) AutoReload(interval time.Duration, reloader func() *Config) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// load immediately at first
	rconf := reloader()
	m.reloadOnce(rconf)

	// load periodically
	for range ticker.C {
		rconf := reloader()
		m.reloadOnce(rconf)
	}
}

func (m *Registry) reloadOnce(rconf *Config) {
	if rconf == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// refresh whitelist
	m.refreshWhiteList(rconf.WhiteList)
	// refresh ip limit options
	m.refreshIpLimitOpts(rconf.IpLimitOpts)
}

func (m *Registry) refreshWhiteList(all map[string]struct{}) {
	// remove from white list
	for name := range m.whilteList {
		if _, ok := all[name]; !ok {
			delete(m.whilteList, name)
			logrus.WithField("name", name).Info("RateLimit white list item removed")
		}
	}

	// add to white list
	for name := range all {
		if _, ok := m.whilteList[name]; !ok {
			m.whilteList[name] = struct{}{}
			logrus.WithField("name", name).Info("RateLimit white list item added")
		}
	}
}

func (m *Registry) refreshIpLimitOpts(all map[string]Option) {
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
