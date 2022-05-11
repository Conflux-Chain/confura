package rate

import (
	"time"
)

func (m *Registry) AutoReload(interval time.Duration, reloader func() map[string]Option) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

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
		}
	}

	// add or update limiters
	for name, option := range all {
		if current, ok := m.limiters[name]; ok {
			current.Update(option)
		} else {
			m.limiters[name] = NewIpLimiter(option.Rate, option.Burst)
		}
	}
}
