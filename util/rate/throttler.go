package rate

import (
	"github.com/sirupsen/logrus"
)

type VistingContext struct {
	Ip       string // visiter IP
	Key      string // visiter key
	Resource string // visited resource (also used as limit rule)
}

type Throttler struct {
	*Strategy                            // the strategy this throttler uses
	Limiters  map[string]*VistingLimiter // limiters: limiter rule => VistingLimiter
}

func NewThrottler(s *Strategy) *Throttler {
	return &Throttler{
		Strategy: s,
		Limiters: make(map[string]*VistingLimiter),
	}
}

func (t *Throttler) Allow(vc *VistingContext, n int) bool {
	limiter, ok := t.Limiters[vc.Resource]
	if !ok {
		return true
	}

	if t.WouldLimitByIp() {
		return limiter.Allow(vc.Ip, n)
	}

	if t.WouldLimitByKey() {
		return limiter.Allow(vc.Key, n)
	}

	return false
}

func (t *Throttler) Update(strategy *Strategy) (updated bool) {
	if t.MD5 == strategy.MD5 { // not changed
		return false
	}

	olds := t.Strategy
	t.Strategy = strategy

	// limit type changed
	if olds.LimitType != strategy.LimitType {
		logrus.WithFields(logrus.Fields{
			"strategy": strategy.Name,
			"old":      olds.LimitType,
			"new":      strategy.LimitType,
		}).Info("Strategy type changed")
		t.Limiters = make(map[string]*VistingLimiter)
		return true
	}

	// API key length changed
	if olds.KeyLen != strategy.KeyLen {
		logrus.WithFields(logrus.Fields{
			"strategy": strategy.Name,
			"old":      olds.KeyLen,
			"new":      strategy.KeyLen,
		}).Info("Strategy key length changed")
		t.Limiters = make(map[string]*VistingLimiter)
		return true
	}

	// remove limit rules
	for name, option := range olds.Rules {
		if _, ok := strategy.Rules[name]; !ok {
			logrus.WithFields(logrus.Fields{
				"strategy": strategy.Name,
				"rule":     name,
				"option":   option,
			}).Info("Strategy rule removed")
			delete(t.Limiters, name)
			updated = true
		}
	}

	// add or update limit rules
	for name, newOption := range strategy.Rules {
		logger := logrus.WithFields(logrus.Fields{
			"strategy": strategy.Name,
			"rule":     name,
			"option":   newOption,
		})

		option, ok := olds.Rules[name]
		if !ok { // add
			logger.Info("Strategy rule added")
			t.Limiters[name] = NewVisitingLimiter(newOption.Rate, newOption.Burst)
			updated = true
			continue
		}

		if newOption != option && t.Limiters[name].Update(newOption) { // update
			logger.Info("Strategy rule updated")
			updated = true
		}
	}

	return
}
