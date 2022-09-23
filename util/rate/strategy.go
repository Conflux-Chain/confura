package rate

import (
	"crypto/md5"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	// limit types
	LimitTypeByKey = iota
	LimitTypeByIp

	ConfigStrategyPrefix = "ratelimit.strategy."
)

//  Strategy rate limit strategy
type Strategy struct {
	ID    uint32            // strategy ID
	Name  string            // strategy name
	Rules map[string]Option // limit rules: rule name => rule option

	MD5 [md5.Size]byte `json:"-"` // config data fingerprint
}

// LimiterSet limiter set assembled by strategy
type LimiterSet interface {
	Get(vc *VisitContext) (Limiter, bool)
	GC(timeout time.Duration)
	Update(s *Strategy)
}

// limiterCreator limiter factory method
type limiterCreator func(option Option) Limiter

type baseLimiterSet struct {
	*Strategy // used strategy

	uid      string             // unique identifier
	lcreator limiterCreator     // limiter factory
	limiters map[string]Limiter // limit rule => Limiter
}

func newBaseLimiterSet(uid string, s *Strategy, lcreator limiterCreator) *baseLimiterSet {
	limiters := make(map[string]Limiter, len(s.Rules))

	for name, option := range s.Rules {
		limiters[name] = lcreator(option)
	}

	return &baseLimiterSet{
		Strategy: s, uid: uid, lcreator: lcreator, limiters: limiters,
	}
}

// Get returns limiter for current visit context
func (ls *baseLimiterSet) Get(vc *VisitContext) (Limiter, bool) {
	l, ok := ls.limiters[vc.Resource]
	return l, ok
}

// GC garbage collects limiter stale resources
func (ls *baseLimiterSet) GC(timeout time.Duration) {
	for _, l := range ls.limiters {
		l.GC(timeout)
	}
}

// Update updates with new strategy
func (ls *baseLimiterSet) Update(s *Strategy) {
	// remove limit rules
	for name, option := range ls.Rules {
		if _, ok := s.Rules[name]; !ok {
			logrus.WithFields(logrus.Fields{
				"limiterSetUid": ls.uid,
				"rule":          name,
				"option":        option,
			}).Info("Strategy rule removed")

			delete(ls.limiters, name)
		}
	}

	// add or update limit rules
	for name, newOption := range s.Rules {
		logger := logrus.WithFields(logrus.Fields{
			"limiterSetUid": ls.uid,
			"rule":          name,
			"option":        newOption,
		})

		option, ok := ls.Rules[name]
		if !ok { // add
			logger.Info("Strategy rule added")

			ls.limiters[name] = ls.lcreator(newOption)
			continue
		}

		if newOption == option { // no change
			continue
		}

		if l, ok := ls.limiters[name]; ok { // update
			logger.Info("Strategy rule updated")

			l.Update(newOption)
		}
	}
}

// IpLimiterSet limiting by IP address
type IpLimiterSet struct {
	*baseLimiterSet
}

func NewIpLimiterSet(s *Strategy) *IpLimiterSet {
	uid := fmt.Sprintf("IpLimiterSet-%s", s.Name)
	base := newBaseLimiterSet(uid, s, func(option Option) Limiter {
		return NewIpLimiter(option)
	})

	return &IpLimiterSet{baseLimiterSet: base}
}

// KeyLimiterSet limiting by key
type KeyLimiterSet struct {
	*baseLimiterSet
}

func NewKeyLimiterSet(s *Strategy) *KeyLimiterSet {
	uid := fmt.Sprintf("KeyLimiterSet-%s", s.Name)
	base := newBaseLimiterSet(uid, s, func(option Option) Limiter {
		return NewKeyLimiter(option)
	})

	return &KeyLimiterSet{baseLimiterSet: base}
}

// KeyBasedIpLimiterSet limiting by IP address yet grouped by limit key
type KeyBasedIpLimiterSet struct {
	*baseLimiterSet
}

func NewKeyBasedIpLimiterSet(s *Strategy) *KeyBasedIpLimiterSet {
	uid := fmt.Sprintf("KeyBasedIpLimiterSet-%s", s.Name)
	base := newBaseLimiterSet(uid, s, func(option Option) Limiter {
		return newkeyBasedIpLimiter(option)
	})

	return &KeyBasedIpLimiterSet{baseLimiterSet: base}
}

// keyBasedIpLimiter limits visits per IP address grouped by limit key
type keyBasedIpLimiter struct {
	Option

	// limit key => *IpLimiter
	limiters map[string]*IpLimiter
}

func newkeyBasedIpLimiter(opt Option) *keyBasedIpLimiter {
	return &keyBasedIpLimiter{
		Option: opt, limiters: make(map[string]*IpLimiter),
	}
}

func (ls *keyBasedIpLimiter) Allow(vc *VisitContext, n int) bool {
	l, ok := ls.limiters[vc.Key]
	if !ok {
		l = NewIpLimiter(ls.Option)
		ls.limiters[vc.Key] = l
	}

	return l.Allow(vc, n)
}

func (ls *keyBasedIpLimiter) GC(timeout time.Duration) {
	for _, l := range ls.limiters {
		l.GC(timeout)
	}
}

func (ls *keyBasedIpLimiter) Update(opt Option) bool {
	if ls.Rate == opt.Rate && ls.Burst == opt.Burst {
		return false
	}

	for _, l := range ls.limiters {
		l.Update(opt)
	}

	ls.Option = opt
	return true
}
