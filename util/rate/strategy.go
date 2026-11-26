package rate

import (
	"encoding/json"
	"fmt"
	"github.com/Conflux-Chain/confura/util"
	"math"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/time/rate"
)

type LimitAlgoType string

const (
	// rate limit algorithms, only `fixed_window` and `token_bucket` are supported for now.
	LimitAlgoFixedWindow LimitAlgoType = "fixed_window"
	LimitAlgoTokenBucket LimitAlgoType = "token_bucket"
)

type LimitType int

const (
	// rate limit types, only `ip` or `key` are supported for now.
	LimitTypeByKey LimitType = iota
	LimitTypeByIp
)

func (typ LimitType) String() string {
	switch typ {
	case LimitTypeByKey:
		return "by_key"
	case LimitTypeByIp:
		return "by_ip"
	default:
		return "unknown"
	}
}

const (
	// pre-defined default strategy name
	DefaultStrategy = "default"
)

// Strategy rate limit strategy
type Strategy struct {
	ID   uint32 // strategy ID
	Name string // strategy name

	LimitOptions map[string]interface{} // resource => limit option
}

func NewStrategy(id uint32, name string) *Strategy {
	return &Strategy{
		ID:           id,
		Name:         name,
		LimitOptions: make(map[string]interface{}),
	}
}

// UnmarshalJSON implements `json.Unmarshaler`
func (s *Strategy) UnmarshalJSON(data []byte) error {
	return s.DecodeRules(data, false)
}

// DecodeRules uses strict field validation for new administrative input. Stored
// configurations retain compatibility with extra fields, but never invalid values.
func (s *Strategy) DecodeRules(data []byte, strict bool) error {
	var rules map[string]json.RawMessage
	if err := util.DecodeJSONObject(data, &rules, strict); err != nil {
		return err
	}
	options := make(map[string]interface{}, len(rules))
	for resource, raw := range rules {
		var rule struct {
			Algo   LimitAlgoType
			Option json.RawMessage
		}
		if err := util.DecodeJSONObject(raw, &rule, strict); err != nil {
			return fmt.Errorf("rule %q: %w", resource, err)
		}
		var opt interface{}
		switch rule.Algo {
		case LimitAlgoFixedWindow:
			var value struct {
				Interval string
				Quota    int
			}
			if err := util.DecodeJSONObject(rule.Option, &value, strict); err != nil {
				return err
			}
			interval, err := time.ParseDuration(value.Interval)
			if err != nil {
				return err
			}
			opt = FixedWindowOption{Interval: interval, Quota: value.Quota}
		case LimitAlgoTokenBucket:
			var value TokenBucketOption
			if err := util.DecodeJSONObject(rule.Option, &value, strict); err != nil {
				return err
			}
			opt = value
		default:
			return errors.New("invalid rate limit algorithm")
		}
		if err := ValidateLimitOption(opt); err != nil {
			return fmt.Errorf("rule %q: %w", resource, err)
		}
		options[resource] = opt
	}
	s.LimitOptions = options
	return nil
}

// ValidateLimitOption does not construct a limiter or start background work.
func ValidateLimitOption(option interface{}) error {
	switch opt := option.(type) {
	case FixedWindowOption:
		if opt.Interval <= 0 || opt.Quota <= 0 {
			return errors.New("fixed window interval and quota must be positive")
		}
	case TokenBucketOption:
		if opt.Rate <= 0 || math.IsNaN(float64(opt.Rate)) || math.IsInf(float64(opt.Rate), 0) {
			return errors.New("token bucket rate must be positive and finite")
		}
		if opt.Burst <= 0 {
			return errors.New("token bucket burst must be positive")
		}
		if float64(opt.Burst)/float64(opt.Rate) >= float64(math.MaxInt64/int64(time.Second)) {
			return errors.New("token bucket refill interval is too large")
		}
	default:
		return errors.New("invalid limit option")
	}
	return nil
}

// FixedWindowOption limit option for fixed window
type FixedWindowOption struct {
	Interval time.Duration
	Quota    int
}

// UnmarshalJSON implements `json.Unmarshaler`
func (fwo *FixedWindowOption) UnmarshalJSON(data []byte) error {
	var tmp struct {
		Interval string
		Quota    int
	}

	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	fwo.Quota = tmp.Quota

	interval, err := time.ParseDuration(tmp.Interval)
	if err == nil {
		fwo.Interval = interval
	}

	return err
}

// TokenBucketOption limit option for token bucket
type TokenBucketOption struct {
	Rate  rate.Limit
	Burst int
}

func NewTokenBucketOption(r, b int) TokenBucketOption {
	return TokenBucketOption{
		Rate:  rate.Limit(r),
		Burst: b,
	}
}

// LimitRule resource limit rule
type LimitRule struct {
	Algo   LimitAlgoType
	Option interface{}
}

func (r *LimitRule) UnmarshalJSON(data []byte) (err error) {
	var tmp struct {
		Algo   LimitAlgoType
		Option json.RawMessage
	}

	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	r.Algo = tmp.Algo

	switch tmp.Algo {
	case LimitAlgoFixedWindow:
		var fwopt FixedWindowOption
		if err = json.Unmarshal(tmp.Option, &fwopt); err == nil {
			r.Option = fwopt
		}
	case LimitAlgoTokenBucket:
		var tbopt TokenBucketOption
		if err = json.Unmarshal(tmp.Option, &tbopt); err == nil {
			r.Option = tbopt
		}
	default:
		return errors.New("invalid rate limit algorithm")
	}

	return err
}
