package rate

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/time/rate"
)

type LimitAlgoType string

const (
	// rate limit algorithms, only `fixed_window` and `token bucket` are supported for now.
	LimitAlgoFixedWindow LimitAlgoType = "fixed_window"
	LimitAlgoTokenBucket LimitAlgoType = "token_bucket"
)

type LimitType int

const (
	// rate limit types, only `ip` or `key` are supported for now.
	LimitTypeByKey LimitType = iota
	LimitTypeByIp
)

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
	tmpRules := make(map[string]*LimitRule)
	if err := json.Unmarshal(data, &tmpRules); err != nil {
		return errors.WithMessage(err, "malformed json format")
	}

	for resource, rule := range tmpRules {
		s.LimitOptions[resource] = rule.Option
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

// FixedWindowOption limit option for token bucket
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
