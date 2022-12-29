package rate

import (
	"crypto/md5"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/time/rate"
)

type LimitAlgoType string

const (
	// rate limit algorithms, only `time window` and `token bucket` are supported for now.
	LimitAlgoTimeWindow  LimitAlgoType = "time_window"
	LimitAlgoTokenBucket LimitAlgoType = "token_bucket"
)

// Strategy rate limit strategy
type StrategyV2 struct {
	ID   uint32 // strategy ID
	Name string // strategy name

	// limit rule sets of type `TimeWindowRuleSet` or `TokenBucketRuleSet`
	RuleSets []interface{}

	MD5 [md5.Size]byte `json:"-"` // config data fingerprint
}

type jsonUnmarshalUsedStrategy struct {
	RuleSets []struct {
		Algo  LimitAlgoType
		Rules json.RawMessage
	}
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (s *StrategyV2) UnmarshalJSON(data []byte) error {
	var tmp jsonUnmarshalUsedStrategy

	if err := json.Unmarshal(data, &tmp); err != nil {
		return errors.WithMessage(err, "malformed json for limit rule sets")
	}

	// append each rule set per to limit algorithm type
	for _, rs := range tmp.RuleSets {
		switch rs.Algo {
		case LimitAlgoTimeWindow:
			if err := s.appendTimeWindowRuleSet(rs.Rules); err != nil {
				return errors.WithMessage(err, "invalid time window rule sets")
			}
		case LimitAlgoTokenBucket:
			if err := s.appendTokenBucketRuleSet(rs.Rules); err != nil {
				return errors.WithMessage(err, "invalid token bucket rule sets")
			}
		default:
			return errors.New("invalid limit algorithm")
		}
	}

	return nil
}

func (s *StrategyV2) appendTimeWindowRuleSet(rawJson json.RawMessage) error {
	kvMaps := make(map[string][]int64)
	if err := json.Unmarshal(rawJson, &kvMaps); err != nil {
		return errors.WithMessage(err, "malformed json for time window limit option")
	}

	twoptions := make(map[string]TimeWindowOption)
	for name, value := range kvMaps {
		if len(value) != 2 {
			return errors.New("invalid limit option (must be quota/interval pairs)")
		}

		twoptions[name] = TimeWindowOption{
			Quota:    value[0],
			Interval: time.Duration(value[1]) * time.Second,
		}
	}

	s.RuleSets = append(s.RuleSets, NewTimeWindowRuleSet(twoptions))
	return nil
}

func (s *StrategyV2) appendTokenBucketRuleSet(rawJson json.RawMessage) error {
	kvMaps := make(map[string][]int)
	if err := json.Unmarshal(rawJson, &kvMaps); err != nil {
		return errors.WithMessage(err, "malformed json for token bucket limit option")
	}

	tboptions := make(map[string]TokenBucketOption)
	for name, value := range kvMaps {
		if len(value) != 2 {
			return errors.New("invalid limit option (must be rate/burst pairs)")
		}

		tboptions[name] = NewTokenBucketOption(value[0], value[1])
	}

	s.RuleSets = append(s.RuleSets, NewTokenBucketRuleSet(tboptions))
	return nil
}

type TimeWindowOption struct {
	Interval time.Duration
	Quota    int64
}

type TimeWindowRuleSet struct {
	Rules map[string]TimeWindowOption // resource => limit rule
}

func NewTimeWindowRuleSet(options map[string]TimeWindowOption) TimeWindowRuleSet {
	return TimeWindowRuleSet{Rules: options}
}

type TokenBucketOption struct {
	Rate  rate.Limit
	Burst int
}

func NewTokenBucketOption(r int, b int) TokenBucketOption {
	return TokenBucketOption{
		Rate:  rate.Limit(r),
		Burst: b,
	}
}

type TokenBucketRuleSet struct {
	Rules map[string]TokenBucketOption // resource => limit rule
}

func NewTokenBucketRuleSet(options map[string]TokenBucketOption) TokenBucketRuleSet {
	return TokenBucketRuleSet{Rules: options}
}
