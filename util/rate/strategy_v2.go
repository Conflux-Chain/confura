package rate

import (
	"crypto/md5"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/time/rate"
)

type LimitAlgoType uint8

const (
	// rate limit algorithms, only `time window` and `token bucket` are supported for now.
	LimitAlgoUnknown LimitAlgoType = iota
	LimitAlgoTimeWindow
	LimitAlgoTokenBucket
	LimitAlgoLastIndex
)

// Strategy rate limit strategy
type StrategyV2 struct {
	ID   uint32 // strategy ID
	Name string // strategy name

	RuleSets []LimitRuleSet // limit rule sets
	MD5      [md5.Size]byte `json:"-"` // config data fingerprint
}

type jsonUnmarshalUsedStrategy struct {
	RuleSets []struct {
		baseLimitRuleSet
		Rules json.RawMessage
	}
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (s *StrategyV2) UnmarshalJSON(data []byte) error {
	var tmp jsonUnmarshalUsedStrategy

	if err := json.Unmarshal(data, &tmp); err != nil {
		return errors.WithMessage(err, "malformed json for limit rule sets")
	}

	// unmarshal each rule set per to limit algorithm type
	for _, rs := range tmp.RuleSets {
		switch rs.Algo {
		case LimitAlgoTimeWindow:
			if err := s.unmarshalTimeWindowRuleSet(rs.Rules); err != nil {
				return err
			}
		case LimitAlgoTokenBucket:
			if err := s.unmarshalTokenBucketRuleSet(rs.Rules); err != nil {
				return err
			}
		default:
			return errors.New("invalid limit algorithm")
		}
	}

	s.MD5 = md5.Sum(data)
	return nil
}

func (s *StrategyV2) unmarshalTimeWindowRuleSet(data json.RawMessage) error {
	var valPairs []int64
	if err := json.Unmarshal(data, &valPairs); err != nil {
		return errors.WithMessage(err, "malformed json for time window limit option")
	}

	if len(valPairs) != 2 {
		return errors.New("invalid limit option (must be integer quota/interval pairs)")
	}

	twoption := TimeWindowOption{
		Quota:    valPairs[0],
		Interval: time.Duration(valPairs[1]) * time.Second,
	}

	s.RuleSets = append(s.RuleSets, NewTimeWindowRuleSet(twoption))
	return nil
}

func (s *StrategyV2) unmarshalTokenBucketRuleSet(data json.RawMessage) error {
	kvMaps := make(map[string][]int)
	if err := json.Unmarshal(data, &kvMaps); err != nil {
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

type LimitRuleSet interface {
	Algorithm() LimitAlgoType
}

type baseLimitRuleSet struct {
	Algo LimitAlgoType // used algorithm
}

func newBaseLimitRuleSet(algo LimitAlgoType) baseLimitRuleSet {
	return baseLimitRuleSet{Algo: algo}
}

func (rs baseLimitRuleSet) Algorithm() LimitAlgoType {
	return rs.Algo
}

type TimeWindowOption struct {
	Interval time.Duration
	Quota    int64
}

type TimeWindowRuleSet struct {
	baseLimitRuleSet
	Rules TimeWindowOption
}

func NewTimeWindowRuleSet(option TimeWindowOption) TimeWindowRuleSet {
	return TimeWindowRuleSet{
		baseLimitRuleSet: newBaseLimitRuleSet(LimitAlgoTimeWindow),
		Rules:            option,
	}
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
	baseLimitRuleSet
	Rules map[string]TokenBucketOption
}

func NewTokenBucketRuleSet(options map[string]TokenBucketOption) TokenBucketRuleSet {
	return TokenBucketRuleSet{
		baseLimitRuleSet: newBaseLimitRuleSet(LimitAlgoTokenBucket),
		Rules:            options,
	}
}
