package rate

import (
	"crypto/md5"
	"encoding/json"
	"regexp"

	"github.com/pkg/errors"
)

const (
	LimitByIp = iota
	LimitByKey
)

//  Strategy rate limit strategy
type Strategy struct {
	ID        uint32            // strategy ID
	Name      string            // strategy name
	LimitType int               // limit type, by IP or key
	KeyLen    int               // key length <= 128
	Priority  int               // priority weight
	Rules     map[string]Option // limit rules: rule name => rule option
	Regex     *regexp.Regexp    `json:"-"` // key regex pattern
	MD5       [md5.Size]byte    `json:"-"` // config data fingerprint
}

type _strategy Strategy
type jsonUnmarshalStrategy struct {
	_strategy
	Rules map[string][]int
	Regex string
}

func (s *Strategy) UnmarshalJSON(input []byte) error {
	var _s jsonUnmarshalStrategy

	if err := json.Unmarshal(input, &_s); err != nil {
		return err
	}

	if _s.LimitType == LimitByKey && _s.KeyLen <= 0 {
		return errors.New("keylen must be positive for limit key type")
	}

	if _s.KeyLen > 128 {
		return errors.New("keylen must be at most 128")
	}

	*s = (Strategy)(_s._strategy)

	s.Rules = make(map[string]Option)
	for name, value := range _s.Rules {
		if len(value) != 2 {
			return errors.New("limit option must be rate/burst integer pairs")
		}

		s.Rules[name] = NewOption(value[0], value[1])
	}

	if len(_s.Regex) != 0 {
		regexp, err := regexp.Compile(_s.Regex)
		if err != nil {
			return errors.WithMessage(err, "invalid regexp pattern")
		}

		s.Regex = regexp
	}

	return nil
}

func (s *Strategy) KeyMust() bool {
	return s.KeyLen > 0
}

func (s *Strategy) ValidateKey(key string) bool {
	if len(key) != s.KeyLen {
		return false
	}

	if s.Regex != nil && !s.Regex.MatchString(key) {
		return false
	}

	return true
}

func (s *Strategy) WouldLimitByIp() bool {
	return s.LimitType == LimitByIp
}

func (s *Strategy) WouldLimitByKey() bool {
	return s.LimitType == LimitByKey
}

func (s *Strategy) applicable(vc *VistingContext) bool {
	if s.WouldLimitByIp() && len(vc.Ip) == 0 {
		return false
	}

	if !s.KeyMust() {
		return true
	}

	return s.ValidateKey(vc.Key)
}
