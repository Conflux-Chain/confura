package rate

import (
	"encoding/json"
	"math/rand"
	"time"

	"github.com/btcsuite/btcutil/base58"
	"github.com/pkg/errors"
)

const (
	LimitKeyLength = 32
)

func GenerateRandomLimitKey(limitType int) (string, error) {
	data := make([]byte, LimitKeyLength)

	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)
	r.Read(data)

	if limitType != LimitTypeByKey && limitType != LimitTypeByIp {
		return "", errors.New("invalid limit type")
	}

	secretb := []byte(base58.Encode(data))
	secretb[0] = '0' + uint8(limitType)

	return string(secretb), nil
}

func JsonUnmarshalStrategyRules(data []byte) (map[string]Option, error) {
	ruleMap := make(map[string][]int)
	if err := json.Unmarshal(data, &ruleMap); err != nil {
		return nil, errors.WithMessage(err, "malformed json string")
	}

	rules := make(map[string]Option)
	for name, value := range ruleMap {
		if len(value) != 2 {
			return nil, errors.New("invalid limit option (must be rate/burst integer pairs)")
		}

		rules[name] = NewOption(value[0], value[1])
	}

	return rules, nil
}
