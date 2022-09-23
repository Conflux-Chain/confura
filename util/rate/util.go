package rate

import (
	"encoding/json"

	"github.com/pkg/errors"
)

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
