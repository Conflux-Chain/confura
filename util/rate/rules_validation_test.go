package rate

import (
	"math"
	"testing"

	"github.com/Conflux-Chain/confura/util/acl"
	"github.com/stretchr/testify/require"
	limitrate "golang.org/x/time/rate"
)

func TestRuleValidation(t *testing.T) {
	for _, input := range []string{
		`null`, `{} {}`, `{"rpc":null}`, `{"rpc":{"algo":"token_bucket","option":null}}`,
		`{"rpc":{"algo":"token_bucket","option":{"rate":0,"burst":1}}}`,
		`{"rpc":{"algo":"token_bucket","option":{"rate":1,"burst":-1}}}`,
		`{"rpc":{"algo":"fixed_window","option":{"interval":"0s","quota":1}}}`,
		`{"rpc":{"algo":"fixed_window","option":{"interval":"999999999999h","quota":1}}}`,
		`{"rpc":{"algo":"fixed_window","option":{"interval":"1s","quota":0}}}`,
		`{"rpc":{"algo":"token_bucket","option":{"rate":1,"burst":1,"typo":2}}}`,
		`{"rpc":{"algo":"token_bucket","option":{"rate":1,"burst":1},"typo":2}}`,
	} {
		t.Run(input, func(t *testing.T) { require.Error(t, NewStrategy(1, "test").DecodeRules([]byte(input), true)) })
	}
	for _, input := range []string{`{}`, `{"rpc":{"algo":"token_bucket","option":{"rate":0.5,"burst":1}}}`, `{"rpc":{"algo":"fixed_window","option":{"interval":"24h","quota":100}}}`} {
		require.NoError(t, NewStrategy(1, "test").DecodeRules([]byte(input), true))
	}
	for _, value := range []float64{math.NaN(), math.Inf(1), -1, 1e-20} {
		require.Error(t, ValidateLimitOption(TokenBucketOption{Rate: limitrate.Limit(value), Burst: 1}))
	}
}

func TestInvalidReloadDoesNotReplaceEitherPolicySet(t *testing.T) {
	r := NewRegistry(nil, acl.NewEthValidator)
	valid := &Config{Strategies: map[uint32]*Strategy{1: NewStrategy(1, "old")}, AllowLists: map[uint32]*acl.AllowList{2: acl.NewAllowList(2, "old")}}
	require.NoError(t, r.reloadOnce(valid, &ConfigCheckSums{}))
	bad := &Config{AllowLists: map[uint32]*acl.AllowList{3: {ID: 3, AllowMethods: []string{"eth_*"}, DisallowMethods: []string{"eth_call"}}}}
	require.Error(t, r.reloadOnce(bad, &ConfigCheckSums{}))
	require.Equal(t, "old", r.Strategies()[0].Name)
	require.Contains(t, r.allowlists, uint32(2))
	require.NotContains(t, r.allowlists, uint32(3))
}
