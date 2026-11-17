package acl

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDecodeRulesIdentity(t *testing.T) {
	for _, input := range []string{`null`, `{} {}`, `{"Name":"other"}`, `{"ID":99}`, `{"Origns":[]}`, `{"AllowMethods":["eth_*"],"DisallowMethods":["eth_call"]}`} {
		require.Error(t, NewAllowList(7, "database").DecodeRules([]byte(input), true))
	}
	al := NewAllowList(7, "database")
	require.NoError(t, al.DecodeRules([]byte(`{"Name":"other","ID":99,"Origins":["https://example.com"]}`), false))
	require.EqualValues(t, 7, al.ID)
	require.Equal(t, "database", al.Name)
	require.Equal(t, []string{"https://example.com"}, al.Origins)
	require.NoError(t, al.DecodeRules([]byte(`{}`), true))
}
