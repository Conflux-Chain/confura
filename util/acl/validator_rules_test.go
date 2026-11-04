package acl

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/stretchr/testify/require"
)

func TestValidatorRuleSemantics(t *testing.T) {
	for _, tc := range []struct {
		name, origin, method string
		list                 AllowList
		want                 error
	}{
		{name: "empty rules", method: "eth_getLogs"},
		{name: "wildcards", list: AllowList{Origins: []string{"HTTPS://*.EXAMPLE.COM"}, AllowMethods: []string{"eth_*"}}, origin: "https://app.example.com", method: "eth_getLogs"},
		{name: "literal origin", list: AllowList{Origins: []string{"https://app.example.com"}}, origin: "https://appXexample.com", want: errInvalidOrigin},
		{name: "literal method", list: AllowList{AllowMethods: []string{"eth_read.v1"}}, method: "eth_readXv1", want: errBadRpcMethod},
		{name: "regexp characters", list: AllowList{AllowMethods: []string{"eth_[read]+(v1)?$"}}, method: "eth_[read]+(v1)?$"},
		{name: "case sensitive method", list: AllowList{AllowMethods: []string{"eth_*"}}, method: "ETH_getLogs", want: errBadRpcMethod},
		{name: "missing origin", list: AllowList{Origins: []string{"*"}}, want: errInvalidOrigin},
		{name: "allow takes priority", list: AllowList{AllowMethods: []string{"eth_getLogs"}, DisallowMethods: []string{"eth_*"}}, method: "eth_getLogs"},
		{name: "allow miss denies", list: AllowList{AllowMethods: []string{"eth_getLogs"}, DisallowMethods: []string{"cfx_*"}}, method: "eth_call", want: errBadRpcMethod},
		{name: "disallow hit", list: AllowList{DisallowMethods: []string{"eth_*"}}, method: "eth_getLogs", want: errBadRpcMethod},
		{name: "disallow miss", list: AllowList{DisallowMethods: []string{"cfx_*"}}, method: "eth_getLogs"},
		{name: "invalid allow denies", list: AllowList{AllowMethods: []string{"eth_\xff"}}, method: "eth_getLogs", want: errBadRpcMethod},
		{name: "invalid disallow ignored", list: AllowList{DisallowMethods: []string{"eth_\xff"}}, method: "eth_getLogs"},
		{name: "invalid followed by valid allow", list: AllowList{AllowMethods: []string{"eth_\xff", "eth_*"}}, method: "eth_getLogs"},
		{name: "invalid followed by valid disallow", list: AllowList{DisallowMethods: []string{"eth_\xff", "eth_*"}}, method: "eth_getLogs", want: errBadRpcMethod},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.origin != "" {
				ctx = context.WithValue(ctx, handlers.CtxKeyReqOrigin, tc.origin)
			}
			err := NewEthValidator(&tc.list).Validate(Context{Context: ctx, RpcMethod: tc.method})
			require.ErrorIs(t, err, tc.want)
		})
	}
}

// Compare decisions with the prior per-request regexp path, including invalid
// UTF-8 and empty-string patterns, whose behavior must not change on compile.
func TestValidatorRulesMatchOriginalDecisions(t *testing.T) {
	ruleSets := [][]string{nil, {""}, {"*"}, {"eth_*", "cfx_*"}, {"eth_\xff"}, {"eth_\xff", "eth_*"}, {"https://*.example.com"}, {"https://\xff.example.com"}, {"[a]+(b)?."}}
	inputs := []string{"", "eth_getLogs", "ETH_getLogs", "eth_\xff", "cfx_call", "https://app.example.com", "https://\xff.example.com", "[a]+(b)?."}
	matches := func(rules []string, input string, lower bool) bool {
		for _, rule := range rules {
			if lower {
				rule, input = strings.ToLower(rule), strings.ToLower(input)
			}
			if ok, err := regexp.MatchString(util.WildCardToRegexp(rule), input); err == nil && ok {
				return true
			}
		}
		return false
	}
	for _, rules := range ruleSets {
		for _, input := range inputs {
			ctx := Context{Context: context.WithValue(context.Background(), handlers.CtxKeyReqOrigin, input), RpcMethod: input}
			for _, kind := range []string{"origin", "allow", "disallow"} {
				al := &AllowList{}
				allowed := true
				switch kind {
				case "origin":
					al.Origins = rules
					allowed = len(rules) == 0 || matches(rules, input, true)
				case "allow":
					al.AllowMethods = rules
					allowed = len(rules) == 0 || matches(rules, input, false)
				case "disallow":
					al.DisallowMethods = rules
					allowed = !matches(rules, input, false)
				}
				require.Equal(t, allowed, NewEthValidator(al).Validate(ctx) == nil, "%s rules=%q input=%q", kind, rules, input)
			}
		}
	}
}

func TestValidatorConcurrentRuleReads(t *testing.T) {
	v := NewEthValidator(&AllowList{Origins: []string{"https://*.example.com"}, AllowMethods: []string{"eth_*"}})
	ctx := Context{Context: context.WithValue(context.Background(), handlers.CtxKeyReqOrigin, "https://app.example.com"), RpcMethod: "eth_getLogs"}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 32; j++ {
				if err := v.Validate(ctx); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkACLValidation(b *testing.B) {
	for _, count := range []int{1, 10} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			al := &AllowList{}
			for i := 0; i < count; i++ {
				al.Origins = append(al.Origins, fmt.Sprintf("https://app%d.*.example.com", i))
				al.AllowMethods = append(al.AllowMethods, fmt.Sprintf("fixture%d_*", i))
			}
			v := NewEthValidator(al)
			ctx := Context{
				Context:   context.WithValue(context.Background(), handlers.CtxKeyReqOrigin, fmt.Sprintf("https://app%d.rpc.example.com", count-1)),
				RpcMethod: fmt.Sprintf("fixture%d_read", count-1),
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := v.Validate(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
