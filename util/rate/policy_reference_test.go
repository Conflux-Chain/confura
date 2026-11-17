package rate

import (
	"context"
	"testing"

	"github.com/Conflux-Chain/confura/util/acl"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/stretchr/testify/require"
)

func TestBoundStrategyDoesNotFallBack(t *testing.T) {
	for _, withDefault := range []bool{false, true} {
		for _, sid := range []uint32{0, 99, 1} {
			for _, typ := range []LimitType{LimitTypeByKey, LimitTypeByIp} {
				key := &KeyInfo{Key: "fixture", SID: sid, Type: typ}
				loader := NewKeyLoader(func(*KeysetFilter) ([]*KeyInfo, error) { return []*KeyInfo{key}, nil })
				r := NewRegistry(loader, acl.NewEthValidator)
				if withDefault {
					r.reloadOnce(&Config{Strategies: map[uint32]*Strategy{1: NewStrategy(1, DefaultStrategy)}}, &ConfigCheckSums{})
				}
				ctx := context.WithValue(context.Background(), handlers.CtxKeyAuthId, key.Key)
				decision, err := r.Resolve(ctx)
				if sid != 1 || !withDefault {
					require.Error(t, err)
					require.Nil(t, decision)
				} else {
					require.NoError(t, err)
					require.Equal(t, typ, decision.LimitType)
					require.Equal(t, ResolverProvisioned, decision.MatchedBy)
				}
			}
		}
	}
}

func TestMissingBoundACLIsDenied(t *testing.T) {
	for _, id := range []uint32{0, 1, 99} {
		key := &KeyInfo{Key: "fixture", SID: 1, AclID: id}
		loader := NewKeyLoader(func(*KeysetFilter) ([]*KeyInfo, error) { return []*KeyInfo{key}, nil })
		r := NewRegistry(loader, acl.NewEthValidator)
		r.reloadOnce(&Config{AllowLists: map[uint32]*acl.AllowList{1: acl.NewAllowList(1, "restricted")}}, &ConfigCheckSums{})
		ctx := acl.Context{Context: context.WithValue(context.Background(), handlers.CtxKeyAuthId, key.Key), RpcMethod: "eth_blockNumber"}
		if id == 99 {
			require.Error(t, r.Allow(ctx))
		} else {
			require.NoError(t, r.Allow(ctx))
		}
		r.reloadOnce(&Config{}, &ConfigCheckSums{})
		if id != 0 {
			require.Error(t, r.Allow(ctx))
		} else {
			require.NoError(t, r.Allow(ctx))
		}
	}
}
