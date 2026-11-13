package virtualfilter

import (
	"context"
	rpc "github.com/openweb3/go-rpc-provider"
	"testing"

	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethtypes "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

func TestEmptyChainConsumptionProgress(t *testing.T) {
	for _, space := range []string{"eth", "cfx"} {
		t.Run(space, func(t *testing.T) {
			_, s := newTestLogSession(t, space, 100)
			s.w.session.fcursors["slow"] = &filterDelegate{}
			apply := func(c filterChanges) { require.NoError(t, s.persist(c)); require.NoError(t, s.w.merge(c)) }
			read := func() filterChanges { c, e := s.f.fetch(context.Background()); require.NoError(t, e); return c }
			apply(s.batch(10, 11))
			require.Equal(t, 2, logCount(read()))
			var revert filterChanges
			if space == "eth" {
				a, b := testEthLog(10), testEthLog(11)
				a.Removed = true
				b.Removed = true
				revert = &ethtypes.FilterChanges{Logs: []ethtypes.Log{b, a}}
			} else {
				revert = coreRevert(9)
			}
			apply(revert)
			want := 2
			if space == "cfx" {
				want = 1
			}
			require.Equal(t, want, logCount(read()))
			require.Zero(t, logCount(read()))
			// A newly accepted filter starts at this empty chain, not old genesis.
			s.w.session.fcursors["new"] = &filterDelegate{cursor: s.w.session.fchain.snapshotLatestCursor()}
			if space == "cfx" {
				apply(coreRevert(0))
				got := read().(*cfxtypes.CfxFilterChanges)
				require.Len(t, got.Logs, 1)
				require.Zero(t, got.Logs[0].RevertTo.ToInt().Uint64())
				require.Zero(t, logCount(read()))
				cw := s.f.(*cfxLogFilter).worker
				for _, id := range []string{"new", "slow"} {
					d := s.w.session.fcursors[rpc.ID(id)]
					pc, err := cw.fetchPollingChanges(d.cursor)
					require.NoError(t, err)
					require.NotEmpty(t, pc.epochs)
					require.True(t, pc.epochs[0].reorged())
					require.Zero(t, pc.epochs[0].revertedTo)
				}
			}
			// Re-mining old hashes after full rewind must deliver new canonical logs.
			apply(s.batch(10, 11))
			got := read()
			require.Equal(t, 2, logCount(got))
			require.Zero(t, logCount(read()))
		})
	}
}
func coreRevert(to uint64) *cfxtypes.CfxFilterChanges {
	return &cfxtypes.CfxFilterChanges{Type: "log", Logs: []*cfxtypes.SubscriptionLog{{ChainReorg: &cfxtypes.ChainReorg{RevertTo: (*hexutil.Big)(newBig(to))}}}}
}
