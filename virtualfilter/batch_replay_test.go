package virtualfilter

import (
	"context"
	"fmt"
	"testing"

	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	ethtypes "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

func TestBatchReplayMatchesCache(t *testing.T) {
	for _, space := range []string{"eth", "cfx"} {
		for _, cache := range []int{1, 100} {
			t.Run(fmt.Sprintf("%s/cache=%d", space, cache), func(t *testing.T) {
				db, s := newAcceptedLogSession(t, space, cache)
				require.NoError(t, s.persist(s.batch(1)))
				require.NoError(t, s.w.merge(s.batch(1)))
				_, err := s.f.fetch(context.Background())
				require.NoError(t, err)
				var batch filterChanges
				if space == "eth" {
					a, b := testEthLog(2), testEthLog(3)
					ra, rb := a, b
					ra.Removed = true
					rb.Removed = true
					batch = &ethtypes.FilterChanges{Logs: []ethtypes.Log{a, b, rb, ra, a, b, testEthLog(4)}}
				} else {
					batch = coreBatch(testCfxLog(2), testCfxLog(3), coreRevert(1).Logs[0], testCfxLog(2), testCfxLog(3), testCfxLog(4))
				}
				require.NoError(t, s.persist(batch))
				require.NoError(t, s.w.merge(batch))
				got, err := s.f.fetch(context.Background())
				require.NoError(t, err)
				if space == "eth" {
					logs := got.(*ethtypes.FilterChanges).Logs
					require.Equal(t, []ethtypes.Log{testEthLog(2), testEthLog(3), testEthLog(4)}, logs)
				} else {
					logs := got.(*cfxtypes.CfxFilterChanges).Logs
					require.Len(t, logs, 4)
					require.EqualValues(t, 1, logs[0].RevertTo.ToInt().Uint64())
					for i, log := range logs[1:] {
						require.EqualValues(t, i+2, log.EpochNumber.ToInt().Uint64())
					}
				}
				var active, count int64
				require.NoError(t, db.Table("vflogs_0xabc_0").Where("is_del = ?", false).Count(&active).Error)
				require.EqualValues(t, 4, active)
				require.NoError(t, db.Table("bn_partitions").Select("count").Row().Scan(&count))
				require.EqualValues(t, 4, count, "partition volume counts rows actually inserted")
			})
		}
	}
}
