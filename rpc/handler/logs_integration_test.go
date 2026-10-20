package handler

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common"
	web3types "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestGetLogsSmallestUnitReturnsAllLogs(t *testing.T) {
	oldLimit := maxGetLogsResponseBytes
	maxGetLogsResponseBytes = 1 // Both count and byte limits must be exempt for one unit.
	t.Cleanup(func() { maxGetLogsResponseBytes = oldLimit })
	for _, space := range []string{"core", "espace"} {
		for _, mode := range []string{"range", "reorg", "hash"} {
			if space == "core" && mode == "hash" {
				continue
			} // Core hashes are tested on the direct-FN path.
			t.Run(space+"/"+mode, func(t *testing.T) {
				db := newScanLogsHandlerTestDB(t)
				createGetLogsTables(t, db)
				hash := common.HexToHash("0x1234")
				epoch, blockTo := uint64(10), uint64(10)
				address := common.HexToAddress("0x1000000000000000000000000000000000000001").Hex()
				if space == "core" {
					epoch = 1
					blockTo = 11
					address = cfxaddress.MustNewFromHex(address, 1029).String()
				}
				insertScanLogsMapping(t, db, epoch, 10, blockTo, hash.Hex())
				require.NoError(t, db.Exec("UPDATE bn_partitions SET count = 10002, bn_max = ?", blockTo).Error)
				extra, err := json.Marshal(map[string]string{"addr": address, "bh": hash.Hex(), "data": "0x01"})
				require.NoError(t, err)
				// A Core epoch can contain multiple blocks; eSpace's unit is one block.
				blockExpression := "10"
				indexExpression := "n"
				if space == "core" {
					blockExpression = "10+n/6000"
					indexExpression = "n%6000"
				}
				require.NoError(t, db.Exec(`WITH RECURSIVE seq(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM seq WHERE n < 10001) INSERT INTO logs_0 (id,bn,epoch,log_index,extra) SELECT n+1,`+blockExpression+`,?,`+indexExpression+`,? FROM seq`, epoch, extra).Error)
				changed := false
				if mode == "reorg" {
					require.NoError(t, db.Callback().Query().After("gorm:query").Register("test:reorg_between_pages", func(tx *gorm.DB) {
						if changed || tx.Statement.Table != "logs_0" || tx.RowsAffected != 1000 {
							return
						}
						changed = true
						replacement, err := json.Marshal(map[string]string{"addr": address, "bh": hash.Hex(), "data": "0x02"})
						require.NoError(t, err)
						require.NoError(t, db.Exec("UPDATE logs_0 SET extra = ?", replacement).Error)
						require.NoError(t, db.Exec(`INSERT INTO configs (name,value) VALUES ('reorg.version','1')`).Error)
					}))
				}
				expected := byte(1)
				if mode == "reorg" {
					expected = 2
				}
				if space == "core" {
					logs, hit, err := newTestCfxScanLogsHandler(db).GetLogs(context.Background(), nil, &cfxtypes.LogFilter{FromEpoch: epochNumber(1), ToEpoch: epochNumber(1)}, "")
					require.NoError(t, err)
					require.True(t, hit)
					require.Len(t, logs, int(store.MaxLogLimit)+2)
					for i, log := range logs {
						require.Equal(t, []byte{expected}, []byte(log.Data))
						require.EqualValues(t, i%6000, log.LogIndex.ToInt().Uint64())
					}
				} else {
					filter := &web3types.FilterQuery{FromBlock: blockNumber(10), ToBlock: blockNumber(10)}
					client := &ethHashLogsClient{block: &web3types.Block{Number: big.NewInt(10)}}
					if mode == "hash" {
						filter = &web3types.FilterQuery{BlockHash: &hash}
					}
					logs, hit, err := newTestEthScanLogsHandler(db).GetLogs(context.Background(), client, filter, "")
					require.NoError(t, err)
					require.True(t, hit)
					require.Len(t, logs, int(store.MaxLogLimit)+2)
					require.Zero(t, client.calls)
					for i, log := range logs {
						require.Equal(t, []byte{expected}, log.Data)
						require.EqualValues(t, i, log.Index)
						require.Equal(t, hash, log.BlockHash)
					}
				}
				require.Equal(t, mode == "reorg", changed)
			})
		}
	}
}
