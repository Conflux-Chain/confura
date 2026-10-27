package handler

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common"
	web3types "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestGetLogsSplitRangeKeepsBoundChecks(t *testing.T) {
	oldBytes := maxGetLogsResponseBytes
	oldBlocks, oldEpochs := store.MaxLogBlockRange, store.MaxLogEpochRange
	maxGetLogsResponseBytes = 1 << 30
	store.MaxLogBlockRange, store.MaxLogEpochRange = 1000, 1000
	t.Cleanup(func() {
		maxGetLogsResponseBytes = oldBytes
		store.MaxLogBlockRange, store.MaxLogEpochRange = oldBlocks, oldEpochs
	})
	for _, space := range []string{"espace", "core"} {
		t.Run(space, func(t *testing.T) {
			db := newScanLogsHandlerTestDB(t)
			createGetLogsTables(t, db)
			epoch := uint64(10)
			if space == "core" {
				epoch = 1
			}
			insertScanLogsMapping(t, db, epoch, 10, 10, common.HexToHash("0x1234").Hex())
			require.NoError(t, db.Exec("UPDATE bn_partitions SET count = 10002").Error)
			require.NoError(t, db.Exec(`WITH RECURSIVE seq(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM seq WHERE n < 10001) INSERT INTO logs_0 (id,bn,epoch,log_index,extra) SELECT n+1,10,?,n,'{}' FROM seq`, epoch).Error)
			observed := false
			require.NoError(t, db.Callback().Query().Before("gorm:query").Register("test:split_range_bounds", func(tx *gorm.DB) {
				if strings.Contains(tx.Statement.Table, "logs_0") {
					observed = true
					require.True(t, store.IsBoundChecksEnabled(tx.Statement.Context))
				}
			}))
			// Only the DB segment is one unit; the original RPC range spans two.
			if space == "core" {
				client := &hashLogsClient{}
				logs, _, err := newTestCfxScanLogsHandler(db).GetLogs(context.Background(), client, &cfxtypes.LogFilter{FromEpoch: epochNumber(1), ToEpoch: epochNumber(2)}, "")
				require.ErrorIs(t, err, store.ErrFilterResultSetTooLarge)
				require.Empty(t, logs)
				require.Zero(t, client.calls)
			} else {
				client := &ethHashLogsClient{}
				logs, _, err := newTestEthScanLogsHandler(db).GetLogs(context.Background(), client, &web3types.FilterQuery{FromBlock: blockNumber(10), ToBlock: blockNumber(11)}, "")
				require.ErrorIs(t, err, store.ErrFilterResultSetTooLarge)
				require.Empty(t, logs)
				require.Zero(t, client.calls)
			}
			require.True(t, observed, "must inspect the DB log query context")
		})
	}
}

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

func TestGetLogsResponseBytesAcrossSources(t *testing.T) {
	oldCheck, oldBytes, oldErr := checkMixedSourceConsistency, maxGetLogsResponseBytes, errResponseBodySizeTooLarge
	oldBlocks, oldEpochs := store.MaxLogBlockRange, store.MaxLogEpochRange
	checkMixedSourceConsistency = false // Isolate byte accounting from metadata validation.
	store.MaxLogBlockRange, store.MaxLogEpochRange = 1000, 1000
	errResponseBodySizeTooLarge = errors.New("test byte limit")
	t.Cleanup(func() {
		checkMixedSourceConsistency, maxGetLogsResponseBytes, errResponseBodySizeTooLarge = oldCheck, oldBytes, oldErr
		store.MaxLogBlockRange, store.MaxLogEpochRange = oldBlocks, oldEpochs
	})
	for _, space := range []string{"espace", "core"} {
		for _, source := range []string{"fullnode", "empty_db_and_fullnode", "db_and_fullnode"} {
			t.Run(space+"/"+source, func(t *testing.T) {
				db := newScanLogsHandlerTestDB(t)
				createGetLogsTables(t, db)
				epoch := uint64(10)
				address := common.HexToAddress("0x1000000000000000000000000000000000000001").Hex()
				if space == "core" {
					epoch = 1
					address = cfxaddress.MustNewFromHex(address, 1029).String()
				}
				insertScanLogsMapping(t, db, epoch, 10, 10, common.HexToHash("0x1234").Hex())
				extra, err := json.Marshal(map[string]string{"addr": address, "data": "0x01"})
				require.NoError(t, err)
				if source == "db_and_fullnode" {
					require.NoError(t, db.Exec(`INSERT INTO logs_0 (id,bn,epoch,log_index,extra) VALUES (1,10,?,0,?)`, epoch, extra).Error)
					require.NoError(t, db.Exec(`UPDATE bn_partitions SET count=1`).Error)
				}
				var expected []byte
				var run func() error
				stored := &store.Log{BlockNumber: 10, Epoch: epoch, Extra: extra}
				if space == "espace" {
					fn := []web3types.Log{{BlockNumber: 11, Data: []byte{2}}, {BlockNumber: 12, Data: []byte{3}}}
					all := fn
					if source == "db_and_fullnode" {
						log, err := stored.ToEthLog()
						require.NoError(t, err)
						all = append([]web3types.Log{*log}, fn...)
					}
					expected, err = json.Marshal(all)
					require.NoError(t, err)
					client := &ethHashLogsClient{logs: fn}
					filter := &web3types.FilterQuery{FromBlock: blockNumber(10), ToBlock: blockNumber(12)}
					if source == "fullnode" {
						filter.FromBlock = blockNumber(11)
					}
					run = func() error {
						logs, _, err := newTestEthScanLogsHandler(db).GetLogs(context.Background(), client, filter, "")
						if err == nil {
							require.Len(t, logs, len(all))
						} else {
							require.Empty(t, logs)
						}
						return err
					}
				} else {
					fn := []cfxtypes.Log{{Address: cfxaddress.MustNewFromBase32(address), EpochNumber: cfxtypes.NewBigInt(2), Data: []byte{2}}, {Address: cfxaddress.MustNewFromBase32(address), EpochNumber: cfxtypes.NewBigInt(3), Data: []byte{3}}}
					all := fn
					if source == "db_and_fullnode" {
						log, err := stored.ToCfxLog()
						require.NoError(t, err)
						all = append([]cfxtypes.Log{*log}, fn...)
					}
					expected, err = json.Marshal(all)
					require.NoError(t, err)
					client := &hashLogsClient{logs: fn}
					filter := &cfxtypes.LogFilter{FromEpoch: epochNumber(1), ToEpoch: epochNumber(3)}
					if source == "fullnode" {
						filter.FromEpoch = epochNumber(2)
					}
					run = func() error {
						logs, _, err := newTestCfxScanLogsHandler(db).GetLogs(context.Background(), client, filter, "")
						if err == nil {
							require.Len(t, logs, len(all))
						} else {
							require.Empty(t, logs)
						}
						return err
					}
				}
				maxGetLogsResponseBytes = uint64(len(expected))
				require.NoError(t, run())
				maxGetLogsResponseBytes--
				require.ErrorIs(t, run(), errResponseBodySizeTooLarge)
			})
		}
	}
}
