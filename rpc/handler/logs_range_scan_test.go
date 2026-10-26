package handler

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common"
	web3types "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

func TestGetLogsRangeScanFinalByteLimit(t *testing.T) {
	oldLimit, oldErr := maxGetLogsResponseBytes, errResponseBodySizeTooLarge
	t.Cleanup(func() { maxGetLogsResponseBytes, errResponseBodySizeTooLarge = oldLimit, oldErr })
	errResponseBodySizeTooLarge = errors.New("test response byte limit")
	for _, space := range []string{"core", "espace"} {
		t.Run(space, func(t *testing.T) {
			db := newScanLogsHandlerTestDB(t)
			sqlDB, err := db.DB()
			require.NoError(t, err)
			t.Cleanup(func() { sqlDB.Close() })
			insertScanLogsMapping(t, db, 1, 10, 19, "")
			insertScanLogsMapping(t, db, 2, 20, 20, "")
			require.NoError(t, db.Exec(`CREATE TABLE bn_partitions (id INTEGER PRIMARY KEY, entity TEXT, pi INTEGER, count INTEGER, bn_min INTEGER, bn_max INTEGER, created_at DATETIME, updated_at DATETIME)`).Error)
			require.NoError(t, db.Exec(`INSERT INTO bn_partitions (id,entity,pi,count,bn_min,bn_max) VALUES (1,'logs',0,2,10,20)`).Error)
			require.NoError(t, db.Exec(`CREATE TABLE logs_0 (id INTEGER PRIMARY KEY, bn INTEGER, epoch INTEGER, topic0 TEXT, topic1 TEXT, topic2 TEXT, topic3 TEXT, log_index INTEGER, extra BLOB)`).Error)
			ethAddress := common.HexToAddress("0x1000000000000000000000000000000000000001")
			cfxAddress := cfxaddress.MustNewFromHex(ethAddress.Hex(), 1029)
			address := ethAddress.Hex()
			other := common.HexToAddress("0x1000000000000000000000000000000000000002").Hex()
			if space == "core" {
				address = cfxAddress.String()
				otherAddr := cfxaddress.MustNewFromHex(other, 1029)
				other = otherAddr.String()
			}
			selected, err := json.Marshal(map[string]string{"addr": address, "data": "0x"})
			require.NoError(t, err)
			unselected, err := json.Marshal(map[string]string{"addr": other, "data": "0x" + strings.Repeat("ff", 1000)})
			require.NoError(t, err)
			require.NoError(t, db.Exec(`INSERT INTO logs_0 (id,bn,epoch,log_index,extra) VALUES (1,10,1,0,?),(2,11,1,0,?)`, selected, unselected).Error)
			cfg := &mysql.Config{GetLogsRangeScan: mysql.GetLogsRangeScanConfig{Enabled: true, MinAddresses: 1}}
			var run func() (int, bool, error)
			var expectedJSON []byte
			selectedLog := store.Log{BlockNumber: 10, Epoch: 1, Extra: selected}
			if space == "core" {
				typed, err := selectedLog.ToCfxLog()
				require.NoError(t, err)
				expectedJSON, err = json.Marshal([]cfxtypes.Log{*typed})
				require.NoError(t, err)
				handler := NewCfxLogsApiHandler(mysql.NewCfxStore(db, cfg, store.StoreConfig()), nil, 0)
				filter := &cfxtypes.LogFilter{FromEpoch: cfxtypes.NewEpochNumberUint64(1), ToEpoch: cfxtypes.NewEpochNumberUint64(2), Address: []cfxtypes.Address{cfxAddress}}
				dbFilters, fnFilter, err := handler.splitLogFilter(nil, filter)
				require.NoError(t, err)
				require.Nil(t, fnFilter)
				require.Len(t, dbFilters, 1)
				require.EqualValues(t, 10, dbFilters[0].BlockFrom)
				require.EqualValues(t, 20, dbFilters[0].BlockTo)
				run = func() (int, bool, error) {
					logs, hit, err := handler.GetLogs(context.Background(), nil, filter, "")
					return len(logs), hit, err
				}
			} else {
				typed, err := selectedLog.ToEthLog()
				require.NoError(t, err)
				expectedJSON, err = json.Marshal([]web3types.Log{*typed})
				require.NoError(t, err)
				handler := NewEthLogsApiHandler(mysql.NewEthStore(db, cfg, store.EthStoreConfig()), 0)
				from, to := web3types.BlockNumber(10), web3types.BlockNumber(20)
				filter := &web3types.FilterQuery{FromBlock: &from, ToBlock: &to, Addresses: []common.Address{ethAddress}}
				run = func() (int, bool, error) {
					logs, hit, err := handler.GetLogs(context.Background(), nil, filter, "")
					return len(logs), hit, err
				}
			}
			// An oversized nonmatching candidate does not count toward response bytes.
			maxGetLogsResponseBytes = uint64(len(expectedJSON))
			count, hit, err := run()
			require.NoError(t, err)
			require.True(t, hit)
			require.Equal(t, 1, count)
			maxGetLogsResponseBytes = uint64(len(expectedJSON) - 1)
			_, _, err = run()
			require.ErrorIs(t, err, errResponseBodySizeTooLarge)
		})
	}
}
