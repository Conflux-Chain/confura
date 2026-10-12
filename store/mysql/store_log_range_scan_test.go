package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func rangeScanFixture(t *testing.T, count int) (*MysqlStore[store.EthData], store.LogFilter) {
	t.Helper()
	db, e := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, e)
	sqlDB, e := db.DB()
	require.NoError(t, e)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { sqlDB.Close() })
	require.NoError(t, db.AutoMigrate(&bnPartition{}))
	require.NoError(t, db.Table("logs_0").AutoMigrate(&log{}))
	require.NoError(t, db.Create(&bnPartition{Entity: "logs", Index: 0, BnMin: sql.NullInt64{Int64: 10, Valid: true}, BnMax: sql.NullInt64{Int64: 20, Valid: true}}).Error)
	rows := make([]*log, count)
	for i := range rows {
		rows[i] = &log{BlockNumber: 10, Epoch: 10, LogIndex: uint64(i), Topic0: fmt.Sprintf("0x%064x", 1), Extra: []byte(fmt.Sprintf(`{"addr":"0x%040x"}`, i%2+1))}
	}
	if count > 0 {
		require.NoError(t, db.Table("logs_0").CreateInBatches(rows, 100).Error)
	}
	ms := &MysqlStore[store.EthData]{config: &Config{GetLogsRangeScan: GetLogsRangeScanConfig{Enabled: true, MinAddresses: 32}}, ls: newLogStore[store.EthData](db, nil, nil, nil)}
	var addresses []string
	for i := 1; i <= 32; i++ {
		addresses = append(addresses, fmt.Sprintf("0x%040x", i))
	}
	return ms, store.LogFilter{BlockFrom: 10, BlockTo: 10, Contracts: store.NewVariadicValue(addresses...)}
}

func TestRangeScanCandidateBoundaries(t *testing.T) {
	for _, n := range []int{0, 9999, 10000, 10001} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			ms, f := rangeScanFixture(t, n)
			rows, handled, e := ms.tryRangeScanLogs(context.Background(), f)
			require.NoError(t, e)
			require.Equal(t, n <= 10000, handled)
			if handled {
				require.Len(t, rows, n)
			}
		})
	}
}

func TestRangeScanRoutingAndFiltering(t *testing.T) {
	ms, f := rangeScanFixture(t, 4)
	for _, n := range []int{31, 32, 33} {
		var addresses []string
		for i := 1; i <= n; i++ {
			addresses = append(addresses, fmt.Sprintf("0x%040x", i))
		}
		f.Contracts = store.NewVariadicValue(addresses...)
		_, hit, e := ms.tryRangeScanLogs(context.Background(), f)
		require.NoError(t, e)
		require.Equal(t, n >= 32, hit)
	}
	ms.config.GetLogsRangeScan.MinAddresses = 1
	f.Contracts = store.NewVariadicValue(fmt.Sprintf("0x%040x", 1))
	f.Topics = []store.VariadicValue[string]{store.NewVariadicValue(fmt.Sprintf("0x%064x", 1))}
	rows, hit, e := ms.tryRangeScanLogs(context.Background(), f)
	require.NoError(t, e)
	require.True(t, hit)
	require.Len(t, rows, 2)
	require.EqualValues(t, 2, rows[1].LogIndex)
	f.Topics = append(f.Topics, store.NewVariadicValue(fmt.Sprintf("0x%064x", 2)))
	rows, hit, e = ms.tryRangeScanLogs(context.Background(), f)
	require.NoError(t, e)
	require.True(t, hit)
	require.Empty(t, rows)
	f.BlockTo = 21
	_, hit, e = ms.tryRangeScanLogs(context.Background(), f)
	require.NoError(t, e)
	require.False(t, hit)
	f.BlockTo = 11
	ms.config.GetLogsRangeScan.MaxBlocks = 1
	_, hit, e = ms.tryRangeScanLogs(context.Background(), f)
	require.NoError(t, e)
	require.False(t, hit)
	ms.config.GetLogsRangeScan.Enabled = false
	_, hit, e = ms.tryRangeScanLogs(context.Background(), f)
	require.NoError(t, e)
	require.False(t, hit)
}

func TestRangeScanIDGapsAndCancellation(t *testing.T) {
	ms, f := rangeScanFixture(t, 2)
	require.NoError(t, ms.ls.db.Table("logs_0").Where("id = 2").Update("id", 20000).Error)
	_, hit, e := ms.tryRangeScanLogs(context.Background(), f)
	require.NoError(t, e)
	require.False(t, hit)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, e = ms.tryRangeScanLogs(ctx, f)
	require.ErrorIs(t, e, store.ErrGetLogsTimeout)
}

func TestRangeScanActualCountGuard(t *testing.T) {
	ms, f := rangeScanFixture(t, 10000)
	injected := false
	require.NoError(t, ms.ls.db.Callback().Query().After("gorm:query").Register("insert_after_estimate", func(db *gorm.DB) {
		if !injected && db.Statement.Table == "logs_0" {
			injected = true
			require.NoError(t, ms.ls.db.Table("logs_0").Create(&log{BlockNumber: 10, Epoch: 10, LogIndex: 10000, Extra: []byte(`{}`)}).Error)
		}
	}))
	_, hit, e := ms.tryRangeScanLogs(context.Background(), f)
	require.NoError(t, e)
	require.True(t, injected)
	require.False(t, hit)
}

func TestRangeScanPartitionsReorgAndSQLCount(t *testing.T) {
	ms, f := rangeScanFixture(t, 2)
	db := ms.ls.db
	require.NoError(t, db.Exec(`CREATE TABLE logs_1 (id INTEGER PRIMARY KEY AUTOINCREMENT, bn INTEGER, epoch INTEGER, topic0 TEXT, topic1 TEXT, topic2 TEXT, topic3 TEXT, log_index INTEGER, extra BLOB)`).Error)
	require.NoError(t, db.Model(&bnPartition{}).Where("entity = ?", "logs").Update("bn_max", 10).Error)
	require.NoError(t, db.Create(&bnPartition{Entity: "logs", Index: 1, BnMin: sql.NullInt64{Int64: 15, Valid: true}, BnMax: sql.NullInt64{Int64: 20, Valid: true}}).Error)
	require.NoError(t, db.Table("logs_1").Create(&log{BlockNumber: 15, Epoch: 15, Extra: []byte(fmt.Sprintf(`{"addr":"0x%040x"}`, 1))}).Error)
	f.BlockTo = 20
	queries := 0
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register("count_queries", func(db *gorm.DB) {
		if !db.DryRun {
			queries++
		}
	}))
	rows, hit, e := ms.tryRangeScanLogs(context.Background(), f)
	require.NoError(t, e)
	require.True(t, hit)
	require.Len(t, rows, 3)
	require.Equal(t, 5, queries) // one router query, two estimates and two reads
	var manyAddresses []string
	for i := 1; i <= 921; i++ {
		manyAddresses = append(manyAddresses, fmt.Sprintf("0x%040x", i))
	}
	f.Contracts = store.NewVariadicValue(manyAddresses...)
	queries = 0
	rows, hit, e = ms.tryRangeScanLogs(context.Background(), f)
	require.NoError(t, e)
	require.True(t, hit)
	require.Len(t, rows, 3)
	require.Equal(t, 5, queries)

	// Simulate rollback and replacement at the same block with a new auto-increment ID.
	require.NoError(t, db.Table("logs_1").Where("bn = 15").Delete(&log{}).Error)
	require.NoError(t, db.Table("logs_1").Create(&log{BlockNumber: 15, Epoch: 15, LogIndex: 7, Extra: []byte(fmt.Sprintf(`{"addr":"0x%040x"}`, 1))}).Error)
	rows, hit, e = ms.tryRangeScanLogs(context.Background(), f)
	require.NoError(t, e)
	require.True(t, hit)
	require.Len(t, rows, 3)
	require.EqualValues(t, 7, rows[2].LogIndex)
	f.BlockFrom = 9
	_, hit, e = ms.tryRangeScanLogs(context.Background(), f)
	require.NoError(t, e)
	require.False(t, hit)
}

func TestRangeScanMatchesAddressPath(t *testing.T) {
	ms, f := rangeScanFixture(t, 4)
	db := ms.ls.db
	ms.cs = NewContractStore(db)
	ms.ts = NewTopicStore(db)
	require.NoError(t, db.AutoMigrate(&Contract{}))
	require.NoError(t, db.Exec("DROP INDEX index_lue").Error)
	require.NoError(t, db.AutoMigrate(&Topic{}))
	ms.ails = NewAddressIndexedLogStore[store.EthData](db, ms.cs, ms.ts, 1)
	ms.bcls = newBigContractLogStore[store.EthData](db, ms.cs, ms.ts, nil, ms.ails, nil)
	require.NoError(t, db.Table("addr_logs_0").AutoMigrate(&AddressIndexedLog{}))
	topic := fmt.Sprintf("0x%064x", 1)
	tid, _, e := ms.ts.GetOrCreate(topic)
	require.NoError(t, e)
	var originals []*log
	require.NoError(t, db.Table("logs_0").Find(&originals).Error)
	for i := 1; i <= 2; i++ {
		addr := fmt.Sprintf("0x%040x", i)
		cid, _, e := ms.cs.AddContractIfAbsent(addr)
		require.NoError(t, e)
		for _, row := range originals {
			parsed, e := (*store.Log)(row).ToEthLog()
			require.NoError(t, e)
			if parsed.Address.Hex() != fmt.Sprintf("0x%040x", i) {
				continue
			}
			require.NoError(t, db.Table("addr_logs_0").Create(&AddressIndexedLog{ContractID: cid, BlockNumber: row.BlockNumber, Epoch: row.Epoch, Topic0ID: tid, LogIndex: row.LogIndex, Extra: row.Extra}).Error)
		}
	}
	for _, topics := range [][]store.VariadicValue[string]{nil, {store.NewVariadicValue(topic)}, {store.NewVariadicValue(fmt.Sprintf("0x%064x", 2))}, {store.NewVariadicValue[string](), store.NewVariadicValue(topic)}} {
		f.Topics = topics
		old, e := ms.getContractLogs(context.Background(), f.Contracts.ToSlice(), f)
		require.NoError(t, e)
		got, hit, e := ms.tryRangeScanLogs(context.Background(), f)
		require.NoError(t, e)
		require.True(t, hit)
		require.Equal(t, old, got)
	}
}

func TestRangeScanCompletedMigrationAndPublicDispatch(t *testing.T) {
	ms, f := rangeScanFixture(t, 2)
	db := ms.ls.db
	// A completed migration is represented by a nonempty initial dedicated partition.
	require.NoError(t, db.AutoMigrate(&Contract{}))
	require.NoError(t, db.Exec("DROP INDEX index_lue").Error)
	require.NoError(t, db.AutoMigrate(&Topic{}))
	ms.cs = NewContractStore(db)
	ms.ts = NewTopicStore(db)
	ms.ails = NewAddressIndexedLogStore[store.EthData](db, ms.cs, ms.ts, 1)
	ms.bcls = newBigContractLogStore[store.EthData](db, ms.cs, ms.ts, nil, ms.ails, nil)
	createAddressScanTable(t, db)
	cid, _, e := ms.cs.AddContractIfAbsent(fmt.Sprintf("0x%040x", 1))
	require.NoError(t, e)
	tid, _, e := ms.ts.GetOrCreate(fmt.Sprintf("0x%064x", 1))
	require.NoError(t, e)
	require.NoError(t, db.Exec(`CREATE TABLE clogs_1_0 (id INTEGER PRIMARY KEY, bn INTEGER, epoch INTEGER, tid INTEGER, topic1 TEXT, topic2 TEXT, topic3 TEXT, log_index INTEGER, extra BLOB)`).Error)
	var row log
	require.NoError(t, db.Table("logs_0").First(&row).Error)
	require.NoError(t, db.Exec("INSERT INTO clogs_1_0 (id,bn,epoch,tid,log_index,extra) VALUES (1,10,10,?,0,?)", tid, row.Extra).Error)
	require.NoError(t, db.Create(&bnPartition{Entity: fmt.Sprintf("clogs_%d", cid), Count: 1, BnMin: sql.NullInt64{Int64: 10, Valid: true}, BnMax: sql.NullInt64{Int64: 20, Valid: true}}).Error)
	// The other address is unknown to the index: select only the migrated contract.
	f.Contracts = store.NewVariadicValue(fmt.Sprintf("0x%040x", 1))
	ms.config.GetLogsRangeScan.MinAddresses = 1
	for _, ctx := range []context.Context{context.Background(), store.NewContextWithBoundChecksDisabled(context.Background())} {
		old, e := ms.getContractLogs(ctx, f.Contracts.ToSlice(), f)
		require.NoError(t, e)
		got, e := ms.GetLogs(ctx, f)
		require.NoError(t, e)
		require.Equal(t, old, got)
	}
}

func TestRangeScanPropagatesErrors(t *testing.T) {
	ms, f := rangeScanFixture(t, 1)
	require.NoError(t, ms.ls.db.Table("logs_0").Where("id = 1").Update("extra", []byte("invalid JSON")).Error)
	_, hit, e := ms.tryRangeScanLogs(context.Background(), f)
	require.Error(t, e)
	require.False(t, hit)
	require.NoError(t, ms.ls.db.Exec("DROP TABLE logs_0").Error)
	_, hit, e = ms.tryRangeScanLogs(context.Background(), f)
	require.Error(t, e)
	require.False(t, hit)
}

func TestRangeScanConfiguration(t *testing.T) {
	const key = "range_scan_config_test"
	viper.Set(key, map[string]interface{}{"enabled": true})
	defer viper.Set(key, map[string]interface{}{})
	cfg := mustNewConfigFromViper(key)
	require.False(t, cfg.GetLogsRangeScan.Enabled)
	require.Equal(t, 32, cfg.GetLogsRangeScan.MinAddresses)
	require.Zero(t, cfg.GetLogsRangeScan.MaxBlocks)
	viper.Set(key, map[string]interface{}{"enabled": true, "getLogsRangeScan": map[string]interface{}{"enabled": true, "minAddresses": 0}})
	logger := logrus.StandardLogger()
	originalExit := logger.ExitFunc
	t.Cleanup(func() { logger.ExitFunc = originalExit })
	var exitCode int
	logger.ExitFunc = func(code int) {
		exitCode = code
		panic("intercept fatal exit")
	}
	require.Panics(t, func() { mustNewConfigFromViper(key) })
	require.Equal(t, 1, exitCode)
}

func TestRangeScanCoreMatchesSharedAndDedicated(t *testing.T) {
	ms, db := newScanMysqlStoreTestHarness(t)
	ms.config = &Config{GetLogsRangeScan: GetLogsRangeScanConfig{Enabled: true, MinAddresses: 2}}
	createAddressScanTable(t, db)
	require.NoError(t, db.Table("logs_0").AutoMigrate(&log{}))
	require.NoError(t, db.Create(&bnPartition{Entity: "logs", BnMin: sql.NullInt64{Int64: 10, Valid: true}, BnMax: sql.NullInt64{Int64: 20, Valid: true}}).Error)
	addresses := []cfxtypes.Address{
		cfxaddress.MustNewFromHex("0x1000000000000000000000000000000000000001", 1029),
		cfxaddress.MustNewFromHex("0x1000000000000000000000000000000000000002", 1029),
	}
	topic0 := cfxtypes.Hash(fmt.Sprintf("0x%064x", 1))
	topic1 := cfxtypes.Hash(fmt.Sprintf("0x%064x", 2))
	absent := cfxtypes.Hash(fmt.Sprintf("0x%064x", 3))
	tid, _, err := ms.ts.GetOrCreate(topic0.String())
	require.NoError(t, err)
	var cids []uint64
	for i, addr := range addresses {
		cid, _, err := ms.cs.AddContractIfAbsent(addr.String())
		require.NoError(t, err)
		cids = append(cids, cid)
		for j := 0; j < 2; j++ {
			extra, err := json.Marshal(map[string]interface{}{"addr": addr})
			require.NoError(t, err)
			bn := uint64(10 + i*2 + j)
			row := log{BlockNumber: bn, Epoch: 1, Topic0: topic0.String(), LogIndex: 0, Extra: extra}
			if j == 0 {
				row.Topic1 = topic1.String()
			}
			require.NoError(t, db.Table("logs_0").Create(&row).Error)
			require.NoError(t, db.Table("addr_logs_0").Create(&AddressIndexedLog{ContractID: cid, BlockNumber: bn, Epoch: 1, Topic0ID: tid, Topic1: row.Topic1, Extra: extra}).Error)
		}
	}
	filters := []struct {
		name   string
		topics [][]cfxtypes.Hash
		count  int
	}{
		{"all", nil, 4},
		{"or", [][]cfxtypes.Hash{{absent, topic0}}, 4},
		{"and", [][]cfxtypes.Hash{{topic0}, {topic1}}, 2},
		{"wildcard", [][]cfxtypes.Hash{nil, {topic1}}, 2},
		{"missing topic", [][]cfxtypes.Hash{nil, nil, {topic1}}, 0},
	}
	compare := func(t *testing.T) {
		for _, test := range filters {
			t.Run(test.name, func(t *testing.T) {
				filter := store.ParseCfxLogFilter(10, 13, &cfxtypes.LogFilter{Address: addresses, Topics: test.topics})
				old, err := ms.getContractLogs(context.Background(), filter.Contracts.ToSlice(), filter)
				require.NoError(t, err)
				got, err := ms.GetLogs(context.Background(), filter)
				require.NoError(t, err)
				require.Len(t, got, test.count)
				require.Equal(t, old, got)
			})
		}
	}
	t.Run("shared", compare)
	// Move one contract to the dedicated side while retaining universal logs.
	require.NoError(t, db.Exec("CREATE TABLE clogs_1_0 AS SELECT * FROM addr_logs_0 WHERE cid = ?", cids[0]).Error)
	require.NoError(t, db.Table("addr_logs_0").Where("cid = ?", cids[0]).Delete(&AddressIndexedLog{}).Error)
	require.NoError(t, db.Create(&bnPartition{Entity: "clogs_1", Count: 2, BnMin: sql.NullInt64{Int64: 10, Valid: true}, BnMax: sql.NullInt64{Int64: 20, Valid: true}}).Error)
	t.Run("mixed dedicated and shared", compare)
	// Canonical matching also accepts uppercase base32.
	f := store.LogFilter{BlockFrom: 10, BlockTo: 13, Contracts: store.NewVariadicValue(strings.ToUpper(addresses[0].String()), addresses[1].String())}
	got, hit, err := ms.tryRangeScanLogs(context.Background(), f)
	require.NoError(t, err)
	require.True(t, hit)
	require.Len(t, got, 4)
	// RPC handlers normalize and deduplicate addresses before the store call.
	f.Contracts = store.NewVariadicValue(addresses[0].String())
	_, hit, err = ms.tryRangeScanLogs(context.Background(), f)
	require.NoError(t, err)
	require.False(t, hit)
}

func TestFilterRangeScanLogsDoesNotDecodeRPCPayload(t *testing.T) {
	// Only the address and indexed topics are needed for filtering. RPC payload
	// conversion and final byte limits remain the handler's responsibility.
	row := &log{ID: 7, Topic0: "0xabc", Extra: []byte(`{"addr":"0xAbCd","data":"invalid hex"}`)}
	filter := store.LogFilter{
		Contracts: store.NewVariadicValue("0xabcd"),
		Topics:    []store.VariadicValue[string]{store.NewVariadicValue("0xABC")},
	}
	got, err := filterRangeScanLogs(context.Background(), filter, []*log{row})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Zero(t, got[0].ID)
	require.Equal(t, uint64(7), row.ID)
	require.Equal(t, row.Extra, got[0].Extra)
}

func TestFilterRangeScanLogsMatchesTopicsBeforeDecodingAddress(t *testing.T) {
	row := &log{Topic0: "0xabc", Extra: []byte("invalid JSON")}
	filter := store.LogFilter{
		Contracts: store.NewVariadicValue("0xabcd"),
		Topics:    []store.VariadicValue[string]{store.NewVariadicValue("0xdef")},
	}
	result, err := filterRangeScanLogs(context.Background(), filter, []*log{row})
	require.NoError(t, err)
	require.Empty(t, result)

	// A matching topic still requires address parsing, including its errors.
	filter.Topics[0] = store.NewVariadicValue("0xabc")
	_, err = filterRangeScanLogs(context.Background(), filter, []*log{row})
	require.Error(t, err)
}

func TestRangeScanContextErrors(t *testing.T) {
	t.Run("expired deadline", func(t *testing.T) {
		ms, filter := rangeScanFixture(t, 1)
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		_, _, err := ms.tryRangeScanLogs(ctx, filter)
		require.ErrorIs(t, err, store.ErrGetLogsTimeout)
	})
	t.Run("cancel between metadata and candidate queries", func(t *testing.T) {
		ms, filter := rangeScanFixture(t, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		require.NoError(t, ms.ls.db.Callback().Query().After("gorm:query").Register("cancel_after_metadata", func(db *gorm.DB) {
			if !db.DryRun && db.Statement.Table == "bn_partitions" {
				cancel()
			}
		}))
		_, _, err := ms.tryRangeScanLogs(ctx, filter)
		require.ErrorIs(t, err, store.ErrGetLogsTimeout)
	})
}
