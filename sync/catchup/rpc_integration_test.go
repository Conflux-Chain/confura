//go:build integration

package catchup

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/sync/election"
	"github.com/Conflux-Chain/confura/sync/monitor"
	"github.com/Conflux-Chain/confura/types"
	"github.com/ethereum/go-ethereum/common"
	et "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	driver "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type logsOnly struct{ noPayload }

func (logsOnly) IsLogDisabled() bool { return false }

func newBoostLogStore(t *testing.T) (*mysql.MysqlStore[*store.EthData], *gorm.DB) {
	t.Helper()
	socket := os.Getenv("CONFURA_TEST_MYSQL_SOCKET")
	require.NotEmpty(t, socket, "use a disposable local MySQL instance")
	gc := &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)}
	admin, err := gorm.Open(driver.Open("root@unix("+socket+")/?parseTime=true"), gc)
	require.NoError(t, err)
	name := fmt.Sprintf("confura_boost_%d", time.Now().UnixNano())
	require.NoError(t, admin.Exec("CREATE DATABASE "+name).Error)
	db, err := gorm.Open(driver.Open("root@unix("+socket+")/"+name+"?parseTime=true"), gc)
	require.NoError(t, err)
	t.Cleanup(func() { d, _ := db.DB(); d.Close(); admin.Exec("DROP DATABASE " + name); a, _ := admin.DB(); a.Close() })
	// Only metadata tables are created here. All log tables and partitions are
	// created by the production stores, with their real indexes and write paths.
	for _, ddl := range []string{
		"CREATE TABLE epoch_block_map (epoch BIGINT UNSIGNED PRIMARY KEY,bn_min BIGINT UNSIGNED NOT NULL,bn_max BIGINT UNSIGNED NOT NULL,pivot_hash VARCHAR(66) NOT NULL)",
		"CREATE TABLE bn_partitions (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,entity VARCHAR(64) NOT NULL,pi INT UNSIGNED NOT NULL,count INT UNSIGNED NOT NULL DEFAULT 0,bn_min BIGINT NULL,bn_max BIGINT NULL,created_at DATETIME(3),updated_at DATETIME(3),UNIQUE KEY uidx_entity_pi(entity,pi))",
		"CREATE TABLE configs (id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,name VARCHAR(128) NOT NULL UNIQUE,value TEXT NOT NULL,created_at DATETIME(3),updated_at DATETIME(3))",
	} {
		require.NoError(t, db.Exec(ddl).Error)
	}
	require.NoError(t, db.Migrator().CreateTable(&mysql.Contract{}, &mysql.Topic{}))
	cfg := &mysql.Config{Database: name, AddressIndexedLogPartitions: 2, TopicIndexedLogPartitions: 2}
	cs, ts := mysql.NewContractStore(db), mysql.NewTopicStore(db)
	ails := mysql.NewAddressIndexedLogStore[*store.EthData](db, cs, ts, 2)
	_, err = ails.CreatePartitionedTables()
	require.NoError(t, err)
	tils := mysql.NewTopicIndexedLogStore[*store.EthData](db, ts, 2)
	_, err = tils.CreatePartitionedTables()
	require.NoError(t, err)
	return mysql.NewMysqlStore[*store.EthData](db, cfg, logsOnly{}), db
}

func TestMySQLBoostPreservesCanonicalLogs(t *testing.T) {
	ms, _ := newBoostLogStore(t)
	block, logs := evmLogFixture()
	data, err := fixtureEvmClient(block, logs).BoostQueryChainData(context.Background(), 1, 1)
	require.NoError(t, err)
	require.NoError(t, ms.Pushn(data))
	got, err := ms.GetLogs(context.Background(), store.LogFilter{BlockFrom: 1, BlockTo: 1})
	require.NoError(t, err)
	require.Len(t, got, 1)
	event, err := got[0].ToEthLog()
	require.NoError(t, err)
	require.Equal(t, logs[0].BlockHash, event.BlockHash)
	require.Equal(t, logs[0].TxHash, event.TxHash)
	require.EqualValues(t, logs[0].Index, got[0].LogIndex)
	// The pure database consumer must no longer see the old canonical event after Pop.
	require.NoError(t, ms.Popn(1))
	got, err = ms.GetLogs(context.Background(), store.LogFilter{BlockFrom: 1, BlockTo: 1})
	require.NoError(t, err)
	require.Empty(t, got)
	_, ok, err := ms.MaxEpoch()
	require.NoError(t, err)
	require.False(t, ok)
}

func TestMySQLBoostPressureRetryPreservesLogs(t *testing.T) {
	ms, db := newBoostLogStore(t)
	fixtures := make(map[uint64]*store.EthData)
	for n := uint64(1); n <= 8; n++ {
		block, logs := evmLogFixture()
		block.Number = new(big.Int).SetUint64(n)
		block.Hash = common.BigToHash(new(big.Int).SetUint64(1000 + n))
		txHash := common.BigToHash(new(big.Int).SetUint64(2000 + n))
		block.Transactions = *et.NewTxOrHashListByTxs([]et.TransactionDetail{{Hash: txHash, Value: big.NewInt(0)}})
		logs[0].BlockNumber, logs[0].BlockHash, logs[0].TxHash = n, block.Hash, txHash
		data, err := fixtureEvmClient(block, logs).BoostQueryChainData(context.Background(), n, n)
		require.NoError(t, err)
		fixtures[n] = data[0]
	}
	runAttempt := func(start uint64, failQuery bool) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		release, laterStarted := make(chan struct{}), make(chan struct{})
		var earliest, later sync.Once
		client := &testClient{query: func(_ context.Context, a, b uint64) (result []*store.EthData, err error) {
			failed := false
			if a == start {
				earliest.Do(func() { <-release; failed = failQuery })
			} else {
				later.Do(func() { close(laterStarted) })
			}
			if failed {
				return nil, fmt.Errorf("retry earliest range")
			}
			for n := a; n <= b; n++ {
				result = append(result, fixtures[n])
			}
			return result, nil
		}}
		cfg := coordinatorConfig()
		cfg.DefaultTaskSize, cfg.MemoryThreshold = 2, 1
		cfg.MemoryCheckInterval, cfg.ForcePersistenceInterval = time.Millisecond, time.Second
		var workers []*worker[*store.EthData]
		for range 2 {
			workers = append(workers, mustNewWorker("test", client, 1))
		}
		s := &Syncer[*store.EthData]{config: config{Boost: cfg, DbRowsThreshold: 1}, workers: workers, db: ms,
			elm: election.NewNoopLeaderManager(), filter: logsOnly{}, monitor: monitor.NewMonitor(monitor.NewConfig(), nil)}
		boost := newBoostSyncer(s)
		coord := newCoordinator(cfg, boost.workers, types.RangeUint64{From: start, To: 8}, boost.resultChan)
		var wg sync.WaitGroup
		wg.Add(1)
		go coord.run(ctx, &wg)
		// Hold the first range until a later range starts and the real monitor
		// enables pressure. This fixes the ordering without relying on RPC timing.
		select {
		case <-laterStarted:
		case <-ctx.Done():
			close(release)
			wg.Wait()
			t.Fatal("later worker did not start")
		}
		wg.Add(1)
		go func() { defer wg.Done(); boost.memoryMonitorLoop(ctx, coord) }()
		pressured := assert.Eventually(t, coord.isBackpressured, time.Second, time.Millisecond)
		close(release)
		err := boost.fetchAndPersistResults(ctx, start, 8, nil)
		cancel()
		wg.Wait()
		require.True(t, pressured)
		if failQuery {
			require.ErrorContains(t, err, "injected database failure")
		} else {
			require.NoError(t, err)
		}
	}
	var writes atomic.Int32
	const callbackName = "test:fail_third_boost_write"
	require.NoError(t, db.Callback().Create().Before("gorm:create").Register(callbackName, func(tx *gorm.DB) {
		if tx.Statement.Table == "epoch_block_map" && writes.Add(1) == 3 {
			tx.AddError(fmt.Errorf("injected database failure"))
		}
	}))
	t.Cleanup(func() {
		if db.Callback().Create().Get(callbackName) != nil {
			db.Callback().Create().Remove(callbackName)
		}
	})
	runAttempt(1, true)
	n, ok, err := ms.MaxEpoch()
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 2, n)
	got, err := ms.GetLogs(context.Background(), store.LogFilter{BlockFrom: 1, BlockTo: 8})
	require.NoError(t, err)
	require.Len(t, got, 2, "the failed third write must not publish a log")
	require.NoError(t, db.Callback().Create().Remove(callbackName))
	runAttempt(n+1, false)
	n, ok, err = ms.MaxEpoch()
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 8, n)
	got, err = ms.GetLogs(context.Background(), store.LogFilter{BlockFrom: 1, BlockTo: 8})
	require.NoError(t, err)
	require.Len(t, got, 8)
	for i, log := range got {
		event, err := log.ToEthLog()
		require.NoError(t, err)
		n := uint64(i + 1)
		require.Equal(t, n, event.BlockNumber)
		require.Equal(t, fixtures[n].Block.Hash, event.BlockHash)
		require.Equal(t, common.BigToHash(new(big.Int).SetUint64(2000+n)), event.TxHash)
		require.Zero(t, event.Index)
	}
}
