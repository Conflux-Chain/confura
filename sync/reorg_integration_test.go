//go:build integration

package sync

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/sync/election"
	"github.com/Conflux-Chain/confura/sync/monitor"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	ct "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/openweb3/go-rpc-provider/interfaces"
	"github.com/openweb3/web3go"
	et "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
	driver "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type syncTestFilter struct{}

func (syncTestFilter) IsBlockDisabled() bool   { return false }
func (syncTestFilter) IsTxnDisabled() bool     { return false }
func (syncTestFilter) IsReceiptDisabled() bool { return false }
func (syncTestFilter) IsLogDisabled() bool     { return false }

type syncTestProvider struct {
	interfaces.Provider
	call func(context.Context, interface{}, string, ...interface{}) error
}

func (p *syncTestProvider) CallContext(c context.Context, o interface{}, m string, a ...interface{}) error {
	return p.call(c, o, m, a...)
}
func (*syncTestProvider) Close() {}

func openSyncTestDB(t *testing.T, name string) *gorm.DB {
	t.Helper()
	socket := os.Getenv("CONFURA_TEST_MYSQL_SOCKET")
	require.NotEmpty(t, socket, "use a disposable local MySQL instance")
	db, err := gorm.Open(driver.Open("root@unix("+socket+")/"+name+"?parseTime=true&timeout=3s&readTimeout=10s&writeTimeout=10s"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	t.Cleanup(func() { d, _ := db.DB(); d.Close() })
	return db
}

func newSyncTestDB(t *testing.T) (*gorm.DB, *mysql.Config) {
	t.Helper()
	admin := openSyncTestDB(t, "")
	name := fmt.Sprintf("confura_sync_%d", time.Now().UnixNano())
	require.NoError(t, admin.Exec("CREATE DATABASE "+name).Error)
	t.Cleanup(func() { require.NoError(t, admin.Exec("DROP DATABASE "+name).Error) })
	db := openSyncTestDB(t, name)
	for _, ddl := range []string{
		"CREATE TABLE epoch_block_map (epoch BIGINT UNSIGNED PRIMARY KEY,bn_min BIGINT UNSIGNED NOT NULL,bn_max BIGINT UNSIGNED NOT NULL,pivot_hash VARCHAR(66) NOT NULL)",
		"CREATE TABLE bn_partitions (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,entity VARCHAR(64) NOT NULL,pi INT UNSIGNED NOT NULL,count INT UNSIGNED NOT NULL DEFAULT 0,bn_min BIGINT NULL,bn_max BIGINT NULL,created_at DATETIME(3),updated_at DATETIME(3),UNIQUE KEY uidx_entity_pi(entity,pi))",
		"CREATE TABLE configs (id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,name VARCHAR(128) NOT NULL UNIQUE,value TEXT NOT NULL,created_at DATETIME(3),updated_at DATETIME(3))",
		"CREATE TABLE blocks (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,epoch BIGINT UNSIGNED NOT NULL,block_number BIGINT UNSIGNED NOT NULL,hash_id BIGINT UNSIGNED NOT NULL,hash VARCHAR(66) NOT NULL,pivot BOOL NOT NULL,extra MEDIUMTEXT)",
		"CREATE TABLE txs (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,epoch BIGINT UNSIGNED NOT NULL,hash_id BIGINT UNSIGNED NOT NULL,hash VARCHAR(66) NOT NULL,num_receipt_logs INT NOT NULL,extra MEDIUMTEXT,receipt_extra MEDIUMTEXT)",
	} {
		require.NoError(t, db.Exec(ddl).Error)
	}
	require.NoError(t, db.Migrator().CreateTable(&mysql.Contract{}, &mysql.Topic{}))
	cs, ts := mysql.NewContractStore(db), mysql.NewTopicStore(db)
	ails := mysql.NewAddressIndexedLogStore[store.ChainData](db, cs, ts, 2)
	_, err := ails.CreatePartitionedTables()
	require.NoError(t, err)
	tils := mysql.NewTopicIndexedLogStore[store.ChainData](db, ts, 2)
	_, err = tils.CreatePartitionedTables()
	require.NoError(t, err)
	return db, &mysql.Config{Database: name, AddressIndexedLogPartitions: 2, TopicIndexedLogPartitions: 2}
}

func syncTestEth(n uint64, fork byte) *store.EthData {
	hash := common.HexToHash(fmt.Sprintf("0x%02x%062x", fork, n))
	parent := common.HexToHash(fmt.Sprintf("0x%02x%062x", fork, n-1))
	return &store.EthData{BlockNo: n, Block: &et.Block{Number: new(big.Int).SetUint64(n), Difficulty: big.NewInt(0), Hash: hash, ParentHash: parent}, Receipts: map[common.Hash]*et.Receipt{}}
}
func syncTestCore(n uint64, fork byte) *store.EpochData {
	e := syncTestEth(n, fork)
	h := ct.Hash(e.Block.Hash.Hex())
	b := &ct.Block{BlockHeader: ct.BlockHeader{Hash: h, DeferredStateRoot: h, DeferredReceiptsRoot: h, DeferredLogsBloomHash: h, TransactionsRoot: h, ParentHash: ct.Hash(e.Block.ParentHash.Hex()), EpochNumber: ct.NewBigInt(n), BlockNumber: ct.NewBigInt(n), GasUsed: ct.NewBigInt(0)}}
	return &store.EpochData{EpochNo: n, PivotHash: &h, Blocks: []*ct.Block{b}, Receipts: map[ct.Hash]*ct.TransactionReceipt{}}
}
func seedSyncEth(t *testing.T, ms *mysql.EthStore) {
	for n := uint64(0); n <= 3; n++ {
		d := syncTestEth(n, 'A')
		if n > 0 {
			th, status := common.BigToHash(new(big.Int).SetUint64(n)), uint64(1)
			d.Block.Transactions = *et.NewTxOrHashListByTxs([]et.TransactionDetail{{Hash: th, Status: &status, Value: big.NewInt(0)}})
			d.Receipts[th] = &et.Receipt{BlockNumber: n, BlockHash: d.Block.Hash, TransactionHash: th, Status: &status, Logs: []*et.Log{{BlockNumber: n, BlockHash: d.Block.Hash, TxHash: th, Address: common.HexToAddress("0x1"), Topics: []common.Hash{common.HexToHash("0x2")}}}}
		}
		require.NoError(t, ms.Push(d))
	}
}

func TestMySQLEthWaitsForNewHead(t *testing.T) {
	for _, initialHead := range []uint64{3, 2} {
		t.Run(fmt.Sprint(initialHead), func(t *testing.T) {
			head := initialHead
			db, cfg := newSyncTestDB(t)
			ms := mysql.NewEthStore(db, cfg, syncTestFilter{})
			seedSyncEth(t, ms)
			fork := byte('A')
			var calls []string
			cli := web3go.NewClientWithProvider(&syncTestProvider{call: func(_ context.Context, o interface{}, m string, a ...interface{}) error {
				calls = append(calls, m)
				if m != "eth_getBlockByNumber" {
					return fmt.Errorf("unexpected %s", m)
				}
				n := a[0].(et.BlockNumber)
				if n < 0 {
					n = et.BlockNumber(head)
				}
				d := syncTestEth(uint64(n), fork)
				if n == 0 {
					d = syncTestEth(0, 'A')
				}
				if n == 1 {
					d.Block.ParentHash = syncTestEth(0, 'A').Block.Hash
				}
				*o.(**et.Block) = d.Block
				return nil
			}})
			fresh := func() *EthSyncer {
				return &EthSyncer{conf: &ethSyncConfig{}, w3cs: []*web3go.Client{cli}, db: ms, maxSyncBlocks: 1, pivotWin: newEvmSlidingPivotWindow(1), elm: election.NewNoopLeaderManager(), monitor: monitor.NewMonitor(monitor.NewConfig(), nil)}
			}
			s := fresh()
			done, err := s.syncOnce(context.Background())
			require.NoError(t, err)
			require.True(t, done)
			n, _, err := ms.MaxEpoch()
			require.NoError(t, err)
			require.EqualValues(t, 3, n, "lagging same-chain node must not delete data")
			fork = 'B'
			calls = nil
			done, err = s.syncOnce(context.Background())
			require.NoError(t, err)
			require.True(t, done, "wait while the node has no height beyond the database")
			require.Equal(t, []string{"eth_getBlockByNumber"}, calls, "do not add a canonical-history RPC")
			require.EqualValues(t, 4, s.fromBlock)
			n, _, err = ms.MaxEpoch()
			require.NoError(t, err)
			require.EqualValues(t, 3, n)
			stored, err := ms.GetBlockByNumber(et.BlockNumber(3), false)
			require.NoError(t, err)
			require.Equal(t, syncTestEth(3, 'A').Block.Hash, stored.Hash)
			logs, err := ms.GetLogs(context.Background(), store.LogFilter{BlockFrom: 1, BlockTo: 3})
			require.NoError(t, err)
			require.Len(t, logs, 3, "waiting must preserve the old view")

			// Existing parent-hash validation starts recovery when a new block arrives.
			head = 4
			fail := fmt.Errorf("fixed rollback failure")
			require.NoError(t, db.Callback().Delete().Before("gorm:delete").Register("test:rollback", func(tx *gorm.DB) { tx.AddError(fail) }))
			done, err = s.syncOnce(context.Background())
			require.ErrorIs(t, err, fail)
			require.False(t, done)
			require.EqualValues(t, 4, s.fromBlock)
			n, _, err = ms.MaxEpoch()
			require.NoError(t, err)
			require.EqualValues(t, 3, n)
			require.NoError(t, db.Callback().Delete().Remove("test:rollback"))
			done, err = s.syncOnce(context.Background())
			require.NoError(t, err)
			require.False(t, done)
			require.EqualValues(t, 3, s.fromBlock)
			// A new process reloads the rollback watermark and finds the ancestor even
			// when the fork is deeper than its one-block pivot cache.
			s = fresh()
			for range 8 {
				_, err = s.syncOnce(context.Background())
				require.NoError(t, err)
			}
			require.Equal(t, head+1, s.fromBlock)
			for h := uint64(1); h <= head; h++ {
				b, err := ms.GetBlockByNumber(et.BlockNumber(h), false)
				require.NoError(t, err)
				require.Equal(t, syncTestEth(h, 'B').Block.Hash, b.Hash)
			}
			got, err := ms.GetLogs(context.Background(), store.LogFilter{BlockFrom: 1, BlockTo: 3})
			require.NoError(t, err)
			require.Empty(t, got, "old fork events must disappear from the database consumer")
			for _, table := range []string{"txs", "logs_0"} {
				var count int64
				require.NoError(t, db.Table(table).Count(&count).Error)
				require.Zero(t, count, table)
			}
		})
	}
}

func TestMySQLCoreWaitsForNewHead(t *testing.T) {
	for _, initialHead := range []uint64{3, 2} {
		t.Run(fmt.Sprint(initialHead), func(t *testing.T) {
			head := initialHead
			db, cfg := newSyncTestDB(t)
			ms := mysql.NewCfxStore(db, cfg, syncTestFilter{})
			for n := uint64(0); n <= 3; n++ {
				require.NoError(t, ms.Push(syncTestCore(n, 'A')))
			}
			fork := byte('A')
			var calls []string
			block := func(n uint64) *ct.Block {
				d := syncTestCore(n, fork)
				if n == 0 {
					d = syncTestCore(0, 'A')
				}
				if n == 1 {
					d.Blocks[0].ParentHash = syncTestCore(0, 'A').Blocks[0].Hash
				}
				return d.Blocks[0]
			}
			cli, err := sdk.NewClientWithProvider(&syncTestProvider{call: func(_ context.Context, o interface{}, m string, a ...interface{}) error {
				calls = append(calls, m)
				switch m {
				case "cfx_getStatus":
					*o.(*ct.Status) = ct.Status{NetworkID: 1}
				case "cfx_epochNumber":
					*o.(**hexutil.Big) = ct.NewBigInt(head)
				case "cfx_getBlockByEpochNumber":
					n, _ := a[0].(*ct.Epoch).ToInt()
					*o.(**ct.Block) = block(n.Uint64())
				case "cfx_getBlocksByEpoch":
					n, _ := a[0].(*ct.Epoch).ToInt()
					*o.(*[]ct.Hash) = []ct.Hash{block(n.Uint64()).Hash}
				case "cfx_getBlockByHashWithPivotAssumption":
					*o.(*ct.Block) = *block(uint64(a[2].(hexutil.Uint64)))
				default:
					return fmt.Errorf("unexpected %s", m)
				}
				return nil
			}})
			require.NoError(t, err)
			fresh := func() *DatabaseSyncer {
				return &DatabaseSyncer{conf: &cfxSyncConfig{}, cfxs: []*sdk.Client{cli}, db: ms, maxSyncEpochs: 1, pivotWin: newCfxSlidingPivotWindow(1), elm: election.NewNoopLeaderManager(), monitor: monitor.NewMonitor(monitor.NewConfig(), nil)}
			}
			s := fresh()
			done, err := s.syncOnce(context.Background())
			require.NoError(t, err)
			require.True(t, done)
			n, _, err := ms.MaxEpoch()
			require.NoError(t, err)
			require.EqualValues(t, 3, n)
			fork = 'B'
			calls = nil
			done, err = s.syncOnce(context.Background())
			require.NoError(t, err)
			require.True(t, done, "wait while the node has no epoch beyond the database")
			require.Equal(t, []string{"cfx_epochNumber"}, calls, "do not fetch the overlapping pivot")
			require.EqualValues(t, 4, s.epochFrom)
			n, _, err = ms.MaxEpoch()
			require.NoError(t, err)
			require.EqualValues(t, 3, n)
			stored, err := ms.GetBlockSummaryByEpoch(context.Background(), 3)
			require.NoError(t, err)
			require.Equal(t, syncTestCore(3, 'A').Blocks[0].Hash, stored.Hash)

			// Existing parent-hash validation starts recovery when a new epoch arrives.
			head = 4
			done, err = s.syncOnce(context.Background())
			require.NoError(t, err)
			require.False(t, done)
			require.EqualValues(t, 3, s.epochFrom)
			s = fresh()
			for range 8 {
				_, err = s.syncOnce(context.Background())
				require.NoError(t, err)
			}
			require.Equal(t, head+1, s.epochFrom)
			for h := uint64(1); h <= head; h++ {
				b, err := ms.GetBlockSummaryByEpoch(context.Background(), h)
				require.NoError(t, err)
				require.Equal(t, syncTestCore(h, 'B').Blocks[0].Hash, b.Hash)
			}
			require.Error(t, s.pivotSwitchRevert(context.Background(), 0))
		})
	}
}

func TestMySQLGenesisMismatchOnNewHead(t *testing.T) {
	t.Run("evm", func(t *testing.T) {
		db, cfg := newSyncTestDB(t)
		ms := mysql.NewEthStore(db, cfg, syncTestFilter{})
		block := syncTestEth(0, 'A').Block
		cli := web3go.NewClientWithProvider(&syncTestProvider{call: func(_ context.Context, o interface{}, m string, _ ...interface{}) error {
			if m != "eth_getBlockByNumber" {
				return fmt.Errorf("unexpected %s", m)
			}
			*o.(**et.Block) = block
			return nil
		}})
		s := &EthSyncer{conf: &ethSyncConfig{}, db: ms, w3cs: []*web3go.Client{cli}, maxSyncBlocks: 1, pivotWin: newEvmSlidingPivotWindow(1), elm: election.NewNoopLeaderManager(), monitor: monitor.NewMonitor(monitor.NewConfig(), nil)}
		done, err := s.syncOnce(context.Background())
		require.NoError(t, err)
		require.False(t, done)
		done, err = s.syncOnce(context.Background())
		require.NoError(t, err)
		require.True(t, done)
		block = syncTestEth(0, 'B').Block
		done, err = s.syncOnce(context.Background())
		require.NoError(t, err)
		require.True(t, done)
		block = syncTestEth(1, 'B').Block
		done, err = s.syncOnce(context.Background())
		require.ErrorContains(t, err, "genesis")
		require.False(t, done)
		hash, ok, err := ms.PivotHash(0)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, syncTestEth(0, 'A').Hash(), hash)
	})
	t.Run("core", func(t *testing.T) {
		db, cfg := newSyncTestDB(t)
		ms := mysql.NewCfxStore(db, cfg, syncTestFilter{})
		block := syncTestCore(0, 'A').Blocks[0]
		cli, err := sdk.NewClientWithProvider(&syncTestProvider{call: func(_ context.Context, o interface{}, m string, _ ...interface{}) error {
			switch m {
			case "cfx_getStatus":
				*o.(*ct.Status) = ct.Status{NetworkID: 1}
			case "cfx_epochNumber":
				*o.(**hexutil.Big) = block.EpochNumber
			case "cfx_getBlocksByEpoch":
				*o.(*[]ct.Hash) = []ct.Hash{block.Hash}
			case "cfx_getBlockByHashWithPivotAssumption":
				*o.(*ct.Block) = *block
			case "cfx_getBlockByEpochNumber":
				*o.(**ct.Block) = block
			default:
				return fmt.Errorf("unexpected %s", m)
			}
			return nil
		}})
		require.NoError(t, err)
		s := &DatabaseSyncer{conf: &cfxSyncConfig{}, db: ms, cfxs: []*sdk.Client{cli}, maxSyncEpochs: 1, pivotWin: newCfxSlidingPivotWindow(1), elm: election.NewNoopLeaderManager(), monitor: monitor.NewMonitor(monitor.NewConfig(), nil)}
		done, err := s.syncOnce(context.Background())
		require.NoError(t, err)
		require.False(t, done)
		done, err = s.syncOnce(context.Background())
		require.NoError(t, err)
		require.True(t, done)
		block = syncTestCore(0, 'B').Blocks[0]
		done, err = s.syncOnce(context.Background())
		require.NoError(t, err)
		require.True(t, done)
		block = syncTestCore(1, 'B').Blocks[0]
		done, err = s.syncOnce(context.Background())
		require.ErrorContains(t, err, "genesis")
		require.False(t, done)
		hash, ok, err := ms.PivotHash(0)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, syncTestCore(0, 'A').Hash(), hash)
	})
}
