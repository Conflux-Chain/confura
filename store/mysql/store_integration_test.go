//go:build integration

package mysql

import (
	"fmt"
	"math/big"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
	driver "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var integrationDBSequence atomic.Uint64

type integrationFilter struct{ all bool }

func (f integrationFilter) IsBlockDisabled() bool   { return !f.all }
func (f integrationFilter) IsTxnDisabled() bool     { return !f.all }
func (f integrationFilter) IsReceiptDisabled() bool { return !f.all }
func (f integrationFilter) IsLogDisabled() bool     { return false }

func newIntegrationDB(t *testing.T) (*gorm.DB, *Config) {
	t.Helper()
	// Explicit opt-in: never read application configuration or credentials.
	socket := os.Getenv("CONFURA_TEST_MYSQL_SOCKET")
	if socket == "" {
		t.Fatal("integration tests require CONFURA_TEST_MYSQL_SOCKET pointing to a disposable local MySQL instance")
	}
	cfg := &gorm.Config{Logger: logger.Default.LogMode(logger.Silent), CreateBatchSize: 200}
	admin, err := gorm.Open(driver.Open("root@unix("+socket+")/?parseTime=true"), cfg)
	require.NoError(t, err)
	name := fmt.Sprintf("confura_test_%d_%d", time.Now().UnixNano(), integrationDBSequence.Add(1))
	require.NoError(t, admin.Exec("CREATE DATABASE "+name).Error)
	db, err := gorm.Open(driver.Open("root@unix("+socket+")/"+name+"?parseTime=true&timeout=3s&readTimeout=10s&writeTimeout=10s"), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { s, _ := db.DB(); s.Close(); admin.Exec("DROP DATABASE " + name); a, _ := admin.DB(); a.Close() })
	require.NoError(t, db.Migrator().CreateTable(allModels...))
	return db, &Config{Database: name, AddressIndexedLogPartitions: 2, TopicIndexedLogPartitions: 2, CreateBatchSize: 200}
}
func newIntegrationStore(t *testing.T, all bool) (*MysqlStore[*store.EthData], *gorm.DB) {
	db, cfg := newIntegrationDB(t)
	ms := NewMysqlStore[*store.EthData](db, cfg, integrationFilter{all})
	t.Cleanup(func() { close(ms.pruner.newBnPartitionObsChan) })
	_, err := ms.ails.CreatePartitionedTables()
	require.NoError(t, err)
	_, err = ms.tils.CreatePartitionedTables()
	require.NoError(t, err)
	return ms, db
}
func integrationData(n uint64, count int) *store.EthData {
	bh := common.BigToHash(new(big.Int).SetUint64(n + 100))
	status := uint64(1)
	txs := []ethtypes.TransactionDetail{}
	receipts := map[common.Hash]*ethtypes.Receipt{}
	for j := 0; j < 2; j++ {
		th := common.BigToHash(new(big.Int).SetUint64(n*10 + uint64(j) + 1000))
		txs = append(txs, ethtypes.TransactionDetail{Hash: th, Status: &status, Value: big.NewInt(0)})
		receipts[th] = &ethtypes.Receipt{BlockNumber: n, BlockHash: bh, TransactionHash: th, Status: &status}
	}
	for i := 0; i < count; i++ {
		th := txs[i%2].Hash
		topics := []common.Hash{common.BigToHash(big.NewInt(int64(i%2 + 1))), common.BigToHash(big.NewInt(int64(i%3 + 10)))}
		receipts[th].Logs = append(receipts[th].Logs, &ethtypes.Log{BlockNumber: n, BlockHash: bh, TxHash: th, Index: uint(i), Address: common.BigToAddress(big.NewInt(int64(i%2 + 1))), Topics: topics})
	}
	return &store.EthData{BlockNo: n, Block: &ethtypes.Block{Number: new(big.Int).SetUint64(n), Hash: bh, Transactions: *ethtypes.NewTxOrHashListByTxs(txs)}, Receipts: receipts}
}
func integrationCount(t *testing.T, db *gorm.DB, table string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Table(table).Count(&n).Error)
	return n
}

func TestMySQLStoreFixture(t *testing.T) {
	ms, db := newIntegrationStore(t, true)
	require.NoError(t, ms.Push(integrationData(1, 4)))
	n, ok, err := ms.MaxEpoch()
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 1, n)
	require.EqualValues(t, 4, integrationCount(t, db, "logs_0"))
}
