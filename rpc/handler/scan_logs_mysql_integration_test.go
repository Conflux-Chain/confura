package handler

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	citypes "github.com/Conflux-Chain/confura/types"
	"github.com/ethereum/go-ethereum/common"
	drivermysql "github.com/go-sql-driver/mysql"
	web3types "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func openScanLogsMySQLIntegrationAdmin(t *testing.T) *gorm.DB {
	t.Helper()
	if os.Getenv("SCANLOGS_RUN_MYSQL_INTEGRATION") != "1" {
		t.Skip("set SCANLOGS_RUN_MYSQL_INTEGRATION=1 to run disposable MySQL integration tests")
	}

	port, err := strconv.Atoi(os.Getenv("SCANLOGS_MYSQL_PORT"))
	require.NoError(t, err)
	cfg := drivermysql.Config{
		User:                 os.Getenv("SCANLOGS_MYSQL_USER"),
		Passwd:               os.Getenv("SCANLOGS_MYSQL_PASSWORD"),
		Net:                  "tcp",
		Addr:                 fmt.Sprintf("%s:%d", os.Getenv("SCANLOGS_MYSQL_HOST"), port),
		AllowNativePasswords: true,
		ParseTime:            true,
		MultiStatements:      true,
	}
	db, err := gorm.Open(gormmysql.Open(cfg.FormatDSN()), &gorm.Config{})
	require.NoError(t, err)
	return db
}

func newScanLogsMySQLIntegrationDB(t *testing.T, admin *gorm.DB, suffix string) *gorm.DB {
	t.Helper()
	name := fmt.Sprintf("scanlogs_it_%d_%s", time.Now().UnixNano(), suffix)
	require.NoError(t, admin.Exec("CREATE DATABASE `"+name+"`").Error)
	t.Cleanup(func() {
		require.NoError(t, admin.Exec("DROP DATABASE `"+name+"`").Error)
	})

	port, err := strconv.Atoi(os.Getenv("SCANLOGS_MYSQL_PORT"))
	require.NoError(t, err)
	cfg := drivermysql.Config{
		User:                 os.Getenv("SCANLOGS_MYSQL_USER"),
		Passwd:               os.Getenv("SCANLOGS_MYSQL_PASSWORD"),
		Net:                  "tcp",
		Addr:                 fmt.Sprintf("%s:%d", os.Getenv("SCANLOGS_MYSQL_HOST"), port),
		DBName:               name,
		AllowNativePasswords: true,
		ParseTime:            true,
	}
	db, err := gorm.Open(gormmysql.Open(cfg.FormatDSN()), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Exec(`
		CREATE TABLE configs (
			id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL UNIQUE,
			value TEXT NOT NULL,
			created_at DATETIME NULL,
			updated_at DATETIME NULL
		)
	`).Error)
	return db
}

func createScanLogsMySQLMappingTable(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Exec(`
		CREATE TABLE epoch_block_map (
			epoch BIGINT UNSIGNED PRIMARY KEY,
			bn_min BIGINT UNSIGNED NOT NULL,
			bn_max BIGINT UNSIGNED NOT NULL,
			pivot_hash VARCHAR(66) NOT NULL
		)
	`).Error)
}

func TestScanLogsDisposableMySQLMappingStates(t *testing.T) {
	admin := openScanLogsMySQLIntegrationAdmin(t)

	t.Run("empty mapping falls back to fullnode", func(t *testing.T) {
		db := newScanLogsMySQLIntegrationDB(t, admin, "empty")
		createScanLogsMySQLMappingTable(t, db)
		hash := common.HexToHash("0x1")
		client := &fakeEthScanClient{blockFn: func(number int64) (*web3types.Block, error) {
			return &web3types.Block{Number: big.NewInt(number), Hash: hash}, nil
		}}

		result, err := newTestEthScanLogsHandler(db).ScanLogs(
			context.Background(), client,
			EthScanLogParams{
				EthScanLogRequest: &EthScanLogRequest{Limit: 1},
				BlockRange:        citypes.RangeUint64{From: 1, To: 1},
			}, nil,
		)
		require.NoError(t, err)
		assert.Empty(t, result.Logs)
		assert.Equal(t, 2, client.blockCalls)
		assert.Len(t, client.filters, 1)
	})

	t.Run("request before earliest is pruned", func(t *testing.T) {
		db := newScanLogsMySQLIntegrationDB(t, admin, "pruned")
		createScanLogsMySQLMappingTable(t, db)
		require.NoError(t, db.Exec(
			"INSERT INTO epoch_block_map (epoch, bn_min, bn_max, pivot_hash) VALUES (10, 10, 10, ?)",
			common.HexToHash("0x10").String(),
		).Error)
		client := &fakeEthScanClient{}

		result, err := newTestEthScanLogsHandler(db).ScanLogs(
			context.Background(), client,
			EthScanLogParams{
				EthScanLogRequest: &EthScanLogRequest{Limit: 1},
				BlockRange:        citypes.RangeUint64{From: 9, To: 9},
			}, nil,
		)
		assert.Nil(t, result)
		assert.ErrorIs(t, err, store.ErrAlreadyPruned)
		assert.Zero(t, client.blockCalls)
		assert.Empty(t, client.filters)
	})

	t.Run("missing mapping schema returns database error", func(t *testing.T) {
		db := newScanLogsMySQLIntegrationDB(t, admin, "schema")
		client := &fakeEthScanClient{}

		result, err := newTestEthScanLogsHandler(db).ScanLogs(
			context.Background(), client,
			EthScanLogParams{
				EthScanLogRequest: &EthScanLogRequest{Limit: 1},
				BlockRange:        citypes.RangeUint64{From: 1, To: 1},
			}, nil,
		)
		assert.Nil(t, result)
		require.Error(t, err)
		assert.NotErrorIs(t, err, store.ErrAlreadyPruned)
		assert.Zero(t, client.blockCalls)
		assert.Empty(t, client.filters)
	})
}
