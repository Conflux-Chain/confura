//go:build integration

package election

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/go-conflux-util/dlock"
	"github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
	driver "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type guardTestFilter struct{}

func (guardTestFilter) IsBlockDisabled() bool   { return true }
func (guardTestFilter) IsTxnDisabled() bool     { return true }
func (guardTestFilter) IsReceiptDisabled() bool { return true }
func (guardTestFilter) IsLogDisabled() bool     { return true }

func newGuardTestDB(t *testing.T) (*gorm.DB, *mysql.MysqlStore[*store.EthData]) {
	t.Helper()
	socket := os.Getenv("CONFURA_TEST_MYSQL_SOCKET")
	require.NotEmpty(t, socket, "a disposable MySQL socket is required")
	cfg := &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)}
	admin, err := gorm.Open(driver.Open("root@unix("+socket+")/?parseTime=true"), cfg)
	require.NoError(t, err)
	name := fmt.Sprintf("confura_guard_%d", time.Now().UnixNano())
	require.NoError(t, admin.Exec("CREATE DATABASE "+name).Error)
	db, err := gorm.Open(driver.Open("root@unix("+socket+")/"+name+"?parseTime=true&timeout=3s&readTimeout=5s&writeTimeout=5s"), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { d, _ := db.DB(); d.Close(); admin.Exec("DROP DATABASE " + name); a, _ := admin.DB(); a.Close() })
	require.NoError(t, db.AutoMigrate(&dlock.Dlock{}))
	require.NoError(t, db.Exec("CREATE TABLE epoch_block_map (epoch BIGINT UNSIGNED PRIMARY KEY, bn_min BIGINT UNSIGNED NOT NULL, bn_max BIGINT UNSIGNED NOT NULL, pivot_hash VARCHAR(66) NOT NULL)").Error)
	require.NoError(t, db.Exec("CREATE TABLE configs (id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, name VARCHAR(128) UNIQUE, value VARCHAR(16250), created_at DATETIME(3), updated_at DATETIME(3))").Error)
	ms := mysql.NewMysqlStore[*store.EthData](db, &mysql.Config{Database: name}, guardTestFilter{})
	return db, ms
}

func TestMySQLLeadershipWriteGuard(t *testing.T) {
	db, ms := newGuardTestDB(t)
	a := NewDlockLeaderManager(db, Config{Lease: time.Second, Retry: time.Millisecond}, "sync.test")
	b := NewDlockLeaderManager(db, Config{Lease: time.Second, Retry: time.Millisecond}, "sync.test")
	ctx := context.Background()
	require.NoError(t, a.retryLock(ctx))
	a.onElected(ctx)
	oldCtx := WithLeadership(ctx, a)
	oldGuard := a.Guard(oldCtx)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	data := func(n uint64) []*store.EthData {
		return []*store.EthData{{BlockNo: n, Block: &types.Block{Number: new(big.Int).SetUint64(n)}}}
	}
	go func() {
		done <- ms.PushnWithGuard(ctx, data(1), func(tx *gorm.DB) error {
			if err := oldGuard(tx); err != nil {
				return err
			}
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	// A's lease has elapsed while its authorized transaction remains open.
	time.Sleep(2100 * time.Millisecond)
	attempt, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	err := b.acquireLock(attempt)
	cancel()
	require.Error(t, err, "B cannot acquire a lease row locked by A's data transaction")
	close(release)
	require.NoError(t, <-done)
	require.NoError(t, b.retryLock(ctx))
	b.onElected(ctx)
	require.ErrorIs(t, ms.PushnWithGuard(ctx, data(2), oldGuard), store.ErrLeaderRenewal)
	require.ErrorIs(t, ms.PopnWithGuard(ctx, 1, oldGuard), store.ErrLeaderRenewal)
	n, ok, err := ms.MaxEpoch()
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 1, n)
	require.NoError(t, ms.PushnWithGuard(ctx, data(2), b.Guard(WithLeadership(ctx, b))))
	require.NoError(t, b.Stop())
	require.NoError(t, b.Stop(), "stopping an already released leader remains idempotent")
	b.onOusted(ctx)
	a.onOusted(ctx)
	require.NoError(t, a.retryLock(ctx))
	a.onElected(ctx)
	require.ErrorIs(t, ms.PushnWithGuard(ctx, data(3), a.Guard(oldCtx)), store.ErrLeaderRenewal)
	require.ErrorIs(t, a.Extend(oldCtx), store.ErrLeaderRenewal)
	require.NoError(t, ms.PushnWithGuard(ctx, data(3), a.Guard(WithLeadership(ctx, a))))
	require.NoError(t, db.Model(&dlock.Dlock{}).Where("`key` = ?", "sync.test").Update("expired_at", gorm.Expr("NOW() - INTERVAL 1 SECOND")).Error)
	require.ErrorIs(t, a.Extend(ctx), store.ErrLeaderRenewal, "renewal must not reacquire an expired term")
}
