//go:build integration

package catchup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/sync/election"
	"github.com/Conflux-Chain/confura/sync/monitor"
	"github.com/stretchr/testify/require"
	driver "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type noPayload struct{}

func (noPayload) IsBlockDisabled() bool   { return true }
func (noPayload) IsTxnDisabled() bool     { return true }
func (noPayload) IsReceiptDisabled() bool { return true }
func (noPayload) IsLogDisabled() bool     { return true }

func TestMySQLCatchupRetryStartsAtOriginalEpoch(t *testing.T) {
	socket := os.Getenv("CONFURA_TEST_MYSQL_SOCKET")
	require.NotEmpty(t, socket)
	gc := &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)}
	admin, err := gorm.Open(driver.Open("root@unix("+socket+")/?parseTime=true"), gc)
	require.NoError(t, err)
	name := fmt.Sprintf("confura_catchup_%d", time.Now().UnixNano())
	require.NoError(t, admin.Exec("CREATE DATABASE "+name).Error)
	db, err := gorm.Open(driver.Open("root@unix("+socket+")/"+name+"?parseTime=true"), gc)
	require.NoError(t, err)
	t.Cleanup(func() { d, _ := db.DB(); d.Close(); admin.Exec("DROP DATABASE " + name); a, _ := admin.DB(); a.Close() })
	require.NoError(t, db.Exec("CREATE TABLE epoch_block_map (epoch BIGINT UNSIGNED PRIMARY KEY,bn_min BIGINT UNSIGNED NOT NULL,bn_max BIGINT UNSIGNED NOT NULL,pivot_hash VARCHAR(66) NOT NULL)").Error)
	ms := mysql.NewMysqlStore[*store.EthData](db, &mysql.Config{Database: name}, noPayload{})
	fetched := make(chan struct{})
	w := mustNewWorker("test", &testClient{query: func(_ context.Context, a, b uint64) ([]*store.EthData, error) {
		if a == 3 {
			select {
			case <-fetched:
			default:
				close(fetched)
			}
		}
		return []*store.EthData{testData(a)}, nil
	}}, 5)
	failure := errors.New("first persistence fails")
	require.NoError(t, db.Callback().Create().Before("gorm:create").Register("test:fail", func(tx *gorm.DB) { <-fetched; tx.AddError(failure) }))
	s := &Syncer[*store.EthData]{config: config{MaxDbRows: 1, DbRowsThreshold: 1}, workers: []*worker[*store.EthData]{w}, db: ms, elm: election.NewNoopLeaderManager(), filter: noPayload{}, monitor: monitor.NewMonitor(monitor.NewConfig(), nil)}
	s.doSync(context.Background(), nil, 1, 3)
	require.NotEmpty(t, w.resultChan, "real workers buffered later results before the failure")
	_, ok, err := ms.MaxEpoch()
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, db.Callback().Create().Remove("test:fail"))
	s.doSync(context.Background(), nil, 1, 3)
	n, ok, err := ms.MaxEpoch()
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 3, n)
	var epochs []uint64
	require.NoError(t, db.Table("epoch_block_map").Order("epoch").Pluck("epoch", &epochs).Error)
	require.Equal(t, []uint64{1, 2, 3}, epochs)
}
