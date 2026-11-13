//go:build integration

package mysql

import (
	"context"
	"errors"
	"github.com/Conflux-Chain/confura/store"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestMySQLWriteGuardRejectsDataAndMigration(t *testing.T) {
	for _, kind := range []string{"data", "contract", "topic"} {
		t.Run(kind, func(t *testing.T) {
			ms, db := newIntegrationStore(t, false)
			require.NoError(t, ms.Push(integrationData(1, 4)))
			switch kind {
			case "contract":
				require.NoError(t, db.Model(&Contract{}).Where("id > 0").Update("log_count", thresholdBigContractLogCount).Error)
			case "topic":
				require.NoError(t, db.Model(&Topic{}).Where("id > 0").Update("log_count", thresholdBigTopicLogCount).Error)
			}
			denied := errors.New("leadership lost")
			guard := func(*gorm.DB) error { return denied }
			require.ErrorIs(t, ms.PushnWithGuard(context.Background(), []*store.EthData{integrationData(2, 4)}, guard), denied)
			require.EqualValues(t, 4, integrationCount(t, db, "logs_0"))
			require.EqualValues(t, 1, integrationCount(t, db, "epoch_block_map"))
			var n int64
			require.NoError(t, db.Model(&bnPartition{}).Where("entity LIKE ? OR entity LIKE ?", "clogs_%", "tlogs_%").Count(&n).Error)
			require.Zero(t, n, "a rejected migration must not publish a new index route")
			require.ErrorIs(t, ms.PopnWithGuard(context.Background(), 1, guard), denied)
			require.EqualValues(t, 4, integrationCount(t, db, "logs_0"))
		})
	}
}
