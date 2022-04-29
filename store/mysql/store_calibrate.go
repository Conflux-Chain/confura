package mysql

import (
	"database/sql"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/conflux-chain/conflux-infura/store"
	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// calibrateEpochStats calibrates epoch statistics by running MySQL OLAP.
func (ms *mysqlStore) calibrateEpochStats() error {
	var count int64
	if err := ms.db.Model(&epochStats{}).Count(&count).Error; err != nil {
		return errors.WithMessage(err, "failed to count epoch stats table")
	}

	if count > 0 { // already calibrated with records in epoch_stats table
		return ms.loadCalibratedEpochStats()
	}

	epochRanges := make(map[store.EpochDataType]*citypes.RangeUint64)

	for _, t := range store.OpEpochDataTypes {
		// load epoch range
		minEpoch, maxEpoch, err := ms.loadEpochRange(t)

		if err != nil && !ms.IsRecordNotFound(err) {
			return errors.WithMessage(err, "failed to load epoch range")
		}

		er := citypes.RangeUint64{minEpoch, maxEpoch}
		if err == nil { // update global epoch range
			ms.minEpoch = util.MinUint64(ms.minEpoch, minEpoch)

			if ms.maxEpoch != math.MaxUint64 {
				ms.maxEpoch = util.MaxUint64(ms.maxEpoch, maxEpoch)
			} else { // initial setting
				ms.maxEpoch = maxEpoch
			}
		}
		epochRanges[t] = &er
	}

	// store epoch statistics to epoch_stats table
	dbTx := ms.db.Begin()
	if dbTx.Error != nil {
		return errors.WithMessage(dbTx.Error, "failed to begin db tx")
	}

	rollback := func(err error) error {
		if rollbackErr := dbTx.Rollback().Error; rollbackErr != nil {
			logrus.WithError(rollbackErr).Error("Failed to rollback db tx")
		}

		return errors.WithMessage(err, "failed to handle with db tx")
	}

	// store epoch ranges
	er := citypes.RangeUint64{ms.minEpoch, ms.maxEpoch}
	if err := ms.initOrUpdateEpochRangeStats(dbTx, store.EpochDataNil, er); err != nil {
		return rollback(errors.WithMessage(err, "failed to update global epoch range stats"))
	}

	for _, dt := range store.OpEpochDataTypes {
		epr := epochRanges[dt]
		if err := ms.initOrUpdateEpochRangeStats(dbTx, dt, *epr); err != nil {
			return rollback(errors.WithMessage(err, "failed to update local epoch range stats"))
		}

		total, err := ms.loadEpochTotal(dt)
		if err != nil {
			return errors.WithMessage(err, "failed to load epoch total")
		}
		if err := ms.initOrUpdateEpochTotalsStats(dbTx, dt, total); err != nil {
			return rollback(errors.WithMessage(err, "failed to update epoch total stats"))
		}
	}

	// also calculate epoch ranges of logs table partitions and save them to epoch_stats table
	partitionNames, err := ms.loadLogsTblPartitionNames(dbTx, ms.config.Database)
	if err != nil {
		return rollback(errors.WithMessage(err, "failed to get logs table partition names"))
	}

	var minUsedPart, maxUsedPart uint64
	minUsedPart, maxUsedPart = math.MaxUint64, 0

	for i, partName := range partitionNames {
		partEpochRange, err := ms.loadLogsTblPartitionEpochRanges(dbTx, partName)

		if err != nil && !ms.IsRecordNotFound(err) {
			return rollback(errors.WithMessagef(err, "failed to get epoch range for logs partition %v", partName))
		} else if err == nil {
			minUsedPart = util.MinUint64(minUsedPart, uint64(i))
			maxUsedPart = util.MaxUint64(maxUsedPart, uint64(i))
		}

		if err := ms.initOrUpdateLogsPartitionEpochRangeStats(dbTx, partName, partEpochRange); err != nil {
			return rollback(errors.WithMessagef(err, "failed to write epoch range for logs partition %v to epoch stats", partName))
		}
	}

	if minUsedPart != math.MaxUint64 {
		atomic.StoreUint64(&ms.maxUsedLogsTblPartIdx, maxUsedPart)
		atomic.StoreUint64(&ms.minUsedLogsTblPartIdx, minUsedPart)
	}

	if err := dbTx.Commit().Error; err != nil {
		return errors.WithMessage(err, "failed to commit db tx")
	}

	return nil
}

func (ms *mysqlStore) loadCalibratedEpochStats() error {
	// load epoch range statistics from epoch_stats table
	erStats, err := ms.loadEpochStats(epochStatsEpochRange)
	if err != nil {
		return errors.WithMessage(err, "failed to load calibrated epoch range stats")
	}

	for _, stats := range erStats {
		edt := getEpochDataTypeByEpochRangeStatsKey(stats.Key)
		if edt == store.EpochDataNil {
			atomic.StoreUint64(&ms.minEpoch, stats.Epoch1)
			atomic.StoreUint64(&ms.maxEpoch, stats.Epoch2)
			break
		}
	}

	// also calculate used logs table partition indexes
	var logsPartStats []epochStats

	mdb := ms.db.Where("`type` = ?", epochStatsLogsPartEpochRange)
	mdb = mdb.Where("`key` LIKE ?", "logs%")
	mdb = mdb.Where("`epoch1` <> ?", citypes.EpochNumberNil)
	if err := mdb.Model(&epochStats{}).Select("key").Order("id ASC").Find(&logsPartStats).Error; err != nil {
		return errors.WithMessage(err, "failed to load calibrated logs table partitions epoch range stats")
	}

	if len(logsPartStats) == 0 { // no used logs partitions at all
		return nil
	}

	minUsedPart, err := ms.getLogsPartitionIndexByName(logsPartStats[0].Key)
	if err != nil {
		return errors.WithMessagef(err, "failed to get min used index with logs partition %v", logsPartStats[0].Key)
	}

	maxUsedPart, err := ms.getLogsPartitionIndexByName(logsPartStats[len(logsPartStats)-1].Key)
	if err != nil {
		return errors.WithMessagef(err, "failed to get max used index with logs partition %v", logsPartStats[len(logsPartStats)-1].Key)
	}

	atomic.StoreUint64(&ms.maxUsedLogsTblPartIdx, maxUsedPart)
	atomic.StoreUint64(&ms.minUsedLogsTblPartIdx, minUsedPart)

	return nil
}

func (ms *mysqlStore) loadEpochRange(t store.EpochDataType) (uint64, uint64, error) {
	sqlStatement := fmt.Sprintf("SELECT MIN(epoch) AS min_epoch, MAX(epoch) AS max_epoch FROM %v", EpochDataTypeTableMap[t])

	row := ms.db.Raw(sqlStatement).Row()
	if err := row.Err(); err != nil {
		return 0, 0, err
	}

	var minEpoch sql.NullInt64
	var maxEpoch sql.NullInt64

	if err := row.Scan(&minEpoch, &maxEpoch); err != nil {
		return 0, 0, err
	}

	if !minEpoch.Valid {
		return math.MaxUint64, math.MaxUint64, gorm.ErrRecordNotFound
	}

	return uint64(minEpoch.Int64), uint64(maxEpoch.Int64), nil
}

func (ms *mysqlStore) loadEpochTotal(t store.EpochDataType) (uint64, error) {
	sqlStatement := fmt.Sprintf("SELECT COUNT(*) AS total FROM %v", EpochDataTypeTableMap[t])

	row := ms.db.Raw(sqlStatement).Row()
	if err := row.Err(); err != nil {
		return 0, err
	}

	var total uint64
	err := row.Scan(&total)

	return total, err
}
