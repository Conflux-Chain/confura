package mysql

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

const (
	// Logs table partition range (by ID) size
	LogsTablePartitionRangeSize = uint64(20000000)
	// Number of logs table partitions, besides there is
	// an extra partition to hold any data beyond the range.
	LogsTablePartitionsNum = 100
)

func initLogsPartitions(db *gorm.DB) error {
	sqlLines := make([]string, 0, 120)
	sqlLines = append(sqlLines, "ALTER TABLE logs PARTITION BY RANGE (id)(")

	for i := uint64(0); i < uint64(LogsTablePartitionsNum); i++ {
		lineStr := fmt.Sprintf("PARTITION logs%v VALUES LESS THAN (%v),", i, (i+1)*LogsTablePartitionRangeSize)
		sqlLines = append(sqlLines, lineStr)
	}

	sqlLines = append(sqlLines, "PARTITION logsow VALUES LESS THAN MAXVALUE);")

	logsPartitionSql := strings.Join(sqlLines, "\n")
	logrus.WithField("logsPartitionSql", logsPartitionSql).Debug("Init logs db table partitions")

	return db.Exec(logsPartitionSql).Error
}

type logPartitioner struct{}

func (logPartitioner) getLogsPartitionIdxFromId(logsId uint64) uint64 {
	return util.MinUint64(logsId/LogsTablePartitionRangeSize, LogsTablePartitionsNum)
}

func (logPartitioner) getLogsPartitionNameByIdx(idx uint64) string {
	if idx < LogsTablePartitionsNum {
		return fmt.Sprintf("logs%v", idx)
	}

	return "logsow"
}

func (logPartitioner) getLogsPartitionIndexByName(partname string) (uint64, error) {
	if partname == "logsow" {
		return LogsTablePartitionsNum, nil
	}

	return strconv.ParseUint(strings.TrimLeft(partname, "logs"), 10, 64)
}

// loadLogsTblPartitionNames retrieves all logs table partitions names sorted by partition oridinal position.
func (logPartitioner) loadLogsTblPartitionNames(db *gorm.DB, dbname string) ([]string, error) {
	sqlStatement := `
SELECT PARTITION_NAME AS partname FROM information_schema.partitions WHERE TABLE_SCHEMA='%v'
	AND TABLE_NAME = 'logs' AND PARTITION_NAME IS NOT NULL ORDER BY PARTITION_ORDINAL_POSITION ASC;
`
	sqlStatement = fmt.Sprintf(sqlStatement, dbname)

	// Load all logs table partition names
	logsTblPartiNames := []string{}
	if err := db.Raw(sqlStatement).Scan(&logsTblPartiNames).Error; err != nil {
		return nil, err
	}

	return logsTblPartiNames, nil
}

// loadLogsTblPartitionEpochRanges loads epoch range for specific partition name.
// The epoch number ranges will be used to find the right partition(s) to boost
// the db query performance when conditioned with epoch.
func (logPartitioner) loadLogsTblPartitionEpochRanges(db *gorm.DB, partiName string) (types.RangeUint64, error) {
	sqlStatement := fmt.Sprintf("SELECT MIN(epoch) as minEpoch, MAX(epoch) as maxEpoch FROM logs PARTITION (%v)", partiName)
	row := db.Raw(sqlStatement).Row()
	if err := row.Err(); err != nil {
		return types.EpochRangeNil, err
	}

	var minEpoch, maxEpoch sql.NullInt64
	if err := row.Scan(&minEpoch, &maxEpoch); err != nil {
		return types.EpochRangeNil, err
	}

	if !minEpoch.Valid || !maxEpoch.Valid {
		return types.EpochRangeNil, gorm.ErrRecordNotFound
	}

	return types.RangeUint64{
		From: uint64(minEpoch.Int64),
		To:   uint64(maxEpoch.Int64),
	}, nil
}

// Find the right logs table partition(s) for the specified epoch range. It will check
// logs table paritions and return all the partitions whose epoch range are overlapped
// with the specified one.
func (logPartitioner) findLogsPartitionsEpochRangeWithinStoreTx(dbTx *gorm.DB, epochFrom, epochTo uint64) (res []string, err error) {
	mdb := dbTx.Where("`type` = ? AND `key` LIKE ?", epochStatsLogsPartEpochRange, "logs%")
	mdb = mdb.Where("`epoch1` <= ? AND `epoch2` >= ?", epochTo, epochFrom)

	err = mdb.Model(&epochStats{}).Select("key").Find(&res).Error
	return
}

func (logPartitioner) diffLogsPartitionEpochRangeForRealSet(beforeER, afterER types.RangeUint64) [2]*uint64 {
	diff := func(before, after uint64) *uint64 {
		if before == after { // no change
			return nil
		}
		return &after
	}

	return [2]*uint64{
		diff(beforeER.From, afterER.From),
		diff(beforeER.To, afterER.To),
	}
}
