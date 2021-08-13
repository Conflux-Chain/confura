package mysql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/conflux-chain/conflux-infura/util"
)

const (
	// Logs table partition range (by ID) size
	LogsTablePartitionRangeSize = uint64(20000000)
	// Number of logs table partitions, besides there is
	// an extra partition to hold any data beyond the range.
	LogsTablePartitionsNum = 100
)

// Get logs partition name by id column of logs table
func getLogsPartitionNameFromId(logsId uint64) string {
	idx := getLogsPartitionIdxFromId(logsId)
	return getLogsPartitionNameByIdx(idx)
}

func getLogsPartitionIdxFromId(logsId uint64) uint64 {
	return util.MinUint64(logsId/LogsTablePartitionRangeSize, LogsTablePartitionsNum)
}

func getLogsPartitionNameByIdx(idx uint64) string {
	if idx < LogsTablePartitionsNum {
		return fmt.Sprintf("logs%v", idx)
	}

	return "logsow"
}

func getLogsPartitionIndexByName(partname string) (uint64, error) {
	if partname == "logsow" {
		return LogsTablePartitionsNum, nil
	}

	return strconv.ParseUint(strings.TrimLeft(partname, "logs"), 10, 64)
}
