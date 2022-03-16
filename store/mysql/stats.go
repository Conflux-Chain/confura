package mysql

import (
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/sirupsen/logrus"
)

var (
	epochDataType2EpochRangeStatsKey = map[store.EpochDataType]string{
		store.EpochDataNil:     "global_epoch_range",
		store.EpochBlock:       "block_epoch_range",
		store.EpochTransaction: "tx_epoch_range",
		store.EpochLog:         "log_epoch_range",
	}

	epochDataType2EpochTotalStatsKey = map[store.EpochDataType]string{
		store.EpochBlock:       "num_blocks",
		store.EpochTransaction: "num_txs",
		store.EpochLog:         "num_logs",
	}
)

func getEpochRangeStatsKey(dt store.EpochDataType) (s string) {
	return epochDataType2EpochRangeStatsKey[dt]
}

func getEpochDataTypeByEpochRangeStatsKey(key string) store.EpochDataType {
	switch key {
	case epochDataType2EpochRangeStatsKey[store.EpochDataNil]:
		return store.EpochDataNil
	case epochDataType2EpochRangeStatsKey[store.EpochBlock]:
		return store.EpochBlock
	case epochDataType2EpochRangeStatsKey[store.EpochTransaction]:
		return store.EpochTransaction
	case epochDataType2EpochRangeStatsKey[store.EpochLog]:
		return store.EpochLog
	}

	logrus.WithField("key", key).Fatal("Unsupported epoch range stats key to get epoch data type")
	return store.EpochDataNil
}

func getEpochTotalStatsKey(dt store.EpochDataType) (s string) {
	return epochDataType2EpochTotalStatsKey[dt]
}
