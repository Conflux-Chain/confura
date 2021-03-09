package store

import (
	"math/big"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/sirupsen/logrus"
)

var defaultSyncEpoch = types.EpochLatestCheckpoint

const maxSyncEpochs = 3

// SyncEpochData starts to sync epoch data against the latest confirmed epoch,
// and persist data in specified store.
func SyncEpochData(cfx sdk.ClientOperator, db Store) {
	minEpoch, maxEpoch, err := db.GetBlockEpochRange()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to read block epoch range from store")
	}

	logrus.WithFields(logrus.Fields{
		"minEpoch": minEpoch,
		"maxEpoch": maxEpoch,
	}).Info("Start to sync epoch data")

	var syncEpochFrom int64

	if maxEpoch != nil {
		syncEpochFrom = maxEpoch.Int64() + 1
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	// TODO case <- revertedEpoch: do revert in database if necessary.
	// E.g. latest_confirmed epoch was reverted
	for range ticker.C {
		epochTo, err := syncEpochDataByEpoch(cfx, db, syncEpochFrom)
		if err != nil {
			logrus.WithError(err).WithField("epochFrom", syncEpochFrom).Error("Failed to sync epoch data")
		} else {
			logrus.WithFields(logrus.Fields{
				"epochFrom": syncEpochFrom,
				"epochTo":   epochTo,
			}).Trace("Succeed to sync epoch data")

			syncEpochFrom = epochTo + 1
		}
	}
}

func syncEpochDataByEpoch(cfx sdk.ClientOperator, db Store, epochFrom int64) (int64, error) {
	epoch, err := cfx.GetEpochNumber(defaultSyncEpoch)
	if err != nil {
		return 0, err
	}

	epochTo := epochFrom + maxSyncEpochs - 1
	if maxEpochTo := epoch.ToInt().Int64(); epochTo > maxEpochTo {
		epochTo = maxEpochTo
	}

	epochDataSlice := make([]*EpochData, 0, epochTo-epochFrom+1)

	for i := epochFrom; i <= epochTo; i++ {
		data, err := QueryEpochData(cfx, big.NewInt(i))
		if err != nil {
			return 0, err
		}

		epochDataSlice = append(epochDataSlice, &data)
	}

	if err = db.PutEpochDataSlice(epochDataSlice); err != nil {
		return 0, err
	}

	return epochTo, nil
}
