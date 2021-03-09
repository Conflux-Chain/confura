package sync

import (
	"math/big"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/sirupsen/logrus"
)

var defaultSyncEpoch = types.EpochLatestCheckpoint

const maxSyncEpochs = 10

// SynchronizeDatabase starts to sync epoch data against the latest confirmed epoch,
// and persist data in specified database.
func SynchronizeDatabase(cfx sdk.ClientOperator, db store.Store) {
	epochFrom := mustLoadLastSyncEpoch(db) + 1

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	// TODO case <- revertedEpoch: do revert in database if necessary.
	// E.g. latest_confirmed epoch was reverted
	for range ticker.C {
		epochTo, err := syncDatabase(cfx, db, epochFrom)
		if err != nil {
			logrus.WithError(err).WithField("epochFrom", epochFrom).Error("Failed to sync epoch data")
		} else {
			logrus.WithFields(logrus.Fields{
				"epochFrom": epochFrom,
				"epochTo":   epochTo,
			}).Trace("Succeed to sync epoch data")

			epochFrom = epochTo + 1
		}
	}
}

func mustLoadLastSyncEpoch(db store.Store) int64 {
	minEpoch, maxEpoch, err := db.GetBlockEpochRange()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to read block epoch range from database")
	}

	logrus.WithFields(logrus.Fields{
		"minEpoch": minEpoch,
		"maxEpoch": maxEpoch,
	}).Info("Start to sync epoch data to database")

	var lastSyncEpoch int64

	if maxEpoch != nil {
		lastSyncEpoch = maxEpoch.Int64()
	}

	return lastSyncEpoch
}

func syncDatabase(cfx sdk.ClientOperator, db store.Store, epochFrom int64) (int64, error) {
	epoch, err := cfx.GetEpochNumber(defaultSyncEpoch)
	if err != nil {
		return 0, err
	}

	epochTo := epochFrom + maxSyncEpochs - 1
	if maxEpochTo := epoch.ToInt().Int64(); epochTo > maxEpochTo {
		epochTo = maxEpochTo
	}

	epochDataSlice := make([]*store.EpochData, 0, epochTo-epochFrom+1)

	for i := epochFrom; i <= epochTo; i++ {
		data, err := store.QueryEpochData(cfx, big.NewInt(i))
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
