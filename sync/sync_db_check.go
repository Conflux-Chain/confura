package sync

import (
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Epoch reverted handler function injected support for flexibility and testability
type epochRevertedChecker func(uint64) (bool, error)
type firstRevertedEpochSearcher func(epochRange citypes.EpochRange) (uint64, error)
type epochRevertedPruner func(epochRange citypes.EpochRange) error

// Ensure last confirmed epoch data valid such as not reverted etc.,
func (syncer *DatabaseSyncer) ensureLastConfirmedEpochOk() error {
	// Get the latest confirmed sync epoch number from db
	minEpoch, maxEpoch, err := syncer.db.GetGlobalEpochRange()

	// If there is no epoch data in db yet, nothing needs to be done
	if syncer.db.IsRecordNotFound(err) {
		return nil
	}

	// Otherwise return err
	if err != nil {
		return errors.Wrap(err, "failed to read block epoch range from database")
	}

	epochRange := citypes.EpochRange{EpochFrom: minEpoch, EpochTo: maxEpoch}

	logger := logrus.WithField("epochRange", epochRange)
	logger.Debug("Ensure epoch data within range ok")

	// Epoch reverted handler
	searcher := func(epochRange citypes.EpochRange) (uint64, error) {
		return syncer.findFirstRevertedEpochInRange(epochRange, syncer.checkIfEpochIsReverted)
	}

	return syncer.ensureEpochRangeNotRerverted(epochRange, searcher, syncer.pruneRevertedEpochData)
}

// Ensure epoch within the specified range not reverted or prune the reverted epoch data
func (syncer *DatabaseSyncer) ensureEpochRangeNotRerverted(epochRange citypes.EpochRange, searcher firstRevertedEpochSearcher, pruner epochRevertedPruner) error {
	logger := logrus.WithField("epochRange", epochRange)
	logger.Debug("Ensure epoch data within range not reverted")

	// Handle 200 epochs per window for each loop
	var winSize, winStart, winEnd, matched uint64 = 200, epochRange.EpochTo, epochRange.EpochTo, 0
	for winStart <= winEnd && winEnd > 0 {
		// Find the first reverted epoch within epoch range (winStart, winEnd)
		firstRevertedEpoch, err := searcher(citypes.EpochRange{EpochFrom: winStart, EpochTo: winEnd})
		if err != nil {
			logrus.WithField("epochRange", citypes.EpochRange{
				EpochFrom: winStart, EpochTo: winEnd,
			}).Error("Failed to find the first reverted epoch within range")

			return err
		} else if firstRevertedEpoch != 0 { // updated matched reverted epoch
			matched = firstRevertedEpoch
		}

		// The first reverted epoch found is not the start epoch within the searching range
		// or no reverted epoch found at all or all epochs are searched by
		if firstRevertedEpoch > winStart || firstRevertedEpoch == 0 || winStart <= 1 {
			break
		}

		// Update winEnd and winStart
		winEnd = winStart - 1           // decrease winEnd to the left one of winStart
		winStart = epochRange.EpochFrom // set winStart to the first epoch of the epoch range
		if winEnd >= winSize {          // if winEnd can cover winSize, calculate the winStart
			winStart = util.MaxUint64(winEnd-winSize+1, epochRange.EpochFrom) // also make sure winStart be within the epoch range
		}
	}

	// Prune reverted epoch data
	if matched != 0 && matched >= epochRange.EpochFrom && matched <= epochRange.EpochTo {
		logger.WithField("matched", matched).Debug("Found the first reverted epoch within range")

		if err := pruner(citypes.EpochRange{EpochFrom: matched, EpochTo: epochRange.EpochTo}); err != nil {
			logger.WithField("epochFrom", matched).Error("Failed to prune reverted epoch within range")

			return errors.Wrap(err, "failed to prune reverted epoch data")
		}
	}

	return nil
}

// Find the first reverted epoch number from a specified epoch range
func (syncer *DatabaseSyncer) findFirstRevertedEpochInRange(er citypes.EpochRange, checker epochRevertedChecker) (uint64, error) {
	// Find the first reverted sync epoch with binary probing
	start, mid, end, matched := er.EpochFrom, er.EpochTo, er.EpochTo, uint64(0)
	for start <= end && mid >= er.EpochFrom && mid <= er.EpochTo {
		reverted, err := checker(mid)
		if err != nil {
			logrus.WithError(err).WithField("epoch", mid).Error("Failed to check epoch reverted")

			return 0, errors.Wrapf(err, "failed to check epoch reverted")
		}

		if reverted {
			logrus.WithFields(logrus.Fields{
				"epochRange": citypes.EpochRange{EpochFrom: start, EpochTo: end},
			}).WithField("matched", mid).Debug("Found a reverted epoch within range")

			matched, end = mid, mid-1
		} else {
			start = mid + 1
		}

		if start <= end { // in case of overflow
			mid = start + (end-start)>>1
		}
	}

	logrus.WithField("epochRange", er).WithField("matched", matched).Debug("Got the final reverted epoch within range")

	return matched, nil
}

// Check if the confirmed epoch data in db is reverted
func (syncer *DatabaseSyncer) checkIfEpochIsReverted(epochNo uint64) (res bool, err error) {
	// Get the confirmed sync epoch block from db
	dbBlock, err := syncer.db.GetBlockSummaryByEpoch(epochNo)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get pivot block for epoch %v from db", epochNo)
	}

	// Fetch the confirmed epoch block from blockchain
	epBlock, err := syncer.cfx.GetBlockByEpoch(types.NewEpochNumberUint64(epochNo))
	if err != nil {
		return false, errors.Wrapf(err, "failed to get pivot block for epoch %v from chain", epochNo)
	}

	// Compare block hash to see if the epoch is reverted
	return dbBlock.BlockHeader.Hash != epBlock.BlockHeader.Hash, nil
}

// Remove reverted epoch data (blocks, trxs, logs etc.,)
func (syncer *DatabaseSyncer) pruneRevertedEpochData(er citypes.EpochRange) error {
	numsEpochs := er.EpochTo - er.EpochFrom + 1
	// Delete at most 200 records per round
	maxDelete := uint64(200)
	rounds := numsEpochs / maxDelete
	if numsEpochs%maxDelete != 0 {
		rounds++
	}

	for i := rounds; i > 0; i-- {
		// Remove reverted epoch data from db
		if err := syncer.db.Popn(er.EpochFrom + (i-1)*maxDelete); err != nil {
			return err
		}
	}

	return nil
}
