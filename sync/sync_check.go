package sync

import (
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/store"
	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Epoch reverted handler function injected support for flexibility and testability
type epochRevertedChecker func(cfx sdk.ClientOperator, s store.Store, epochNo uint64) (res bool, err error)
type firstRevertedEpochSearcher func(cfx sdk.ClientOperator, s store.Store, epochRange citypes.EpochRange) (uint64, error)
type epochRevertedPruner func(s store.Store, er citypes.EpochRange) error

// Ensure epoch data in store valid such as not reverted etc.,
func ensureStoreEpochDataOk(cfx sdk.ClientOperator, s store.Store) error {
	// Get the latest confirmed sync epoch number from store
	minEpoch, maxEpoch, err := s.GetGlobalEpochRange()

	// If there is no epoch data in store yet, nothing needs to be done
	if s.IsRecordNotFound(err) {
		return nil
	}

	// Otherwise return err
	if err != nil {
		return errors.WithMessage(err, "failed to read block epoch range from store")
	}

	epochRange := citypes.EpochRange{EpochFrom: minEpoch, EpochTo: maxEpoch}

	logrus.WithField("epochRange", epochRange).Debug("Ensuring epoch data within range ok...")

	// Epoch reverted handler
	searcher := func(cfx sdk.ClientOperator, s store.Store, epochRange citypes.EpochRange) (uint64, error) {
		return findFirstRevertedEpochInRange(cfx, s, epochRange, checkIfEpochIsReverted)
	}

	return ensureEpochRangeNotRerverted(cfx, s, epochRange, searcher, pruneRevertedEpochData)
}

// Ensure epoch within the specified range not reverted or prune the reverted epoch data
func ensureEpochRangeNotRerverted(cfx sdk.ClientOperator, s store.Store, epochRange citypes.EpochRange, searcher firstRevertedEpochSearcher, pruner epochRevertedPruner) error {
	logger := logrus.WithField("epochRange", epochRange)
	logger.Debug("Ensuring epoch data within range not reverted...")

	// Handle 200 epochs per window for each loop
	var winSize, winStart, winEnd, matched uint64 = 200, epochRange.EpochTo, epochRange.EpochTo, 0
	for winStart <= winEnd && winEnd > 0 {
		// Find the first reverted epoch within epoch range (winStart, winEnd)
		firstRevertedEpoch, err := searcher(cfx, s, citypes.EpochRange{EpochFrom: winStart, EpochTo: winEnd})

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

		pruneEpochRange := citypes.EpochRange{EpochFrom: matched, EpochTo: epochRange.EpochTo}
		if err := pruner(s, pruneEpochRange); err != nil {
			logger.WithField("epochFrom", matched).Error("Failed to prune reverted epoch within range")

			return errors.WithMessage(err, "failed to prune reverted epoch data")
		}

		logger.WithField("pruneEpochRange", pruneEpochRange).Info("Pruned dirty data to ensure epoch data validity")
	}

	return nil
}

// Find the first reverted epoch number from a specified epoch range
func findFirstRevertedEpochInRange(cfx sdk.ClientOperator, s store.Store, er citypes.EpochRange, checker epochRevertedChecker) (uint64, error) {
	// Find the first reverted sync epoch with binary probing
	start, mid, end, matched := er.EpochFrom, er.EpochTo, er.EpochTo, uint64(0)
	for start <= end && mid >= er.EpochFrom && mid <= er.EpochTo {
		reverted, err := checker(cfx, s, mid)
		if err != nil {
			logrus.WithError(err).WithField("epoch", mid).Error("Failed to check epoch reverted")

			return 0, errors.WithMessage(err, "failed to check epoch reverted")
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

// Check if the epoch data in store is reverted
func checkIfEpochIsReverted(cfx sdk.ClientOperator, s store.Store, epochNo uint64) (res bool, err error) {
	// Get the sync epoch block from store
	sBlock, err := s.GetBlockSummaryByEpoch(epochNo)
	if err != nil {
		// Epoch data not found in store, take it as not reverted
		if s.IsRecordNotFound(errors.Cause(err)) {
			return false, nil
		}

		return false, errors.WithMessagef(err, "failed to get pivot block for epoch %v from store", epochNo)
	}

	// Fetch the epoch pivot block from blockchain
	epBlock, err := cfx.GetBlockSummaryByEpoch(types.NewEpochNumberUint64(epochNo))
	if err != nil {
		return false, errors.WithMessagef(err, "failed to get pivot block for epoch %v from blockchain", epochNo)
	}

	// Check if block epoch number matched or not
	if sBlock.EpochNumber == nil || sBlock.EpochNumber.ToInt().Uint64() != epochNo {
		return true, nil
	}

	// Compare block hash to see if the epoch is reverted
	return sBlock.BlockHeader.Hash != epBlock.BlockHeader.Hash, nil
}

// Remove reverted epoch data (blocks, trxs, and logs) from store
func pruneRevertedEpochData(s store.Store, er citypes.EpochRange) error {
	numsEpochs := er.EpochTo - er.EpochFrom + 1
	// Delete at most 200 records per round
	maxDelete := uint64(200)
	rounds := numsEpochs / maxDelete
	if numsEpochs%maxDelete != 0 {
		rounds++
	}

	for i := rounds; i > 0; i-- {
		// Remove reverted epoch data from store
		if err := s.Popn(er.EpochFrom + (i-1)*maxDelete); err != nil {
			return err
		}
	}

	return nil
}
