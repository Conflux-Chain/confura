package gasstation

import (
	"math/big"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/montanaflynn/stats"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	// gas price sample gathering interval
	gatheringInterval = time.Second * 1
	// num of epochs for gas price batch statistics collection
	batchStatsEpochs = 100
	// max continuous failures for gas price sample gathering
	maxGatheringFailtures = 10

	// define fast/fatest/safeLow/average mining duration hirarchy
	// for gas price prediction
	thresholdFastestMiningDuration  = 10 * time.Second // <= 10s
	thresholdFastMiningDuration     = 20 * time.Second // <= 20s
	thresholdAvgMingingDuration     = 30 * time.Second // <= 30s
	thresholdLowSafeMingingDuration = 1 * time.Minute  // <= 1min
)

var (
	gasPriceGatheringEpochType = types.EpochLatestState
)

type GasPriceEstimater struct {
	cfx               sdk.ClientOperator
	gatheringFailures int                     // continuous gather failures
	epochFrom         uint64                  // epoch number to gather from
	estimateWin       *gasPriceSamplingWindow // estimate sampling window
	collectedStats    [5]*gasPriceUsedStats   // collected statistics
}

// MustNewGasPriceEstimater creates an instance of gas price estimator.
func MustNewGasPriceEstimater(cfx sdk.ClientOperator) *GasPriceEstimater {
	latestEpoch, err := cfx.GetEpochNumber(gasPriceGatheringEpochType)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to new gas station price estimater")
	}

	epochFrom := latestEpoch.ToInt().Uint64() - batchStatsEpochs + 1
	return NewGasPriceEstimater(cfx, epochFrom)
}

func NewGasPriceEstimater(cfx sdk.ClientOperator, fromEpoch uint64) *GasPriceEstimater {
	return &GasPriceEstimater{
		cfx: cfx, epochFrom: fromEpoch,
		estimateWin: newGasPriceSamplingWindow(batchStatsEpochs),
	}
}

// Run starts to gather samples from epoch data, collect statistics from samples and
// then predict gas price estimates.
func (e *GasPriceEstimater) Run() {
	ticker := time.NewTicker(gatheringInterval)
	defer ticker.Stop()

	for range ticker.C {
		if err := e.doRun(); err != nil {
			logrus.WithField("epochFrom", e.epochFrom).WithError(err).Error(
				"Gas station failed to run gas price estimater",
			)
		}
	}
}

func (e *GasPriceEstimater) doRun() error {
	logger := logrus.WithFields(logrus.Fields{
		"startEpoch": e.epochFrom, "endEpoch": &e.epochFrom,
	})

	// gather gas price used samples
	newSamplesGathered, err := e.gather()
	if err != nil {
		e.gatheringFailures++

		if e.gatheringFailures > maxGatheringFailtures {
			e.gatheringFailures = 0
			return errors.WithMessage(err, "failed to gather gas price sample")
		}

		logger.Infof(
			"Retrying %v times to gather gas price samples", e.gatheringFailures+1,
		)
		return nil
	}

	// reset gathering failure times
	e.gatheringFailures = 0

	if !newSamplesGathered { // no new samples gathered, nothing to do next
		logger.Debug("No new gas price samples gathered")
		return nil
	}

	logger.Debug("Gas price samples gathered")

	// collect gas price statistics from gathered samples
	statsCollected, err := e.collectStats()
	if err != nil {
		logger.WithError(err).Error("Failed to collect gas statistics from samples")
	}

	if !statsCollected { // no statistics collected
		logger.Debug("No gas price statistics collected")
		return nil
	}

	logger.Debug("Gas price statistics collected")

	// TODO:
	// 1. predict gas price (also need to consider if some stats bucket is empty)
	// 2. store predicted price to database for rpc;
	// 3. add metrics for realtime analysis;
	// 4. shrink sampling window in case of memory blasted.

	return nil
}

func (e *GasPriceEstimater) gather() (bool, error) {
	// get latest epoch for sampling
	latestEpoch, err := e.cfx.GetEpochNumber(gasPriceGatheringEpochType)
	if err != nil {
		return false, errors.WithMessage(err, "failed to get latest epoch")
	}

	latestEpochNo := latestEpoch.ToInt().Uint64()

	logger := logrus.WithFields(logrus.Fields{
		"startEpochFrom": e.epochFrom, "latestEpochNo": latestEpochNo,
	})

	if e.epochFrom > latestEpochNo { // no new epoch to gather samples
		logger.Debug("No new epoch from which to gather gas price samples (may be due to pivot switch)")
		return false, nil
	}

	// gather epoch tx gas price estimates samples
	for e.epochFrom <= latestEpochNo {
		logger.WithField("epochNo", e.epochFrom).Debug("Gathering gas price used samples for epoch")

		// get epoch block hashes
		epoch := types.NewEpochNumberUint64(e.epochFrom)
		blockHashes, err := e.cfx.GetBlocksByEpoch(epoch)
		if err != nil {
			return false, errors.WithMessage(err, "failed to get epoch block hashes")
		}

		var pivotBlock *types.Block
		tx2EstimateSamples := make(map[types.Hash]*gasPriceUsedSample, len(blockHashes))

		// fetch epoch block one by one
		for i := len(blockHashes) - 1; i >= 0; i-- {
			blkHash := blockHashes[i]

			block, err := e.cfx.GetBlockByHash(blkHash)
			if err != nil {
				return false, errors.WithMessage(err, "failed to get block by hash")
			}

			if block.EpochNumber.ToInt().Uint64() != e.epochFrom { // pivot switch?
				logger.Info("Failed to get block by hash (due to pivot switch)")
				return false, nil
			}

			if i == len(blockHashes)-1 { // pivot block?
				pivotBlock = block
			}

			// gather tx estimate samples
			for j := 0; j < len(block.Transactions); j++ {
				tx := &block.Transactions[j]

				// skip unexecuted transaction
				if tx.Status == nil || (*tx.Status != 0 && *tx.Status != 1) {
					logger.WithFields(logrus.Fields{
						"txHash": tx.Hash, "status": tx.Status,
					}).Debug("Skip unexecuted transaction for gas price sample gathering")

					continue
				}

				// duplicates check
				if _, ok := e.estimateWin.txToEstimateSamples[tx.Hash]; ok {
					logger.WithField("txHash", tx.Hash).Debug(
						"Transaction gas price sample already gathered",
					)
					continue
				}

				// estimate total duration for tx from proposal to package roughly
				proposalEpochNo := tx.EpochHeight.ToInt().Uint64()
				proposalEpochBlock, ok := e.estimateWin.epochToPivotBlocks[proposalEpochNo]
				if !ok { // epoch too stale?
					logrus.WithFields(logrus.Fields{
						"txHash": tx.Hash, "proposalEpochNo": proposalEpochNo,
					}).Debug("No pivot block found for proposal epoch for tx")

					continue
				}

				tsdiff := big.NewInt(0)
				tsdiff.Sub(
					pivotBlock.Timestamp.ToInt(), proposalEpochBlock.Timestamp.ToInt(),
				)

				if tsdiff.Int64() > 0 {
					tx2EstimateSamples[tx.Hash] = &gasPriceUsedSample{
						tx: tx, epochNo: e.epochFrom,
						minedDuration: time.Duration(tsdiff.Uint64() * uint64(time.Second)),
					}
				}

				logger.WithFields(logrus.Fields{
					"epochNo": e.epochFrom, "txHash": tx.Hash, "gasPrice": tx.GasPrice.ToInt(),
					"miningDuration(s)": tsdiff.Int64(),
				}).Debug("Gas price used sampled for transaction")
			}
		}

		// cache store epoch pivot block
		e.estimateWin.epochToPivotBlocks[e.epochFrom] = &types.BlockSummary{
			BlockHeader: pivotBlock.BlockHeader,
		}

		// cache store epoch tx gas price used samples
		for txh, sample := range tx2EstimateSamples {
			e.estimateWin.txToEstimateSamples[txh] = sample
			e.estimateWin.epochToTxs[e.epochFrom] = append(e.estimateWin.epochToTxs[e.epochFrom], &txh)
		}

		// expand gas price sampling window
		e.estimateWin.expandTo(e.epochFrom)

		// move to next epoch
		e.epochFrom++
	}

	return true, nil
}

func (e *GasPriceEstimater) collectStats() (bool, error) {
	if e.estimateWin.size() < batchStatsEpochs {
		logrus.WithField("size", e.estimateWin.size()).Debug(
			"Not enough gas price used samples to collect statistics from",
		)
		return false, nil
	}

	// collect tx sample statistics
	startEpoch := util.MaxUint64(
		e.estimateWin.startEpoch, e.estimateWin.endEpoch-batchStatsEpochs+1,
	)

	var gasStatsBuckets [5]*gasPriceUsedStats
	var gasSampleBuckets [5][]float64

	for en := startEpoch; en <= e.estimateWin.endEpoch; en++ {
		epochTxHashes, ok := e.estimateWin.epochToTxs[en]
		if !ok { // no transactions within epoch?
			logrus.WithField("epochNo", en).Debug("No txs within epoch to collect samples")
			continue
		}

		for _, txh := range epochTxHashes {
			sample, ok := e.estimateWin.txToEstimateSamples[*txh]
			if !ok { // no sample found for tx?
				logrus.WithFields(logrus.Fields{
					"epochNo": en, "txHash": txh,
				}).Debug("No price used sample found for tx to collect stats")
				continue
			}

			bucketIdx := len(gasSampleBuckets) - 1
			gasPrice := float64(sample.tx.GasPrice.ToInt().Int64())

			switch {
			case sample.minedDuration <= thresholdFastestMiningDuration:
				bucketIdx = 0
			case sample.minedDuration <= thresholdFastMiningDuration:
				bucketIdx = 1
			case sample.minedDuration <= thresholdAvgMingingDuration:
				bucketIdx = 2
			case sample.minedDuration <= thresholdLowSafeMingingDuration:
				bucketIdx = 3
			default:
			}

			gasSampleBuckets[bucketIdx] = append(gasSampleBuckets[bucketIdx], gasPrice)
		}
	}

	logrus.WithField("gasPriceSampleBuckets", gasSampleBuckets).Debug(
		"Gas price sample buckets collected for stats",
	)

	for i, bucket := range gasSampleBuckets {
		if len(bucket) == 0 { // empty bucket?
			logrus.WithField("bucketIndex", i).Debug("No gas price used samples found in empty bucket")
			continue
		}

		opFuncs := [4]func(stats.Float64Data) (float64, error){ // order is important!!!
			stats.Min, stats.Max, stats.Median, stats.Mean,
		}

		gasStats := &gasPriceUsedStats{}
		setPtrs := []*float64{ // order is important!!!
			&gasStats.min, &gasStats.max, &gasStats.median, &gasStats.mean,
		}

		for j, op := range opFuncs {
			var err error

			*setPtrs[j], err = op(bucket)
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"bucket": bucket, "bucketIndex": i, "opIndex": j,
				}).WithError(err).Error("Failed to calculate statistics for bucket")

				return false, errors.WithMessage(err, "failed to calculate stats")
			}
		}

		gasStatsBuckets[i] = gasStats
	}

	e.collectedStats = gasStatsBuckets
	return true, nil
}
