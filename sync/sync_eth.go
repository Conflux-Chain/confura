package sync

import (
	"context"
	"sync"
	"time"

	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/metrics"
	"github.com/conflux-chain/conflux-infura/rpc/cfxbridge"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/util"
	gometrics "github.com/ethereum/go-ethereum/metrics"
	"github.com/openweb3/web3go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	// viper key for custom eth sync point
	viperEthSyncFromBlockKey = "sync.eth.fromBlock"
)

// EthSyncer is used to synchronize conflux EVM space blockchain data
// into db and cache store.
type EthSyncer struct {
	// EVM space ETH client
	w3c *web3go.Client
	// EVM space chain id
	chainId uint32
	// db store
	db store.Store
	// block number to sync chaindata from
	fromBlock uint64
	// maximum number of blocks to sync once
	maxSyncBlocks uint64
	// interval to sync data in normal status
	syncIntervalNormal time.Duration
	// interval to sync data in catching up mode
	syncIntervalCatchUp time.Duration
}

// MustNewEthSyncer creates an instance of EthSyncer to sync Conflux EVM space chaindata.
func MustNewEthSyncer(ethC *web3go.Client, db store.Store) *EthSyncer {
	ethChainId, err := ethC.Eth.ChainId()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to get chain ID from eth space")
	}

	syncer := &EthSyncer{
		w3c:                 ethC,
		chainId:             uint32(*ethChainId),
		db:                  db,
		maxSyncBlocks:       viper.GetUint64("sync.maxEpochs"),
		syncIntervalNormal:  time.Second,
		syncIntervalCatchUp: time.Millisecond,
	}

	// Load last sync block information
	syncer.mustLoadLastSyncBlock()

	return syncer
}

// Sync starts to sync Conflux EVM space blockchain data.
func (syncer *EthSyncer) Sync(ctx context.Context, wg *sync.WaitGroup) {
	logrus.WithField("fromBlock", syncer.fromBlock).Info("ETH sync starting to sync block data")

	wg.Add(1)
	defer wg.Done()

	ticker := time.NewTicker(syncer.syncIntervalCatchUp)
	defer ticker.Stop()

	for {
		select { // second class priority
		case <-ctx.Done():
			logrus.Info("ETH syncer shutdown ok")
			return
		case <-ticker.C:
			if err := syncer.doTicker(ticker); err != nil {
				logrus.WithError(err).
					WithField("fromBlock", syncer.fromBlock).
					Error("ETH syncer failed to sync block data")
			}
		}
	}
}

func (syncer *EthSyncer) doTicker(ticker *time.Ticker) error {
	logrus.Debug("ETH sync ticking")

	if complete, err := syncer.syncOnce(); err != nil {
		ticker.Reset(syncer.syncIntervalNormal)
		return err
	} else if complete {
		ticker.Reset(syncer.syncIntervalNormal)
	} else {
		ticker.Reset(syncer.syncIntervalCatchUp)
	}

	return nil
}

// Sync data once and return true if catch up to the most recent block, otherwise false.
func (syncer *EthSyncer) syncOnce() (bool, error) {
	recentBlockNumber, err := syncer.w3c.Eth.BlockNumber()
	if err != nil {
		return false, errors.WithMessage(err, "failed to query the latest block number")
	}

	updater := metrics.NewTimerUpdaterByName("infura/duration/eth/sync/once")
	defer updater.Update()

	recentBlockNo := recentBlockNumber.Uint64()
	if syncer.fromBlock > recentBlockNo { // catched up or re-orged?
		logger := logrus.WithFields(logrus.Fields{
			"syncFromBlock": syncer.fromBlock, "recentBlockNo": recentBlockNo,
		})

		if syncer.fromBlock == recentBlockNo+1 { // regarded as catched up even through maybe re-orged
			logger.Debug("ETH syncer skipped due to already catched up")
			return true, nil
		}

		err := syncer.reorgRevert(recentBlockNo)
		if err != nil {
			err = errors.WithMessage(err, "failed to revert block(s) due to invalid block range")
		}

		logger.WithError(err).Info("ETH syncer reverted block(s) due to invalid block range")
		return false, err
	}

	toBlock := util.MinUint64(syncer.fromBlock+syncer.maxSyncBlocks-1, recentBlockNo)
	syncSize := toBlock - syncer.fromBlock + 1

	syncSizeGauge := gometrics.GetOrRegisterGauge("infura/eth/sync/size", nil)
	syncSizeGauge.Update(int64(syncSize))

	logger := logrus.WithFields(logrus.Fields{
		"syncSize": syncSize, "fromBlock": syncer.fromBlock, "toBlock": toBlock,
	})

	logger.Debug("ETH syncer started to sync with block range")

	ethDataSlice := make([]*store.EthData, 0, syncSize)
	for i := uint64(0); i < syncSize; i++ {
		blockNo := syncer.fromBlock + uint64(i)
		blogger := logger.WithField("block", blockNo)

		data, err := store.QueryEthData(syncer.w3c, blockNo)

		// If chain re-orged, stop the querying right now since it's pointless to query data
		// that will be reverted late.
		if errors.Is(err, store.ErrChainReorged) {
			blogger.WithError(err).Info("ETH syncer failed to query eth data due to re-org")
			break
		}

		if err != nil {
			return false, errors.WithMessagef(err, "failed to query eth data for block %v", blockNo)
		}

		if i == 0 { // the first block must be continuous to the latest block in db store
			latestBlockHash, err := syncer.getStoreLatestBlockHash()
			if err != nil {
				blogger.WithError(err).Error(
					"ETH syncer failed to get latest block hash from ethdb for parent hash check",
				)
				return false, errors.WithMessage(err, "failed to get latest block hash")
			}

			if len(latestBlockHash) > 0 && data.Block.ParentHash.Hex() != latestBlockHash {
				if err := syncer.reorgRevert(syncer.fromBlock - 1); err != nil {
					parentBlockHash := data.Block.ParentHash.Hex()

					blogger.WithFields(logrus.Fields{
						"parentBlockHash": parentBlockHash, "latestBlockHash": latestBlockHash,
					}).WithError(err).Info(
						"ETH syncer failed to revert block data from ethdb store due to parent hash mismatched",
					)
					return false, errors.WithMessage(err, "failed to revert block data from ethdb")
				}

				blogger.WithField("latestBlockHash", latestBlockHash).Info(
					"ETH syncer reverted latest block from ethdb store due to parent hash mismatched",
				)

				return false, nil
			}
		} else { // otherwise non-first block must also be continuous to previous one
			continuous, desc := data.IsContinuousTo(ethDataSlice[i-1])
			if !continuous {
				// truncate the batch synced block data until the previous one
				ethDataSlice = ethDataSlice[:i-1]

				blogger.WithField("i", i).Infof(
					"ETH syncer truncated batch synced data due to block not continuous for %v", desc,
				)
				break
			}
		}

		ethDataSlice = append(ethDataSlice, data)

		blogger.Debug("ETH syncer succeeded to query epoch data")
	}

	if len(ethDataSlice) == 0 { // empty eth data query
		logger.Debug("ETH syncer skipped due to empty sync range")
		return false, nil
	}

	// brige for db store logic reuse by converting eth data to epoch data
	epochDataSlice := make([]*store.EpochData, 0, len(ethDataSlice))
	for i := 0; i < len(ethDataSlice); i++ {
		epochData := syncer.convertToEpochData(ethDataSlice[i])
		epochDataSlice = append(epochDataSlice, epochData)
	}

	if err = syncer.db.Pushn(epochDataSlice); err != nil {
		logger.WithError(err).Info("ETH syncer failed to save eth data to ethdb")
		return false, errors.WithMessage(err, "failed to save eth data")
	}

	syncer.fromBlock += uint64(len(ethDataSlice))

	logger.WithFields(logrus.Fields{
		"newSyncFrom":   syncer.fromBlock,
		"finalSyncSize": len(ethDataSlice),
	}).Debug("ETH syncer succeeded to batch sync block data")

	return false, nil
}

func (syncer *EthSyncer) reorgRevert(revertTo uint64) error {
	if revertTo == 0 {
		return errors.New("genesis block must not be reverted")
	}

	logger := logrus.WithFields(logrus.Fields{
		"revertTo": revertTo, "revertFrom": syncer.fromBlock - 1,
	})

	if revertTo >= syncer.fromBlock {
		logger.Debug(
			"ETH syncer skipped re-org revert due to not catched up yet",
		)
		return nil
	}

	// remove block data from database due to chain re-org
	if err := syncer.db.Popn(revertTo); err != nil {
		logger.WithError(err).Error(
			"ETH syncer failed to pop eth data from ethdb due to chain re-org",
		)

		return errors.WithMessage(err, "failed to pop eth data from ethdb")
	}

	// update syncer start block
	syncer.fromBlock = revertTo

	logger.Info("ETH syncer reverted block data due to chain re-org")
	return nil
}

// Load last sync block from databse to continue synchronization.
func (syncer *EthSyncer) mustLoadLastSyncBlock() {
	loaded, err := syncer.loadLastSyncBlock()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to load last sync block range from ethdb")
	}

	// Load eth sync start block config on initial loading if necessary.
	if !loaded && viper.IsSet(viperEthSyncFromBlockKey) {
		syncer.fromBlock = viper.GetUint64(viperEthSyncFromBlockKey)
	}
}

func (syncer *EthSyncer) loadLastSyncBlock() (loaded bool, err error) {
	_, maxBlock, err := syncer.db.GetGlobalEpochRange()
	if err == nil {
		syncer.fromBlock = maxBlock + 1
		return true, nil
	}

	if !syncer.db.IsRecordNotFound(err) {
		return false, errors.WithMessage(err, "failed to read sync block range from ethdb")
	}

	return false, nil
}

func (syncer *EthSyncer) getStoreLatestBlockHash() (string, error) {
	if syncer.fromBlock == 0 { // no block synchronized yet
		return "", nil
	}

	latestBlockNo := syncer.fromBlock - 1

	// TODO load in-memory cache first to improve performance.

	// load from db store
	block, err := syncer.db.GetBlockSummaryByEpoch(latestBlockNo)
	if err == nil {
		return string(block.Hash), nil
	}

	if syncer.db.IsRecordNotFound(err) {
		return "", nil
	}

	return "", errors.WithMessagef(err, "failed to get block #%v", latestBlockNo)
}

// convertToEpochData converts ETH block data to Conflux epoch data. This is used for bridge eth
// block data with epoch data to reduce redundant codes eg., store logic.
func (syncer *EthSyncer) convertToEpochData(ethData *store.EthData) *store.EpochData {
	epochData := &store.EpochData{
		Number: ethData.Number, Receipts: make(map[cfxtypes.Hash]*cfxtypes.TransactionReceipt),
	}

	pivotBlock := cfxbridge.ConvertBlock(ethData.Block, syncer.chainId)
	epochData.Blocks = []*cfxtypes.Block{pivotBlock}

	for txh, rcpt := range ethData.Receipts {
		txRcpt := cfxbridge.ConvertReceipt(rcpt, syncer.chainId)
		txHash := cfxbridge.ConvertHashNullable(&txh)

		epochData.Receipts[*txHash] = txRcpt
	}

	return epochData
}
