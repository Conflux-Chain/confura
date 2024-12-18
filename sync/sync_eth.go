package sync

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Conflux-Chain/confura/rpc/cfxbridge"
	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/sync/election"
	"github.com/Conflux-Chain/confura/sync/monitor"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-util/dlock"
	logutil "github.com/Conflux-Chain/go-conflux-util/log"
	viperutil "github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/openweb3/web3go"
	ethtypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type syncEthConfig struct {
	FromBlock uint64 `default:"1"`
	MaxBlocks uint64 `default:"10"`
}

// EthSyncer is used to synchronize evm space blockchain data into db store.
type EthSyncer struct {
	conf *syncEthConfig
	// EVM space web3go clients
	w3cs []*web3go.Client
	// Selected web3go client index
	w3cIdx atomic.Uint32
	// EVM space chain id
	chainId uint32
	// db store
	db *mysql.MysqlStore
	// block number to sync chaindata from
	fromBlock uint64
	// maximum number of blocks to sync once
	maxSyncBlocks uint64
	// interval to sync data in normal status
	syncIntervalNormal time.Duration
	// interval to sync data in catching up mode
	syncIntervalCatchUp time.Duration
	// window to cache block info
	epochPivotWin *epochPivotWindow
	// HA leader/follower election
	elm election.LeaderManager
	// sync monitor
	monitor *monitor.Monitor
}

// MustNewEthSyncer creates an instance of EthSyncer to sync Conflux EVM space chaindata.
func MustNewEthSyncer(ethClients []*web3go.Client, db *mysql.MysqlStore) *EthSyncer {
	if len(ethClients) == 0 {
		logrus.Fatal("No web3go client provided")
	}

	ethChainId, err := ethClients[0].Eth.ChainId()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to get chain ID from eth space")
	}

	var ethConf syncEthConfig
	viperutil.MustUnmarshalKey("sync.eth", &ethConf)

	dlm := dlock.NewLockManager(dlock.NewMySQLBackend(db.DB()))
	monitor := monitor.NewMonitor(monitor.NewConfig(), func() (latestBlockNum uint64, retErr error) {
		for _, ethC := range ethClients {
			block, err := ethC.Eth.BlockByNumber(ethtypes.SafeBlockNumber, false)
			if err == nil {
				latestBlockNum = max(latestBlockNum, block.Number.Uint64())
			} else {
				retErr = err
			}
		}
		if latestBlockNum > 0 {
			return latestBlockNum, nil
		}
		return 0, retErr
	})

	syncer := &EthSyncer{
		conf:                &ethConf,
		w3cs:                ethClients,
		chainId:             uint32(*ethChainId),
		db:                  db,
		maxSyncBlocks:       ethConf.MaxBlocks,
		syncIntervalNormal:  time.Second,
		syncIntervalCatchUp: time.Millisecond,
		monitor:             monitor,
		epochPivotWin:       newEpochPivotWindow(syncPivotInfoWinCapacity),
		elm:                 election.MustNewLeaderManagerFromViper(dlm, "sync.eth"),
	}
	monitor.SetObserver(syncer)

	// Register leader election callbacks
	syncer.elm.OnElected(func(ctx context.Context, lm election.LeaderManager) {
		syncer.monitor.Start(ctx)
		syncer.onLeadershipChanged(ctx, lm, true)
	})

	syncer.elm.OnOusted(func(ctx context.Context, lm election.LeaderManager) {
		syncer.monitor.Stop()
		syncer.onLeadershipChanged(ctx, lm, false)
	})

	// Load last sync block information
	syncer.mustLoadLastSyncBlock()

	return syncer
}

// Sync starts to sync Conflux EVM space blockchain data.
func (syncer *EthSyncer) Sync(ctx context.Context, wg *sync.WaitGroup) {
	logrus.WithField("fromBlock", syncer.fromBlock).Info("ETH sync starting to sync block data")

	wg.Add(1)
	defer wg.Done()

	go syncer.elm.Campaign(ctx)
	defer syncer.elm.Stop()

	ticker := time.NewTimer(syncer.syncIntervalCatchUp)
	defer ticker.Stop()

	etLogger := logutil.NewErrorTolerantLogger(logutil.DefaultETConfig)
	defer logrus.Info("ETH syncer shutdown ok")

	for syncer.elm.Await(ctx) {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := syncer.doTicker(ctx, ticker)
			etLogger.Log(
				logrus.WithField("fromBlock", syncer.fromBlock),
				err, "ETH syncer failed to sync epoch data",
			)
		}
	}
}

func (syncer *EthSyncer) doTicker(ctx context.Context, ticker *time.Timer) error {
	logrus.Debug("ETH sync ticking")

	start := time.Now()
	complete, err := syncer.syncOnce(ctx)
	metrics.Registry.Sync.SyncOnceQps("eth", "db", err).UpdateSince(start)

	if err != nil {
		ticker.Reset(syncer.syncIntervalNormal)
	} else if complete {
		ticker.Reset(syncer.syncIntervalNormal)
	} else {
		ticker.Reset(syncer.syncIntervalCatchUp)
	}

	return err
}

func (syncer *EthSyncer) nextBlockTo(maxBlockTo uint64) (uint64, uint64) {
	toBlock := util.MinUint64(syncer.fromBlock+syncer.maxSyncBlocks-1, maxBlockTo)
	syncSize := toBlock - syncer.fromBlock + 1
	return toBlock, syncSize
}

// Sync data once and return true if catch up to the most recent block, otherwise false.
func (syncer *EthSyncer) syncOnce(ctx context.Context) (bool, error) {
	w3c := syncer.w3cs[syncer.w3cIdx.Load()]

	latestBlock, err := w3c.Eth.BlockByNumber(ethtypes.SafeBlockNumber, false)
	if err != nil {
		return false, errors.WithMessage(err, "failed to query the latest block number")
	}

	recentBlockNo := latestBlock.Number.Uint64()

	// Load latest sync block from database
	if err := syncer.loadLastSyncBlock(); err != nil {
		return false, errors.WithMessage(err, "failed to load last sync epoch")
	}

	// catched up to the most recent block?
	if syncer.fromBlock > recentBlockNo {
		logrus.WithFields(logrus.Fields{
			"latestBlockNo": recentBlockNo,
			"syncFromBlock": syncer.fromBlock,
			"recentBlockNo": recentBlockNo,
		}).Debug("ETH syncer skipped due to already catched up")

		return true, nil
	}

	toBlock, syncSize := syncer.nextBlockTo(recentBlockNo)

	logger := logrus.WithFields(logrus.Fields{
		"syncSize":  syncSize,
		"fromBlock": syncer.fromBlock,
		"toBlock":   toBlock,
	})

	logger.Debug("ETH syncer started to sync with block range")

	ethDataSlice := make([]*store.EthData, 0, syncSize)
	for i := uint64(0); i < syncSize; i++ {
		blockNo := syncer.fromBlock + uint64(i)
		blogger := logger.WithField("block", blockNo)

		data, err := store.QueryEthData(ctx, w3c, blockNo)

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
				if err := syncer.reorgRevert(ctx, syncer.latestStoreBlock()); err != nil {
					parentBlockHash := data.Block.ParentHash.Hex()

					blogger.WithFields(logrus.Fields{
						"parentBlockHash": parentBlockHash,
						"latestBlockHash": latestBlockHash,
					}).WithError(err).Warn(
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

	metrics.Registry.Sync.SyncOnceSize("eth", "db").Update(int64(len(ethDataSlice)))

	if len(ethDataSlice) == 0 { // empty eth data query
		logger.Debug("ETH syncer skipped due to empty sync range")
		return false, nil
	}

	// reuse db store logic by converting eth data to epoch data
	epochDataSlice := make([]*store.EpochData, 0, len(ethDataSlice))
	for i := 0; i < len(ethDataSlice); i++ {
		epochData := syncer.convertToEpochData(ethDataSlice[i])
		epochDataSlice = append(epochDataSlice, epochData)
	}

	err = syncer.db.PushnWithFinalizer(epochDataSlice, func(d *gorm.DB) error {
		return syncer.elm.Extend(ctx)
	})

	if err != nil {
		if errors.Is(err, store.ErrLeaderRenewal) {
			logger.WithField("leaderIdentity", syncer.elm.Identity()).
				WithError(err).
				Info("ETH syncer failed to renew leadership on pushing eth data to db")
			return false, nil
		}

		logger.WithError(err).Error("ETH syncer failed to save eth data to ethdb")
		return false, errors.WithMessage(err, "failed to save eth data")
	}

	for _, edata := range ethDataSlice { // cache eth block info for late use
		cfxbh := cfxbridge.ConvertBlockHeader(edata.Block, syncer.chainId)
		err := syncer.epochPivotWin.Push(&cfxtypes.Block{BlockHeader: *cfxbh})
		if err != nil {
			logger.WithField("blockNumber", edata.Number).WithError(err).Info(
				"ETH syncer failed to push block into cache window",
			)

			syncer.epochPivotWin.Reset()
			break
		}
	}

	syncer.fromBlock += uint64(len(ethDataSlice))
	syncer.monitor.Update(syncer.fromBlock)

	logger.WithFields(logrus.Fields{
		"newSyncFrom":   syncer.fromBlock,
		"finalSyncSize": len(ethDataSlice),
	}).Debug("ETH syncer succeeded to batch sync block data")

	return false, nil
}

func (syncer *EthSyncer) reorgRevert(ctx context.Context, revertTo uint64) error {
	if revertTo == 0 {
		return errors.New("genesis block must not be reverted")
	}

	logger := logrus.WithFields(logrus.Fields{
		"revertTo": revertTo, "revertFrom": syncer.latestStoreBlock(),
	})

	if revertTo >= syncer.fromBlock {
		logger.Debug(
			"ETH syncer skipped re-org revert due to not catched up yet",
		)
		return nil
	}

	// remove block data from database due to chain re-org
	err := syncer.db.PopnWithFinalizer(revertTo, func(d *gorm.DB) error {
		return syncer.elm.Extend(ctx)
	})

	if err != nil {
		if errors.Is(err, store.ErrLeaderRenewal) {
			logger.WithField("leaderIdentity", syncer.elm.Identity()).
				WithError(err).
				Info("ETH syncer failed to renew leadership on popping eth data from db")
			return nil
		}

		logger.WithError(err).Error(
			"ETH syncer failed to pop eth data from ethdb due to chain re-org",
		)
		return errors.WithMessage(err, "failed to pop eth data from ethdb")
	}

	// remove block hash of reverted block from cache window
	syncer.epochPivotWin.Popn(revertTo)
	// update syncer start block
	syncer.fromBlock = revertTo

	logger.Info("ETH syncer reverted block data due to chain re-org")
	return nil
}

// Load last sync block from databse to continue synchronization.
func (syncer *EthSyncer) mustLoadLastSyncBlock() {
	if err := syncer.loadLastSyncBlock(); err != nil {
		logrus.WithError(err).Fatal("Failed to load last sync block range from ethdb")
	}
}

func (syncer *EthSyncer) loadLastSyncBlock() error {
	// load last sync block from databse
	maxBlock, ok, err := syncer.db.MaxEpoch()
	if err != nil {
		return errors.WithMessage(err, "failed to get max block from e2b mapping")
	}

	if ok { // continue from the last sync epoch
		syncer.fromBlock = maxBlock + 1
	} else { // start from genesis or configured start block
		syncer.fromBlock = 0
		if syncer.conf != nil {
			syncer.fromBlock = syncer.conf.FromBlock
		}
	}

	return nil
}

func (syncer *EthSyncer) getStoreLatestBlockHash() (string, error) {
	if syncer.fromBlock == 0 { // no block synchronized yet
		return "", nil
	}

	latestBlockNo := syncer.latestStoreBlock()

	// load from in-memory cache first
	if blockHash, ok := syncer.epochPivotWin.GetPivotHash(latestBlockNo); ok {
		return string(blockHash), nil
	}

	pivotHash, _, err := syncer.db.PivotHash(latestBlockNo)
	return pivotHash, err
}

// convertToEpochData converts evm space block data to core space epoch data. This is used to bridge
// eth block data with epoch data to reuse code logic eg., db store logic.
func (syncer *EthSyncer) convertToEpochData(ethData *store.EthData) *store.EpochData {
	epochData := &store.EpochData{
		Number:      ethData.Number,
		Receipts:    make(map[cfxtypes.Hash]*cfxtypes.TransactionReceipt),
		ReceiptExts: make(map[cfxtypes.Hash]*store.ReceiptExtra),
	}

	pivotBlock := cfxbridge.ConvertBlock(ethData.Block, syncer.chainId)
	epochData.Blocks = []*cfxtypes.Block{pivotBlock}

	blockExt := store.ExtractEthBlockExt(ethData.Block)
	epochData.BlockExts = []*store.BlockExtra{blockExt}

	for txh, rcpt := range ethData.Receipts {
		txRcpt := cfxbridge.ConvertReceipt(rcpt, syncer.chainId)
		txHash := cfxbridge.ConvertHash(txh)

		epochData.Receipts[txHash] = txRcpt
		epochData.ReceiptExts[txHash] = store.ExtractEthReceiptExt(rcpt)
	}

	// Transaction `status` field is not a standard field for evm-compatible chain, so we have
	// to manually fill this field from their receipt.
	for i := range pivotBlock.Transactions {
		if pivotBlock.Transactions[i].Status != nil {
			continue
		}

		txnHash := pivotBlock.Transactions[i].Hash
		if rcpt, ok := epochData.Receipts[txnHash]; ok && rcpt != nil {
			txnStatus := rcpt.OutcomeStatus
			pivotBlock.Transactions[i].Status = &txnStatus
		}
	}

	return epochData
}

func (syncer *EthSyncer) latestStoreBlock() uint64 {
	if syncer.fromBlock > 0 {
		return syncer.fromBlock - 1
	}

	return 0
}

func (syncer *EthSyncer) onLeadershipChanged(
	ctx context.Context, lm election.LeaderManager, gainedOrLost bool) {
	syncer.epochPivotWin.Reset()
	if !gainedOrLost && ctx.Err() != context.Canceled {
		logrus.WithField("leaderID", lm.Identity()).Warn("ETH syncer lost HA leadership")
	}
}

func (syncer *EthSyncer) OnStateChange(state monitor.HealthState, details ...string) {
	if len(syncer.w3cs) > 1 && state == monitor.Unhealthy {
		// Switch to the next cfx client if the sync progress is not healthy
		newW3cIdx := (syncer.w3cIdx.Load() + 1) % uint32(len(syncer.w3cs))
		syncer.w3cIdx.Store(newW3cIdx)
		logrus.WithField("w3cIndex", newW3cIdx).Info("Switched to the next web3go client")
	}
}
