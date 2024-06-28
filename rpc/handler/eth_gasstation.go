package handler

import (
	"container/list"
	"math/big"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/types"
	logutil "github.com/Conflux-Chain/go-conflux-util/log"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/openweb3/go-rpc-provider/utils"
	ethtypes "github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
)

// zeroHash represents a geth hash value with all zero bytes.
var zeroHash common.Hash

// EthGasStationHandler handles RPC requests for gas price estimation.
type EthGasStationHandler struct {
	config         *GasStationConfig       // Gas station configuration
	status         atomic.Value            // Gas station status
	clientProvider *node.EthClientProvider // Client provider to get full node clients
	clients        []*node.Web3goClient    // Clients used to get historical data
	cliIndex       int                     // Index of the main client
	fromBlock      uint64                  // Start epoch number to sync from
	blockHashList  *list.List              // Linked list to store block hashes ascendingly
	window         *PriorityFeeWindow      // Block priority fee window
}

func MustNewEthGasStationHandlerFromViper(cp *node.EthClientProvider) *EthGasStationHandler {
	var cfg GasStationConfig
	viper.MustUnmarshalKey("gasStation", &cfg)

	if !cfg.Enabled {
		return nil
	}

	// Get all clients in the http group.
	clients, err := cp.GetClientsByGroup(node.GroupEthHttp)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to get fullnode cluster")
	}

	if len(clients) == 0 {
		logrus.Fatal("No full node client available")
	}

	// Select a random client as the main client.
	cliIndex := rand.Int() % len(clients)
	// Get the latest block number with the main client.
	latestBlockNumber, err := clients[cliIndex].Eth.BlockNumber()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to get latest block number")
	}

	fromBlock := latestBlockNumber.Uint64() - uint64(cfg.HistoricalPeekCount)
	h := &EthGasStationHandler{
		config:         &cfg,
		clientProvider: cp,
		clients:        clients,
		cliIndex:       cliIndex,
		blockHashList:  list.New(),
		fromBlock:      fromBlock,
		window:         NewPriorityFeeWindow(cfg.HistoricalPeekCount),
	}

	go h.run()
	return h
}

// run starts to sync historical data and refresh cluster nodes.
func (h *EthGasStationHandler) run() {
	syncTicker := time.NewTimer(0)
	defer syncTicker.Stop()

	refreshTicker := time.NewTicker(clusterUpdateInterval)
	defer refreshTicker.Stop()

	etLogger := logutil.NewErrorTolerantLogger(logutil.DefaultETConfig)
	for {
		select {
		case <-syncTicker.C:
			complete, err := h.sync()
			etLogger.Log(
				logrus.WithFields(logrus.Fields{
					"status":    h.status.Load(),
					"fromBlock": h.fromBlock,
					"clients":   h.clients,
				}), err, "Gas Station handler sync error",
			)
			h.updateStatus(err)
			h.resetSyncTicker(syncTicker, complete, err)
		case <-refreshTicker.C:
			if err := h.refreshClusterNodes(); err != nil {
				logrus.WithError(err).Error("Gas station handler cluster refresh error")
			}
		}
	}
}

// sync synchronizes historical data from the full node cluster.
func (h *EthGasStationHandler) sync() (complete bool, err error) {
	if len(h.clients) == 0 {
		return false, StationStatusClientUnavailable
	}

	h.cliIndex %= len(h.clients)
	for idx := h.cliIndex; ; {
		complete, err = h.trySync(h.clients[idx])
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"cliIndex": idx,
				"nodeUrl":  h.clients[idx].URL,
			}).WithError(err).Debug("Gas station handler sync once error")
		}

		if err == nil || utils.IsRPCJSONError(err) {
			h.cliIndex = idx
			break
		}

		idx = (idx + 1) % len(h.clients)
		if idx == h.cliIndex { // failed all nodes?
			break
		}
	}

	return complete, err
}

func (h *EthGasStationHandler) trySync(eth *node.Web3goClient) (bool, error) {
	logrus.WithFields(logrus.Fields{
		"fromBlock": h.fromBlock,
		"nodeUrl":   eth.URL,
	}).Debug("Gas station handler syncing once")

	// Get the latest block number.
	latestBlockNumber, err := eth.Eth.BlockNumber()
	if err != nil {
		return false, err
	}

	if h.fromBlock > latestBlockNumber.Uint64() { // already catch-up?
		return true, nil
	}

	// Get the block by number.
	block, err := eth.Eth.BlockByNumber(ethtypes.BlockNumber(h.fromBlock), true)
	if err != nil {
		return false, err
	}

	prevBlockHash := h.prevBlockHash()
	if prevBlockHash != zeroHash && prevBlockHash != block.ParentHash {
		logrus.WithFields(logrus.Fields{
			"prevBlockHash":   prevBlockHash,
			"blockHash":       block.Hash,
			"blockParentHash": block.ParentHash,
		}).Debug("Gas station handler detected reorg")

		// Reorg due to parent hash not match, remove the last block.
		h.handleReorg()
		h.fromBlock--
		return false, nil
	}

	h.handleBlock(block)
	h.push(block.Hash)
	h.fromBlock++
	return false, nil
}

func (h *EthGasStationHandler) handleReorg() {
	if blockHash := h.pop(); blockHash != zeroHash {
		h.window.Remove(blockHash.String())
		logrus.WithField("blockHash", blockHash).Info("Gas station handler removed blocks due to reorg")
	}
}

func (h *EthGasStationHandler) handleBlock(block *ethtypes.Block) {
	ratio := float64(block.GasUsed) / float64(block.GasLimit)
	blockFee := &BlockPriorityFee{
		number:       block.Number.Uint64(),
		hash:         block.Hash.String(),
		baseFee:      block.BaseFeePerGas,
		gasUsedRatio: ratio,
	}

	var txnTips []*TxnPriorityFee
	txns := block.Transactions.Transactions()
	for i := range txns {
		// Calculate max priority fee per gas if not set
		if txns[i].MaxPriorityFeePerGas == nil {
			maxFeePerGas := txns[i].MaxFeePerGas
			if maxFeePerGas == nil {
				maxFeePerGas = txns[i].GasPrice
			}

			baseFeePerGas := block.BaseFeePerGas
			if maxFeePerGas == nil || baseFeePerGas == nil || maxFeePerGas.Cmp(baseFeePerGas) < 0 {
				// Skip empty transaction eg., core space `CrossSpaceCall`
				continue
			}

			txns[i].MaxPriorityFeePerGas = big.NewInt(0).Sub(maxFeePerGas, baseFeePerGas)
		}

		logrus.WithFields(logrus.Fields{
			"txnHash":              txns[i].Hash,
			"maxPriorityFeePerGas": txns[i].MaxPriorityFeePerGas,
			"maxFeePerGas":         txns[i].MaxFeePerGas,
			"baseFeePerGas":        block.BaseFeePerGas,
			"gasPrice":             txns[i].GasPrice,
		}).Debug("Gas station handler found txn priority fee")

		txnTips = append(txnTips, &TxnPriorityFee{
			hash: txns[i].Hash.String(),
			tip:  txns[i].MaxPriorityFeePerGas,
		})
	}

	logrus.WithFields(logrus.Fields{
		"blockFeeInfo": blockFee,
		"txnCount":     len(txnTips),
	}).Debug("Gas station handler pushing block")

	blockFee.Append(txnTips...)
	h.window.Push(blockFee)
}

func (h *EthGasStationHandler) pop() common.Hash {
	if h.blockHashList.Len() == 0 {
		return zeroHash
	}

	lastElement := h.blockHashList.Back()
	return h.blockHashList.Remove(lastElement).(common.Hash)
}

func (h *EthGasStationHandler) prevBlockHash() common.Hash {
	if h.blockHashList.Len() == 0 {
		return zeroHash
	}

	return h.blockHashList.Back().Value.(common.Hash)
}

func (h *EthGasStationHandler) push(blockHash common.Hash) {
	h.blockHashList.PushBack(blockHash)
	for h.blockHashList.Len() > maxCachedBlockHashEpochs {
		// Remove old block hashes if capacity is reached
		h.blockHashList.Remove(h.blockHashList.Front())
	}
}

func (h *EthGasStationHandler) updateStatus(err error) {
	if err != nil && !utils.IsRPCJSONError(err) {
		// Set the gas station as unavailable due to network error.
		h.status.Store(err)
	} else {
		h.status.Store(StationStatusOk)
	}
}

func (h *EthGasStationHandler) refreshClusterNodes() error {
	clients, err := h.clientProvider.GetClientsByGroup(node.GroupEthHttp)
	if err != nil {
		return err
	}

	h.clients = clients
	return nil
}

func (h *EthGasStationHandler) resetSyncTicker(syncTicker *time.Timer, complete bool, err error) {
	switch {
	case err != nil:
		syncTicker.Reset(syncIntervalNormal)
	case complete:
		syncTicker.Reset(syncIntervalNormal)
	default:
		syncTicker.Reset(syncIntervalCatchUp)
	}
}

func (h *EthGasStationHandler) Suggest(eth *node.Web3goClient) (*types.SuggestedGasFees, error) {
	if status := h.status.Load(); status != StationStatusOk {
		return nil, status.(error)
	}

	latestBlock, err := eth.Eth.BlockByNumber(ethtypes.LatestBlockNumber, false)
	if err != nil {
		return nil, err
	}

	baseFeePerGas := latestBlock.BaseFeePerGas
	// Calculate the gas fee stats from the priority fee window.
	stats := h.window.Calculate(h.config.Percentiles[:])

	priorityFees := stats.AvgPercentiledPriorityFee
	if priorityFees == nil { // use gas fees directly from the blockchain if no estimation made
		oracleFee, err := eth.Eth.MaxPriorityFeePerGas()
		if err != nil {
			return nil, err
		}

		for i := 0; i < 3; i++ {
			priorityFees = append(priorityFees, oracleFee)
		}
	}

	return &types.SuggestedGasFees{
		Low: types.GasFeeEstimation{
			SuggestedMaxPriorityFeePerGas: (*hexutil.Big)(priorityFees[0]),
			SuggestedMaxFeePerGas:         (*hexutil.Big)(big.NewInt(0).Add(baseFeePerGas, priorityFees[0])),
		},
		Medium: types.GasFeeEstimation{
			SuggestedMaxPriorityFeePerGas: (*hexutil.Big)(priorityFees[1]),
			SuggestedMaxFeePerGas:         (*hexutil.Big)(big.NewInt(0).Add(baseFeePerGas, priorityFees[1])),
		},
		High: types.GasFeeEstimation{
			SuggestedMaxPriorityFeePerGas: (*hexutil.Big)(priorityFees[2]),
			SuggestedMaxFeePerGas:         (*hexutil.Big)(big.NewInt(0).Add(baseFeePerGas, priorityFees[2])),
		},
		EstimatedBaseFee:           (*hexutil.Big)(latestBlock.BaseFeePerGas),
		NetworkCongestion:          stats.NetworkCongestion,
		LatestPriorityFeeRange:     ToHexBigSlice(stats.LatestPriorityFeeRange),
		HistoricalPriorityFeeRange: ToHexBigSlice(stats.HistoricalPriorityFeeRange),
		HistoricalBaseFeeRange:     ToHexBigSlice(stats.HistoricalBaseFeeRange),
		PriorityFeeTrend:           stats.PriorityFeeTrend,
		BaseFeeTrend:               stats.BaseFeeTrend,
	}, nil
}
