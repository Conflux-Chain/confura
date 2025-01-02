package handler

import (
	"errors"
	"math/big"
	"math/rand"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/types"
	"github.com/Conflux-Chain/confura/util"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/openweb3/go-rpc-provider/utils"
	"github.com/sirupsen/logrus"
)

const (
	// maxCachedBlockHashEpochs is the max number of epochs to cache their block hashes.
	maxCachedBlockHashEpochs = 100
)

// CfxGasStationHandler handles RPC requests for gas price estimation.
type CfxGasStationHandler struct {
	*baseGasStationHandler
	clientProvider *node.CfxClientProvider // Client provider to get full node clients
	clients        []sdk.ClientOperator    // Clients used to get historical data
	cliIndex       int                     // Index of the main client
	fromEpoch      uint64                  // Start epoch number to sync from
}

func MustNewCfxGasStationHandlerFromViper(cp *node.CfxClientProvider) *CfxGasStationHandler {
	var cfg GasStationConfig
	viper.MustUnmarshalKey("gasStation", &cfg)

	if !cfg.Enabled {
		return nil
	}

	// Get all clients in the http group.
	clients, err := cp.GetClientsByGroup(node.GroupCfxHttp)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to get fullnode cluster")
	}

	if len(clients) == 0 {
		logrus.Fatal("No full node client available")
	}

	// Select a random client as the main client.
	cliIndex := rand.Int() % len(clients)
	// Get the latest epoch number with the main client.
	latestEpoch, err := clients[cliIndex].GetEpochNumber(cfxtypes.EpochLatestState)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to get latest epoch number")
	}

	fromEpoch := latestEpoch.ToInt().Uint64() - uint64(cfg.HistoricalPeekCount)
	h := &CfxGasStationHandler{
		baseGasStationHandler: newBaseGasStationHandler(&cfg),
		clientProvider:        cp,
		clients:               clients,
		cliIndex:              cliIndex,
		fromEpoch:             fromEpoch,
	}

	go h.run(h.sync, h.refreshClusterNodes)
	return h
}

// sync synchronizes historical data from the full node cluster.
func (h *CfxGasStationHandler) sync() (complete bool, err error) {
	if len(h.clients) == 0 {
		return false, StationStatusClientUnavailable
	}

	h.cliIndex %= len(h.clients)
	for idx := h.cliIndex; ; {
		complete, err = h.trySync(h.clients[idx])
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"cliIndex": idx,
				"nodeUrl":  h.clients[idx].GetNodeURL(),
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

func (h *CfxGasStationHandler) trySync(cfx sdk.ClientOperator) (bool, error) {
	logrus.WithFields(logrus.Fields{
		"fromEpoch": h.fromEpoch,
		"nodeUrl":   cfx.GetNodeURL(),
	}).Debug("Gas station handler syncing once")

	// Get the latest epoch number.
	latestEpoch, err := cfx.GetEpochNumber(cfxtypes.EpochLatestState)
	if err != nil {
		return false, err
	}

	if latestEpoch == nil {
		return false, errors.New("latest epoch is nil")
	}

	latestEpochNo := latestEpoch.ToInt().Uint64()
	if h.fromEpoch > latestEpochNo { // already catch-up?
		return true, nil
	}

	// Get the pivot block.
	epoch := cfxtypes.NewEpochNumberUint64(h.fromEpoch)
	pivotBlock, err := cfx.GetBlockByEpoch(epoch)
	if err != nil {
		return false, err
	}

	if pivotBlock == nil {
		return false, errors.New("pivot block is nil")
	}

	prevEpochBh := h.prevEpochPivotBlockHash()
	if len(prevEpochBh) > 0 && prevEpochBh != pivotBlock.ParentHash {
		logrus.WithFields(logrus.Fields{
			"prevEpochBh":          prevEpochBh,
			"pivotBlockHash":       pivotBlock.Hash,
			"pivotBlockParentHash": pivotBlock.ParentHash,
		}).Debug("Gas station handler detected reorg")

		// Reorg due to parent hash not match, remove the last epoch.
		h.handleReorg()
		h.fromEpoch--
		return false, nil
	}

	blockHashes, blocks, err := h.fetchBlocks(cfx, epoch, pivotBlock)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"pivotBlockHash": pivotBlock.Hash,
			"epoch":          epoch,
		}).WithError(err).Debug("Gas station handler fetch blocks error")
		return false, err
	}

	for i := range blocks {
		h.handleBlock(blocks[i])
	}

	h.push(blockHashes)
	h.fromEpoch++
	return false, nil
}

func (h *CfxGasStationHandler) fetchBlocks(
	cfx sdk.ClientOperator, epoch *cfxtypes.Epoch, pivotBlock *cfxtypes.Block,
) ([]cfxtypes.Hash, []*cfxtypes.Block, error) {
	// Get epoch block hashes.
	blockHashes, err := cfx.GetBlocksByEpoch(epoch)
	if err != nil {
		return nil, nil, err
	}

	if len(blockHashes) == 0 {
		return nil, nil, errors.New("empty block hashes")
	}

	pivotHash := blockHashes[len(blockHashes)-1]
	if pivotBlock.Hash != pivotHash { // abandon this epoch due to pivot switched
		return nil, nil, errors.New("pivot switched")
	}

	var blocks []*cfxtypes.Block
	for i := 0; i < len(blockHashes)-1; i++ {
		block, err := cfx.GetBlockByHashWithPivotAssumption(blockHashes[i], pivotHash, hexutil.Uint64(h.fromEpoch))
		if err != nil {
			return nil, nil, err
		}
		if block.GasUsed == nil {
			return nil, nil, errors.New("unexecuted block")
		}
		blocks = append(blocks, &block)
	}

	blocks = append(blocks, pivotBlock)
	return blockHashes, blocks, nil
}

func (h *CfxGasStationHandler) handleReorg() {
	var blockHashes []string
	for _, bh := range h.pop() {
		blockHashes = append(blockHashes, bh.String())
	}

	if len(blockHashes) > 0 {
		h.window.Remove(blockHashes...)
		logrus.WithField("blockHashes", blockHashes).
			Info("Gas station handler removed blocks due to reorg")
	}
}

func (h *CfxGasStationHandler) handleBlock(block *cfxtypes.Block) {
	if block.GasLimit == nil || block.GasLimit.ToInt().Int64() == 0 {
		return
	}

	ratio, _ := new(big.Rat).SetFrac(block.GasUsed.ToInt(), block.GasLimit.ToInt()).Float64()
	blockFee := &BlockPriorityFee{
		number:       block.BlockNumber.ToInt().Uint64(),
		hash:         block.Hash.String(),
		baseFee:      block.BaseFeePerGas.ToInt(),
		gasUsedRatio: ratio,
	}

	var txnTips []*TxnPriorityFee
	for i := range block.Transactions {
		txn := block.Transactions[i]

		// Skip unexecuted transaction, e.g.
		// 1) already executed in previous block
		// 2) never executed, e.g. nonce mismatch
		if !util.IsTxExecutedInBlock(&txn) {
			continue
		}

		// Calculate max priority fee per gas if not set
		if txn.MaxPriorityFeePerGas == nil {
			maxFeePerGas := txn.MaxFeePerGas.ToInt()
			if maxFeePerGas == nil {
				maxFeePerGas = txn.GasPrice.ToInt()
			}

			baseFeePerGas := block.BaseFeePerGas.ToInt()
			if maxFeePerGas == nil || baseFeePerGas == nil || maxFeePerGas.Cmp(baseFeePerGas) < 0 {
				// This shouldn't happen, but just in case.
				logrus.WithField("txnHash", txn.Hash).Warn("Gas station handler found abnormal txn fee")
				continue
			}

			txn.MaxPriorityFeePerGas = (*hexutil.Big)(big.NewInt(0).Sub(maxFeePerGas, baseFeePerGas))
		}

		logrus.WithFields(logrus.Fields{
			"txnHash":              txn.Hash,
			"maxPriorityFeePerGas": txn.MaxPriorityFeePerGas,
			"maxFeePerGas":         txn.MaxFeePerGas,
			"baseFeePerGas":        block.BaseFeePerGas,
			"gasPrice":             txn.GasPrice,
		}).Debug("Gas station handler found txn priority fee")

		txnTips = append(txnTips, &TxnPriorityFee{
			hash: txn.Hash.String(),
			tip:  txn.MaxPriorityFeePerGas.ToInt(),
		})
	}

	logrus.WithFields(logrus.Fields{
		"blockFeeInfo": blockFee,
		"execTxnCount": len(txnTips),
	}).Debug("Gas station handler pushing block")

	blockFee.Append(txnTips...)
	h.window.Push(blockFee)
}

func (h *CfxGasStationHandler) pop() []cfxtypes.Hash {
	if h.blockHashList.Len() == 0 {
		return nil
	}

	lastElement := h.blockHashList.Back()
	return h.blockHashList.Remove(lastElement).([]cfxtypes.Hash)
}

func (h *CfxGasStationHandler) prevEpochPivotBlockHash() cfxtypes.Hash {
	if h.blockHashList.Len() == 0 {
		return cfxtypes.Hash("")
	}

	blockHashes := h.blockHashList.Back().Value.([]cfxtypes.Hash)
	return blockHashes[len(blockHashes)-1]
}

func (h *CfxGasStationHandler) push(blockHashes []cfxtypes.Hash) {
	h.blockHashList.PushBack(blockHashes)
	for h.blockHashList.Len() > maxCachedBlockHashEpochs {
		// Remove old epoch block hashes if capacity is reached
		h.blockHashList.Remove(h.blockHashList.Front())
	}
}

func (h *CfxGasStationHandler) refreshClusterNodes() error {
	clients, err := h.clientProvider.GetClientsByGroup(node.GroupCfxHttp)
	if err != nil {
		return err
	}

	h.clients = clients
	return nil
}

func (h *CfxGasStationHandler) Suggest(cfx sdk.ClientOperator) (*types.SuggestedGasFees, error) {
	if err := h.checkStatus(); err != nil {
		return nil, err
	}

	latestBlock, err := cfx.GetBlockSummaryByEpoch(cfxtypes.EpochLatestState)
	if err != nil {
		return nil, err
	}

	baseFeePerGas := latestBlock.BaseFeePerGas.ToInt()
	// Calculate the gas fee stats from the priority fee window.
	stats := h.window.Calculate(h.config.Percentiles[:])

	if priorityFees := stats.AvgPercentiledPriorityFee; priorityFees == nil {
		// Use priority fee directly from the blockchain if no estimation were managed to make.
		oracleFee, err := cfx.GetMaxPriorityFeePerGas()
		if err != nil {
			return nil, err
		}

		for i := 0; i < len(h.config.Percentiles); i++ {
			priorityFees = append(priorityFees, oracleFee.ToInt())
		}
		stats.AvgPercentiledPriorityFee = priorityFees
	}

	return assembleSuggestedGasFees(baseFeePerGas, &stats), nil
}
