package handler

import (
	"errors"
	"math/big"
	"math/rand"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/types"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/go-rpc-provider/utils"
	ethtypes "github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
)

// zeroGethHash represents a geth hash value with all zero bytes.
var zeroGethHash common.Hash

// EthGasStationHandler handles RPC requests for gas price estimation.
type EthGasStationHandler struct {
	*baseGasStationHandler
	clientProvider *node.EthClientProvider // Client provider to get full node clients
	clients        []*node.Web3goClient    // Clients used to get historical data
	cliIndex       int                     // Index of the main client
	fromBlock      uint64                  // Start block number to sync from
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
		baseGasStationHandler: newBaseGasStationHandler(&cfg),
		clientProvider:        cp,
		clients:               clients,
		cliIndex:              cliIndex,
		fromBlock:             fromBlock,
	}

	go h.run(h.sync, h.refreshClusterNodes)
	return h
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

	if block == nil {
		return false, errors.New("invalid nil block")
	}

	prevBlockHash := h.prevBlockHash()
	if prevBlockHash != zeroGethHash && prevBlockHash != block.ParentHash {
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
	if blockHash := h.pop(); blockHash != zeroGethHash {
		h.window.Remove(blockHash.String())
		logrus.WithField("blockHash", blockHash).
			Info("Gas station handler removed block due to reorg")
	}
}

func (h *EthGasStationHandler) handleBlock(block *ethtypes.Block) {
	if block.GasLimit == 0 {
		return
	}

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
		return zeroGethHash
	}

	lastElement := h.blockHashList.Back()
	return h.blockHashList.Remove(lastElement).(common.Hash)
}

func (h *EthGasStationHandler) prevBlockHash() common.Hash {
	if h.blockHashList.Len() == 0 {
		return zeroGethHash
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

func (h *EthGasStationHandler) refreshClusterNodes() error {
	clients, err := h.clientProvider.GetClientsByGroup(node.GroupEthHttp)
	if err != nil {
		return err
	}

	h.clients = clients
	return nil
}

func (h *EthGasStationHandler) Suggest(eth *node.Web3goClient) (*types.SuggestedGasFees, error) {
	if err := h.checkStatus(); err != nil {
		return nil, err
	}

	latestBlock, err := eth.Eth.BlockByNumber(ethtypes.LatestBlockNumber, false)
	if err != nil {
		return nil, err
	}

	baseFeePerGas := latestBlock.BaseFeePerGas
	// Calculate the gas fee stats from the priority fee window.
	stats := h.window.Calculate(h.config.Percentiles[:])

	if priorityFees := stats.AvgPercentiledPriorityFee; priorityFees == nil {
		// Use priority fee directly from the blockchain if no estimation were managed to make.
		oracleFee, err := eth.Eth.MaxPriorityFeePerGas()
		if err != nil {
			return nil, err
		}

		for i := 0; i < len(h.config.Percentiles); i++ {
			priorityFees = append(priorityFees, oracleFee)
		}
		stats.AvgPercentiledPriorityFee = priorityFees
	}

	return assembleSuggestedGasFees(baseFeePerGas, &stats), nil
}
