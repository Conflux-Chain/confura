package nearhead

import (
	"math/big"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/util/metrics"
	"github.com/pkg/errors"
)

var (
	statEpochsGauge = metrics.GetOrRegisterGauge("infura/nearhead/window/epochs")
	statBlocksGauge = metrics.GetOrRegisterGauge("infura/nearhead/window/blocks")
	statTxsGauge    = metrics.GetOrRegisterGauge("infura/nearhead/window/txs")
	statLogsGauge   = metrics.GetOrRegisterGauge("infura/nearhead/window/logs")
)

// tx may be included in different blocks
type sharedTx struct {
	tx       *types.Transaction
	receipt  *types.TransactionReceipt
	refCount uint // reference count
}

func (tx *sharedTx) deref() uint {
	tx.refCount--
	return tx.refCount
}

func (tx *sharedTx) ref() uint {
	tx.refCount++
	return tx.refCount
}

// blockWindow maintains blocks within a window, which move forward along with epoch increased.
// Note, all operations in this struct are not thread safe.
type blockWindow struct {
	// maximum number of epoch slot in the window
	cap uint64

	// lower and upper bound of epoch numbers in the window
	minEpoch uint64
	maxEpoch uint64

	// indexed blocks, transactions and receipts
	epoch2Blocks map[uint64][]*types.Block
	hash2Blocks  map[string]*types.Block
	hash2Txs     map[string]*sharedTx
}

func newBlockWindow(cap uint64) *blockWindow {
	if cap == 0 {
		panic("invalid cap for block window")
	}

	return &blockWindow{
		cap:          cap,
		hash2Blocks:  make(map[string]*types.Block),
		epoch2Blocks: make(map[uint64][]*types.Block),
		hash2Txs:     make(map[string]*sharedTx),
	}
}

func (window *blockWindow) size() uint64 {
	if window.minEpoch == 0 {
		return 0
	}

	return window.maxEpoch - window.minEpoch + 1
}

func (window *blockWindow) remove(epoch uint64) {
	blocks, ok := window.epoch2Blocks[epoch]
	if !ok {
		return
	}

	delete(window.epoch2Blocks, epoch)
	statEpochsGauge.Dec(1)

	for _, block := range blocks {
		delete(window.hash2Blocks, block.Hash.String())
		statBlocksGauge.Dec(1)

		for _, tx := range block.Transactions {
			txHash := tx.Hash.String()
			sharedTx := window.hash2Txs[txHash]
			if sharedTx.deref() == 0 {
				delete(window.hash2Txs, txHash)
				statTxsGauge.Dec(1)
				if sharedTx.receipt != nil {
					statLogsGauge.Dec(int64(len(sharedTx.receipt.Logs)))
				}
			}
		}
	}

	if len(window.epoch2Blocks) == 0 {
		window.minEpoch = 0
		window.maxEpoch = 0
	} else if epoch == window.minEpoch {
		for _, ok := window.epoch2Blocks[window.minEpoch]; !ok; window.minEpoch++ {
		}
	} else if epoch == window.maxEpoch {
		for _, ok := window.epoch2Blocks[window.maxEpoch]; !ok; window.maxEpoch-- {
		}
	}
}

func (window *blockWindow) put(epoch uint64, blocks []*types.Block, receipts []*types.TransactionReceipt) {
	window.remove(epoch)

	window.epoch2Blocks[epoch] = blocks
	statEpochsGauge.Inc(1)

	for _, block := range blocks {
		window.hash2Blocks[block.Hash.String()] = block
		statBlocksGauge.Inc(1)

		for _, tx := range block.Transactions {
			txHash := tx.Hash.String()
			if existTx, ok := window.hash2Txs[txHash]; ok {
				existTx.ref()
			} else {
				window.hash2Txs[txHash] = &sharedTx{&tx, nil, 1}
				statTxsGauge.Inc(1)
			}
		}
	}

	for _, receipt := range receipts {
		txHash := receipt.TransactionHash.String()
		if sharedTx := window.hash2Txs[txHash]; sharedTx.receipt == nil {
			sharedTx.receipt = receipt
			statLogsGauge.Inc(int64(len(receipt.Logs)))
		}
	}

	if window.minEpoch == 0 || window.minEpoch > epoch {
		window.minEpoch = epoch
	}

	if window.maxEpoch == 0 || window.maxEpoch < epoch {
		window.maxEpoch = epoch
	}
}

func (window *blockWindow) push(epoch uint64, blocks []*types.Block, receipts []*types.TransactionReceipt) bool {
	if window.maxEpoch > 0 && window.maxEpoch+1 != epoch {
		return false
	}

	window.put(epoch, blocks, receipts)

	if window.size() > window.cap {
		window.remove(window.minEpoch)
	}

	return true
}

func (window *blockWindow) pop() bool {
	if window.maxEpoch == 0 {
		return false
	}

	window.remove(window.maxEpoch)

	return true
}

func (window *blockWindow) peek() ([]*types.Block, bool) {
	if window.maxEpoch == 0 {
		return nil, false
	}

	blocks, ok := window.epoch2Blocks[window.maxEpoch]

	return blocks, ok
}

func (window *blockWindow) getLogs(filter *logFilter) ([]types.Log, bool) {
	if filter.epochFrom > filter.epochTo || filter.epochFrom < window.minEpoch || filter.epochTo > window.maxEpoch {
		return nil, false
	}

	var logs []types.Log

	for epoch := filter.epochFrom; epoch <= filter.epochTo; epoch++ {
		for _, block := range window.epoch2Blocks[epoch] {
			if blockHash := block.Hash.String(); filter.matchesBlock(blockHash) {
				for _, tx := range block.Transactions {
					for _, log := range window.hash2Txs[tx.Hash.String()].receipt.Logs {
						// ignore if log.BlockHash mismatch with current handled block
						if log.BlockHash != nil && log.BlockHash.String() == blockHash && filter.matches(&log) {
							logs = append(logs, log)
							if len(logs) >= filter.limit {
								return logs, true
							}
						}
					}
				}
			}
		}
	}

	return logs, true
}

func getBlocks(cfx sdk.ClientOperator, epochNumber *big.Int) ([]*types.Block, error) {
	blockHashes, err := cfx.GetBlocksByEpoch(types.NewEpochNumberBig(epochNumber))
	if err != nil {
		return nil, errors.WithMessagef(err, "Failed to get blocks by epoch %v", epochNumber)
	}

	var blocks []*types.Block

	for _, hash := range blockHashes {
		block, err := cfx.GetBlockByHash(hash)
		if err != nil {
			return nil, errors.WithMessagef(err, "Failed to get block by hash %v", hash)
		}

		blocks = append(blocks, block)
	}

	return blocks, nil
}

func getExecutedBlocks(cfx sdk.ClientOperator, epochNumber *big.Int) ([]*types.Block, []*types.TransactionReceipt, error) {
	blocks, err := getBlocks(cfx, epochNumber)
	if err != nil {
		return nil, nil, errors.WithMessagef(err, "Failed to get blocks by epoch %v", epochNumber)
	}

	// TODO enhance full node RPC to return receipts by block hash
	var receipts []*types.TransactionReceipt
	var logIndex uint64

	for _, block := range blocks {
		for _, tx := range block.Transactions {
			receipt, err := cfx.GetTransactionReceipt(tx.Hash)
			if err != nil {
				return nil, nil, errors.WithMessagef(err, "Failed to get receipt by hash %v", tx.Hash)
			}

			// TODO enhance full node RPC to return entire log info in transaction receipt
			var logs []types.Log
			var txLogIndex uint64

			for _, log := range receipt.Logs {
				log.BlockHash = &receipt.BlockHash
				log.EpochNumber = types.NewBigInt(uint64(*receipt.EpochNumber))
				log.TransactionHash = &receipt.TransactionHash
				log.TransactionIndex = types.NewBigInt(uint64(receipt.Index))
				log.LogIndex = types.NewBigInt(logIndex)
				log.TransactionLogIndex = types.NewBigInt(txLogIndex)
				logs = append(logs, log)

				txLogIndex++
				logIndex++
			}
			receipt.Logs = logs

			receipts = append(receipts, receipt)
		}
	}

	return blocks, receipts, nil
}
