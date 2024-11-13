package catchup

import (
	"context"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	logutil "github.com/Conflux-Chain/go-conflux-util/log"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type worker struct {
	// worker name
	name string
	// result channel to collect queried epoch data
	resultChan chan []*store.EpochData
	// conflux sdk client delegated to fetch epoch data
	cfx sdk.ClientOperator
}

func mustNewWorker(name, nodeUrl string, chanSize int) *worker {
	return &worker{
		name:       name,
		resultChan: make(chan []*store.EpochData, chanSize),
		cfx:        rpc.MustNewCfxClient(nodeUrl),
	}
}

func (w *worker) Sync(ctx context.Context, wg *sync.WaitGroup, epochFrom, epochTo uint64, batchSize, stepN int) {
	batchSize = max(1, batchSize)
	defer wg.Done()

	etLogger := logutil.NewErrorTolerantLogger(logutil.DefaultETConfig)
	for eno := epochFrom; eno <= epochTo; {
		select {
		case <-ctx.Done():
			return
		default:
			targetEpoch := min(epochTo, eno+uint64(batchSize)-1)
			epochDatas, err := w.fetchEpochs(eno, targetEpoch)
			etLogger.Log(
				logrus.WithFields(logrus.Fields{
					"fromEpoch":  eno,
					"toEpoch":    targetEpoch,
					"batchSize":  batchSize,
					"workerName": w.name,
				}), err, "Catch-up worker failed to fetch epochs",
			)

			if err != nil {
				time.Sleep(time.Second)
				break
			}

			select {
			case <-ctx.Done():
				return
			case w.resultChan <- epochDatas:
				eno += uint64(stepN)
			}
		}
	}
}

func (w *worker) Close() {
	w.cfx.Close()
	close(w.resultChan)
}

func (w *worker) ResultChan() <-chan []*store.EpochData {
	return w.resultChan
}

func (w *worker) fetchEpochs(fromEpoch, toEpoch uint64) ([]*store.EpochData, error) {
	if fromEpoch > toEpoch {
		return nil, errors.New("invalid epoch range")
	}

	// If all chain data is disabled, use `cfx_getLogs` to fetch event logs for faster sync.
	// Otherwise, fetch full epoch data.
	disabler := store.StoreConfig()
	if disabler.IsChainBlockDisabled() && disabler.IsChainTxnDisabled() && disabler.IsChainReceiptDisabled() {
		return queryEpochDataWithLogsOnly(w.cfx, fromEpoch, toEpoch)
	}

	epochDatas := make([]*store.EpochData, 0, toEpoch-fromEpoch+1)
	for ; fromEpoch <= toEpoch; fromEpoch++ {
		epochData, err := store.QueryEpochData(w.cfx, fromEpoch, true)
		if err != nil {
			return nil, err
		}

		epochDatas = append(epochDatas, &epochData)
	}

	return epochDatas, nil
}

// queryEpochDataWithLogsOnly retrieves epoch data for the specified epoch range using only event logs.
// It constructs minimal epoch data, including blocks and transaction receipts, based on the retrieved logs.
func queryEpochDataWithLogsOnly(cfx sdk.ClientOperator, fromEpoch, toEpoch uint64) (res []*store.EpochData, err error) {
	startTime := time.Now()
	defer metrics.Registry.Sync.CatchupEpochData("cfx").UpdateSince(startTime)

	defer func() {
		metrics.Registry.Sync.CatchupEpochDataAvailability("cfx").Mark(err == nil)
	}()

	// Retrieve all event logs within the specified epoch range
	logs, err := store.QueryEventLogs(cfx, fromEpoch, toEpoch)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get event logs")
	}

	for epochNum := fromEpoch; epochNum <= toEpoch; epochNum++ {
		// Initialize epoch data for the current epoch
		epochData := &store.EpochData{
			Number:   epochNum,
			Receipts: make(map[types.Hash]*types.TransactionReceipt),
		}

		// Cache to store blocks fetched by their hash to avoid redundant network calls
		blockCache := make(map[types.Hash]*types.Block)

		// Process logs that belong to the current epoch
		for i := range logs {
			if logs[i].EpochNumber.ToInt().Uint64() != fromEpoch {
				logs = logs[i:]
				break
			}

			// Retrieve or fetch the block associated with the current log
			blockHash := *logs[i].BlockHash
			if _, ok := blockCache[blockHash]; !ok {
				var block *types.Block
				block, err = cfx.GetBlockByHash(blockHash)
				if err != nil {
					return nil, errors.WithMessagef(err, "failed to get block by hash %v", blockHash)
				}

				// Add the block to the epoch data and cache
				epochData.Blocks = append(epochData.Blocks, block)
				blockCache[blockHash] = block
			}

			// Retrieve or initialize the transaction receipt associated with the current log
			txnHash := *logs[i].TransactionHash
			txnReceipt, ok := epochData.Receipts[txnHash]
			if !ok {
				txnReceipt = &types.TransactionReceipt{
					EpochNumber:     (*hexutil.Uint64)(&epochNum),
					BlockHash:       blockHash,
					TransactionHash: txnHash,
				}

				epochData.Receipts[txnHash] = txnReceipt
			}

			// Append the current log to the transaction receipt's logs
			txnReceipt.Logs = append(txnReceipt.Logs, logs[i])
		}

		// Append the constructed epoch data to the result list
		res = append(res, epochData)
	}

	return res, nil
}
