package catchup

import (
	"context"
	"sync/atomic"

	"github.com/Conflux-Chain/confura/rpc/cfxbridge"
	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	cfxTypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/openweb3/web3go"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

var (
	_ AbstractRpcClient = (*CoreRpcClient)(nil)
	_ AbstractRpcClient = (*EvmRpcClient)(nil)
)

type FinalizationStatus struct {
	LatestFinalized  uint64
	LatestCheckpoint uint64
}

type AbstractRpcClient interface {
	GetFinalizationStatus(ctx context.Context) (*FinalizationStatus, error)
	QueryEpochData(ctx context.Context, from, to uint64) ([]*store.EpochData, error)
	BoostQueryEpochData(ctx context.Context, from, to uint64) ([]*store.EpochData, error)
	Space() string
	Close()
}

type CoreRpcClient struct {
	sdk.ClientOperator
}

func MustNewCoreRpcClient(nodeUrl string) AbstractRpcClient {
	return NewCoreRpcClient(rpc.MustNewCfxClient(nodeUrl))
}

func NewCoreRpcClient(cfx sdk.ClientOperator) *CoreRpcClient {
	return &CoreRpcClient{cfx}
}

func (c *CoreRpcClient) GetFinalizationStatus(ctx context.Context) (*FinalizationStatus, error) {
	status, err := c.GetStatus()
	if err != nil {
		return nil, err
	}

	return &FinalizationStatus{
		LatestFinalized:  uint64(status.LatestFinalized),
		LatestCheckpoint: uint64(status.LatestCheckpoint),
	}, nil
}

func (c *CoreRpcClient) Space() string {
	return "cfx"
}

func (c *CoreRpcClient) QueryEpochData(ctx context.Context, fromEpoch, toEpoch uint64) (res []*store.EpochData, err error) {
	if fromEpoch > toEpoch {
		return nil, errors.New("invalid epoch range")
	}

	for epochNo := fromEpoch; epochNo <= toEpoch; epochNo++ {
		epochData, err := store.QueryEpochData(c, fromEpoch, true)
		if err != nil {
			return nil, err
		}
		res = append(res, &epochData)
	}

	return res, err
}

func (c *CoreRpcClient) BoostQueryEpochData(ctx context.Context, fromEpoch, toEpoch uint64) (res []*store.EpochData, err error) {
	if fromEpoch > toEpoch {
		return nil, errors.New("invalid epoch range")
	}

	// Retrieve event logs within the specified epoch range
	logFilter := cfxTypes.LogFilter{
		FromEpoch: cfxTypes.NewEpochNumberUint64(fromEpoch),
		ToEpoch:   cfxTypes.NewEpochNumberUint64(toEpoch),
	}
	logs, err := c.GetLogs(logFilter)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get event logs")
	}

	var logCursor int
	for epochNum := fromEpoch; epochNum <= toEpoch; epochNum++ {
		// Initialize epoch data for the current epoch
		epochData := &store.EpochData{
			Number:   epochNum,
			Receipts: make(map[cfxTypes.Hash]*cfxTypes.TransactionReceipt),
		}

		var blockHashes []cfxTypes.Hash
		blockHashes, err = c.GetBlocksByEpoch(cfxTypes.NewEpochNumberUint64(epochNum))
		if err != nil {
			return nil, errors.WithMessagef(err, "failed to get blocks by epoch %v", epochNum)
		}
		if len(blockHashes) == 0 {
			err = errors.Errorf("invalid epoch data (must have at least one block)")
			return nil, err
		}

		// Cache to store blocks fetched by their hash to avoid repeated network calls
		blockCache := make(map[cfxTypes.Hash]*cfxTypes.Block)

		// Get the first and last block of the epoch
		for _, bh := range []cfxTypes.Hash{blockHashes[0], blockHashes[len(blockHashes)-1]} {
			if _, ok := blockCache[bh]; ok {
				continue
			}

			var block *cfxTypes.Block
			block, err = c.GetBlockByHash(bh)
			if err != nil {
				return nil, errors.WithMessagef(err, "failed to get block by hash %v", bh)
			}
			if block == nil {
				err = errors.Errorf("block %v not found", bh)
				return nil, err
			}
			blockCache[bh] = block
		}

		// Process logs that belong to the current epoch
		for ; logCursor < len(logs); logCursor++ {
			if logs[logCursor].EpochNumber.ToInt().Uint64() != epochNum {
				// Move to next epoch data construction if current log doesn't belong here
				break
			}

			// Retrieve or fetch the block associated with the current log
			blockHash := logs[logCursor].BlockHash
			if _, ok := blockCache[*blockHash]; !ok {
				var block *cfxTypes.Block
				block, err = c.GetBlockByHash(*blockHash)
				if err != nil {
					return nil, errors.WithMessagef(err, "failed to get block by hash %v", *blockHash)
				}
				if block == nil {
					err = errors.Errorf("block %v not found", *blockHash)
					return nil, err
				}
				blockCache[*blockHash] = block
			}

			// Retrieve or initialize the transaction receipt associated with the current log
			txnHash := logs[logCursor].TransactionHash
			txnReceipt, ok := epochData.Receipts[*txnHash]
			if !ok {
				txnReceipt = &cfxTypes.TransactionReceipt{
					EpochNumber:     (*hexutil.Uint64)(&epochNum),
					BlockHash:       *blockHash,
					TransactionHash: *txnHash,
				}

				epochData.Receipts[*txnHash] = txnReceipt
			}

			// Append the current log to the transaction receipt's logs
			txnReceipt.Logs = append(txnReceipt.Logs, logs[logCursor])
		}

		// Append all necessary blocks for the epoch
		for _, bh := range blockHashes {
			if block, ok := blockCache[bh]; ok {
				epochData.Blocks = append(epochData.Blocks, block)
			}
		}

		// Append the constructed epoch data to the result list
		res = append(res, epochData)
	}

	if logCursor != len(logs) {
		err = errors.Errorf("failed to process all logs: processed %v, total %v", logCursor, len(logs))
		return nil, err
	}

	return res, nil
}

type EvmRpcClient struct {
	*web3go.Client
	chainId atomic.Value
}

func MustNewEvmRpcClient(nodeUrl string) AbstractRpcClient {
	return NewEvmRpcClient(rpc.MustNewEthClient(nodeUrl))
}

func NewEvmRpcClient(w3c *web3go.Client) *EvmRpcClient {
	return &EvmRpcClient{Client: w3c}
}

func (e *EvmRpcClient) ChainId() (uint64, error) {
	if v, ok := e.chainId.Load().(*uint64); ok {
		return *v, nil
	}

	ethChainId, err := e.Eth.ChainId()
	if err != nil {
		return 0, err
	}
	e.chainId.Store(ethChainId)
	return *ethChainId, nil
}

func (e *EvmRpcClient) GetFinalizationStatus(ctx context.Context) (*FinalizationStatus, error) {
	block, err := e.Eth.BlockByNumber(ethTypes.FinalizedBlockNumber, false)
	if err != nil {
		return nil, err
	}
	return &FinalizationStatus{
		LatestFinalized: block.Number.Uint64(),
	}, nil
}

func (c *EvmRpcClient) Space() string {
	return "eth"
}

func (c *EvmRpcClient) QueryEpochData(ctx context.Context, fromBlock, toBlock uint64) ([]*store.EpochData, error) {
	if fromBlock > toBlock {
		return nil, errors.New("invalid block range")
	}

	chainId, err := c.ChainId()
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get chain ID")
	}

	ethDataSlice := make([]*store.EthData, 0, toBlock-fromBlock+1)
	for blockNo := fromBlock; blockNo <= toBlock; blockNo++ {
		data, err := store.QueryEthData(ctx, c.Client, blockNo)
		if err != nil {
			return nil, err
		}
		ethDataSlice = append(ethDataSlice, data)
	}

	// Convert eth data to epoch data
	epochDataSlice := make([]*store.EpochData, 0, len(ethDataSlice))
	for i := 0; i < len(ethDataSlice); i++ {
		epochData := cfxbridge.ConvertToEpochData(ethDataSlice[i], uint32(chainId))
		epochDataSlice = append(epochDataSlice, epochData)
	}

	return epochDataSlice, nil
}

func (c *EvmRpcClient) BoostQueryEpochData(ctx context.Context, fromBlock, toBlock uint64) ([]*store.EpochData, error) {
	if fromBlock > toBlock {
		return nil, errors.New("invalid block range")
	}

	chainId, err := c.ChainId()
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get chain ID")
	}

	// Retrieve event logs within the specified block range
	fromBn, toBn := ethTypes.BlockNumber(fromBlock), ethTypes.BlockNumber(toBlock)
	logFilter := ethTypes.FilterQuery{FromBlock: &fromBn, ToBlock: &toBn}
	logs, err := c.Eth.Logs(logFilter)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get event logs")
	}

	var logCursor int
	ethDataSlice := make([]*store.EthData, 0, toBlock-fromBlock+1)

	for blockNum := fromBlock; blockNum <= toBlock; blockNum++ {
		// Retrieve the block by number
		block, err := c.Eth.BlockByNumber(ethTypes.BlockNumber(blockNum), true)
		if err != nil {
			return nil, errors.WithMessagef(err, "failed to get block by number %v", blockNum)
		}
		if block == nil {
			return nil, errors.New("empty block data")
		}

		// Initialize ETH data for the current block
		ethData := &store.EthData{
			Number:   blockNum,
			Block:    block,
			Receipts: make(map[common.Hash]*ethTypes.Receipt),
		}

		// Process logs that belong to the current epoch
		for ; logCursor < len(logs); logCursor++ {
			if logs[logCursor].BlockNumber != blockNum {
				// Move to next ETH data construction if current log doesn't belong here
				break
			}

			// Retrieve or initialize the transaction receipt associated with the current log
			txnHash := logs[logCursor].TxHash
			txnReceipt, ok := ethData.Receipts[txnHash]
			if !ok {
				txnReceipt = &ethTypes.Receipt{
					BlockNumber:     blockNum,
					BlockHash:       block.Hash,
					TransactionHash: txnHash,
				}
				ethData.Receipts[txnHash] = txnReceipt
			}

			// Append the current log to the transaction receipt's logs
			txnReceipt.Logs = append(txnReceipt.Logs, &logs[logCursor])
		}

		// Append the constructed epoch data to the result list
		ethDataSlice = append(ethDataSlice, ethData)
	}

	if logCursor != len(logs) {
		err = errors.Errorf("failed to process all logs: processed %v, total %v", logCursor, len(logs))
		return nil, err
	}

	// Convert eth data to epoch data
	res := make([]*store.EpochData, 0, len(ethDataSlice))
	for i := 0; i < len(ethDataSlice); i++ {
		epochData := cfxbridge.ConvertToEpochData(ethDataSlice[i], uint32(chainId))
		res = append(res, epochData)
	}

	return res, nil
}
