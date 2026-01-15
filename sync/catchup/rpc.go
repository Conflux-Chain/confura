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
	_ IRpcClient = (*CoreRpcClient)(nil)
	_ IRpcClient = (*EvmRpcClient)(nil)
)

type FinalizationStatus struct {
	LatestFinalized  uint64
	LatestCheckpoint uint64
}

type IRpcClient interface {
	GetFinalizationStatus(ctx context.Context) (*FinalizationStatus, error)
	QueryEpochData(ctx context.Context, from, to uint64) ([]*store.EpochData, error)
	BoostQueryEpochData(ctx context.Context, from, to uint64) ([]*store.EpochData, error)
	Space() string
	Close()
}

type CoreRpcClient struct {
	sdk.ClientOperator
}

func MustNewCoreRpcClient(nodeUrl string) IRpcClient {
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

func (c *CoreRpcClient) getFirstBlockOfEpoch(epochNum uint64) (*cfxTypes.Block, error) {
	blockHashes, err := c.GetBlocksByEpoch(cfxTypes.NewEpochNumberUint64(epochNum))
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get blocks by epoch")
	}
	if len(blockHashes) == 0 {
		return nil, errors.Errorf("invalid epoch data (must have at least one block)")
	}
	block, err := c.GetBlockByHash(blockHashes[0])
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to get block by hash %v", blockHashes[0])
	}
	if block == nil {
		return nil, errors.Errorf("block %v not found", blockHashes[0])
	}
	return block, nil
}

func (c *CoreRpcClient) getPivotBlock(epochNum uint64) (*cfxTypes.Block, error) {
	block, err := c.GetBlockByEpoch(cfxTypes.NewEpochNumberUint64(epochNum))
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, errors.New("block not found")
	}
	return block, nil
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

		// Cache to store blocks fetched by their hash to avoid repeated network calls
		blockCache := make(map[cfxTypes.Hash]*cfxTypes.Block)

		// Ensure to add the first block within the range
		if epochNum == fromEpoch {
			block, err := c.getFirstBlockOfEpoch(epochNum)
			if err != nil {
				return nil, errors.WithMessagef(err, "failed to get the first block of epoch #%v", epochNum)
			}
			blockCache[block.Hash] = block
			epochData.Blocks = append(epochData.Blocks, block)
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
				block, err := c.GetBlockByHash(*blockHash)
				if err != nil {
					return nil, errors.WithMessagef(err, "failed to get block by hash %v", *blockHash)
				}
				if block == nil {
					return nil, errors.Errorf("block %v not found", *blockHash)
				}
				blockCache[*blockHash] = block
				epochData.Blocks = append(epochData.Blocks, block)
			}

			// Retrieve or initialize the transaction receipt associated with the current log
			txnHash := logs[logCursor].TransactionHash
			txnReceipt, ok := epochData.Receipts[*txnHash]
			if !ok {
				txnReceipt = &cfxTypes.TransactionReceipt{
					EpochNumber:     (hexutil.Uint64)(epochNum),
					BlockHash:       *blockHash,
					TransactionHash: *txnHash,
				}

				epochData.Receipts[*txnHash] = txnReceipt
			}

			// Append the current log to the transaction receipt's logs
			txnReceipt.Logs = append(txnReceipt.Logs, logs[logCursor])
		}

		// Ensure to add the last block within the range
		if epochNum == toEpoch {
			block, err := c.getPivotBlock(epochNum)
			if err != nil {
				return nil, errors.WithMessagef(err, "failed to get the pivot block of epoch #%v", epochNum)
			}
			epochData.Hash = &block.Hash
			if _, ok := blockCache[block.Hash]; !ok {
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

func MustNewEvmRpcClient(nodeUrl string) IRpcClient {
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

func (c *EvmRpcClient) getBlockByNumber(blockNum uint64) (*ethTypes.Block, error) {
	block, err := c.Eth.BlockByNumber(ethTypes.BlockNumber(blockNum), true)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, errors.New("block not found")
	}
	return block, nil
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
		// Initialize ETH data for the current block
		ethData := &store.EthData{
			Number:   blockNum,
			Receipts: make(map[common.Hash]*ethTypes.Receipt),
		}

		// Ensure to retrieve the first block within the range
		if blockNum == fromBlock {
			block, err := c.getBlockByNumber(blockNum)
			if err != nil {
				return nil, errors.WithMessagef(err, "failed to get block #%v", blockNum)
			}
			ethData.Block = block
		}

		// Process logs that belong to the current epoch
		for ; logCursor < len(logs); logCursor++ {
			if logs[logCursor].BlockNumber != blockNum {
				// Move to next ETH data construction if current log doesn't belong here
				break
			}

			if ethData.Block == nil {
				// Retrieve the block by number
				block, err := c.getBlockByNumber(blockNum)
				if err != nil {
					return nil, errors.WithMessagef(err, "failed to get block #%v", blockNum)
				}
				ethData.Block = block
			}

			// Retrieve or initialize the transaction receipt associated with the current log
			txnHash := logs[logCursor].TxHash
			txnReceipt, ok := ethData.Receipts[txnHash]
			if !ok {
				txnReceipt = &ethTypes.Receipt{
					BlockNumber:     blockNum,
					BlockHash:       ethData.Block.Hash,
					TransactionHash: txnHash,
				}
				ethData.Receipts[txnHash] = txnReceipt
			}

			// Append the current log to the transaction receipt's logs
			txnReceipt.Logs = append(txnReceipt.Logs, &logs[logCursor])
		}

		// Ensure to retrieve the last block within the range
		if blockNum == toBlock && ethData.Block == nil {
			block, err := c.getBlockByNumber(blockNum)
			if err != nil {
				return nil, errors.WithMessagef(err, "failed to get block #%v", blockNum)
			}
			ethData.Block = block
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
