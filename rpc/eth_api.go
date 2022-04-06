package rpc

import (
	"context"
	"fmt"
	"math/big"
	"math/bits"

	cimetrics "github.com/conflux-chain/conflux-infura/metrics"
	"github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/openweb3/web3go"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	ethHitStatsCollector = cimetrics.NewHitStatsCollector()
)

func getEthStoreHitRatioMetricKey(method string) string {
	return fmt.Sprintf("infura/rpc/call/%v/ethstore/hitratio", method)
}

// ethAPI provides ethereum relative API within EVM space according to:
// https://github.com/Pana/conflux-doc/blob/master/docs/evm_space_zh.md
type ethAPI struct {
	provider *node.EthClientProvider

	handler          ethHandler // eth rpc handler
	chainId          *uint64    // eth chain ID
	inputBlockMetric cimetrics.InputBlockMetric
}

func newEthAPI(provider *node.EthClientProvider, handler ethHandler) *ethAPI {
	return &ethAPI{provider: provider, handler: handler}
}

// GetBlockByHash returns the requested block. When fullTx is true all transactions in
// the block are returned in full detail, otherwise only the transaction hash is returned.
func (api *ethAPI) GetBlockByHash(
	ctx context.Context, blockHash common.Hash, fullTx bool,
) (*web3Types.Block, error) {
	logger := logrus.WithFields(logrus.Fields{
		"blockHash": blockHash.Hex(), "includeTxs": fullTx,
	})

	if !store.EthStoreConfig().IsChainBlockDisabled() && !util.IsInterfaceValNil(api.handler) {
		isStoreHit := false
		defer func(isHit *bool) {
			metricKey := getEthStoreHitRatioMetricKey("eth_getBlockByHash")
			ethHitStatsCollector.CollectHitStats(metricKey, *isHit)
		}(&isStoreHit)

		block, err := api.handler.GetBlockByHash(ctx, blockHash, fullTx)
		if err == nil {
			logger.Debug("Loading eth data for eth_getBlockByHash hit in the store")

			isStoreHit = true
			return block, nil
		}

		logger.WithError(err).Debug("Loading eth data for eth_getBlockByHash missed from the ethstore")
	}

	logger.Debug("Delegating eth_getBlockByHash rpc request to fullnode")

	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	return w3c.Eth.BlockByHash(blockHash, fullTx)
}

// ChainId returns the chainID value for transaction replay protection.
func (api *ethAPI) ChainId(ctx context.Context) (*hexutil.Uint64, error) {
	if api.chainId != nil {
		return (*hexutil.Uint64)(api.chainId), nil
	}

	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	chainId, err := w3c.Eth.ChainId()
	if err == nil {
		api.chainId = chainId
	}

	return (*hexutil.Uint64)(chainId), err
}

// BlockNumber returns the block number of the chain head.
func (api *ethAPI) BlockNumber(ctx context.Context) (*hexutil.Big, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	blockNum, err := w3c.Eth.BlockNumber()
	return (*hexutil.Big)(blockNum), err
}

// GetBalance returns the amount of wei for the given address in the state of the
// given block number.
func (api *ethAPI) GetBalance(
	ctx context.Context, address common.Address, blockNumOrHash *web3Types.BlockNumberOrHash,
) (*hexutil.Big, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	balance, err := w3c.Eth.Balance(address, blockNumOrHash)

	api.inputBlockMetric.Update2(blockNumOrHash, "eth_getBalance", w3c)
	return (*hexutil.Big)(balance), err
}

// GetBlockByNumber returns the requested canonical block.
// * When blockNr is -1 the chain head is returned.
// * When blockNr is -2 the pending chain head is returned.
// * When fullTx is true all transactions in the block are returned, otherwise
//   only the transaction hash is returned.
func (api *ethAPI) GetBlockByNumber(
	ctx context.Context, blockNum web3Types.BlockNumber, fullTx bool,
) (*web3Types.Block, error) {
	logger := logrus.WithFields(logrus.Fields{
		"blockNum": blockNum, "includeTxs": fullTx,
	})

	if !store.EthStoreConfig().IsChainBlockDisabled() && !util.IsInterfaceValNil(api.handler) {
		isStoreHit := false
		defer func(isHit *bool) {
			metricKey := getEthStoreHitRatioMetricKey("eth_getBlockByNumber")
			ethHitStatsCollector.CollectHitStats(metricKey, *isHit)
		}(&isStoreHit)

		block, err := api.handler.GetBlockByNumber(ctx, &blockNum, fullTx)
		if err == nil {
			logger.Debug("Loading eth data for eth_getBlockByNumber hit in the store")

			isStoreHit = true
			return block, nil
		}

		logger.WithError(err).Debug("Loading eth data for eth_getBlockByNumber missed from the ethstore")
	}

	logger.Debug("Delegating eth_getBlockByNumber rpc request to fullnode")

	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputBlockMetric.Update1(&blockNum, "eth_getBlockByNumber", w3c)
	return w3c.Eth.BlockByNumber(blockNum, fullTx)
}

// GetUncleByBlockNumberAndIndex returns the uncle block for the given block hash and index.
func (api *ethAPI) GetUncleByBlockNumberAndIndex(
	ctx context.Context, blockNr web3Types.BlockNumber, index hexutil.Uint,
) (*web3Types.Block, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputBlockMetric.Update1(&blockNr, "eth_getUncleByBlockNumberAndIndex", w3c)
	return w3c.Eth.UncleByBlockNumberAndIndex(blockNr, uint(index))
}

// GetUncleCountByBlockHash returns the number of uncles in a block from a block matching
// the given block hash.
func (api *ethAPI) GetUncleCountByBlockHash(ctx context.Context, hash common.Hash) (
	*hexutil.Big, error,
) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	count, err := w3c.Eth.BlockUnclesCountByHash(hash)
	return (*hexutil.Big)(count), err
}

// ProtocolVersion returns the current ethereum protocol version.
func (api *ethAPI) ProtocolVersion(ctx context.Context) (string, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return "", err
	}

	return w3c.Eth.ProtocolVersion()
}

// GasPrice returns the current gas price in wei.
func (api *ethAPI) GasPrice(ctx context.Context) (*hexutil.Big, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	gasPrice, err := w3c.Eth.GasPrice()
	return (*hexutil.Big)(gasPrice), err
}

// GetStorageAt returns the value from a storage position at a given address.
func (api *ethAPI) GetStorageAt(
	ctx context.Context, address common.Address, location *hexutil.Big, blockNumOrHash *web3Types.BlockNumberOrHash,
) (common.Hash, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return common.Hash{}, err
	}

	api.inputBlockMetric.Update2(blockNumOrHash, "eth_getStorageAt", w3c)
	return w3c.Eth.StorageAt(address, (*big.Int)(location), blockNumOrHash)
}

// GetCode returns the contract code of the given account.
// The block number can be nil, in which case the code is taken from the latest known block.
func (api *ethAPI) GetCode(
	ctx context.Context, account common.Address, blockNumOrHash *web3Types.BlockNumberOrHash,
) (hexutil.Bytes, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputBlockMetric.Update2(blockNumOrHash, "eth_getCode", w3c)
	return w3c.Eth.CodeAt(account, blockNumOrHash)
}

// GetTransactionCount returns the number of transactions (nonce) sent from the given account.
// The block number can be nil, in which case the nonce is taken from the latest known block.
func (api *ethAPI) GetTransactionCount(
	ctx context.Context, account common.Address, blockNumOrHash *web3Types.BlockNumberOrHash,
) (*hexutil.Big, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputBlockMetric.Update2(blockNumOrHash, "eth_getTransactionCount", w3c)
	count, err := w3c.Eth.TransactionCount(account, blockNumOrHash)
	return (*hexutil.Big)(count), err
}

// SendTransaction injects a signed transaction into the pending pool for execution.
//
// If the transaction was a contract creation use the TransactionReceipt method to get the
// contract address after the transaction has been mined.
func (api *ethAPI) SendRawTransaction(ctx context.Context, signedTx hexutil.Bytes) (common.Hash, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return common.Hash{}, err
	}

	return w3c.Eth.SendRawTransaction(signedTx)
}

// SubmitTransaction is an alias of `SendRawTransaction` method.
func (api *ethAPI) SubmitTransaction(ctx context.Context, signedTx hexutil.Bytes) (common.Hash, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return common.Hash{}, err
	}

	return w3c.Eth.SubmitTransaction(signedTx)
}

// Call executes a new message call immediately without creating a transaction on the block chain.
func (api *ethAPI) Call(
	ctx context.Context, request web3Types.CallRequest, blockNumOrHash *web3Types.BlockNumberOrHash,
) (hexutil.Bytes, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputBlockMetric.Update2(blockNumOrHash, "eth_call", w3c)
	return w3c.Eth.Call(request, blockNumOrHash)
}

// EstimateGas generates and returns an estimate of how much gas is necessary to allow the transaction
// to complete. The transaction will not be added to the blockchain. Note that the estimate may be
// significantly more than the amount of gas actually used by the transaction, for a variety of reasons
// including EVM mechanics and node performance or miner policy.
func (api *ethAPI) EstimateGas(
	ctx context.Context, request web3Types.CallRequest, blockNumOrHash *web3Types.BlockNumberOrHash,
) (*hexutil.Big, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputBlockMetric.Update2(blockNumOrHash, "eth_estimateGas", w3c)
	gas, err := w3c.Eth.EstimateGas(request, blockNumOrHash)
	return (*hexutil.Big)(gas), err
}

// TransactionByHash returns the transaction with the given hash.
func (api *ethAPI) GetTransactionByHash(ctx context.Context, hash common.Hash) (*web3Types.Transaction, error) {
	logger := logrus.WithField("txHash", hash.Hex())

	if !store.EthStoreConfig().IsChainTxnDisabled() && !util.IsInterfaceValNil(api.handler) {
		isStoreHit := false
		defer func(isHit *bool) {
			metricKey := getEthStoreHitRatioMetricKey("eth_getTransactionByHash")
			ethHitStatsCollector.CollectHitStats(metricKey, *isHit)
		}(&isStoreHit)

		tx, err := api.handler.GetTransactionByHash(ctx, hash)
		if err == nil {
			logger.Debug("Loading eth data for eth_getTransactionByHash hit in the store")

			isStoreHit = true
			return tx, nil
		}

		logger.WithError(err).Debug("Loading eth data for eth_getTransactionByHash missed from the ethstore")
	}

	logger.Debug("Delegating eth_getTransactionByHash rpc request to fullnode")

	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	return w3c.Eth.TransactionByHash(hash)
}

// TransactionReceipt returns the receipt of a transaction by transaction hash.
// Note that the receipt is not available for pending transactions.
func (api *ethAPI) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (*web3Types.Receipt, error) {
	logger := logrus.WithField("txHash", txHash.Hex())

	if !store.EthStoreConfig().IsChainReceiptDisabled() && !util.IsInterfaceValNil(api.handler) {
		isStoreHit := false
		defer func(isHit *bool) {
			metricKey := getEthStoreHitRatioMetricKey("eth_getTransactionReceipt")
			ethHitStatsCollector.CollectHitStats(metricKey, *isHit)
		}(&isStoreHit)

		tx, err := api.handler.GetTransactionReceipt(ctx, txHash)
		if err == nil {
			logger.Debug("Loading eth data for eth_getTransactionReceipt hit in the ethstore")

			isStoreHit = true
			return tx, nil
		}

		logger.WithError(err).Debug("Loading eth data for eth_getTransactionReceipt missed from the ethstore")
	}

	logger.Debug("Delegating eth_getTransactionReceipt rpc request to fullnode")

	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	return w3c.Eth.TransactionReceipt(txHash)
}

// GetLogs returns an array of all logs matching a given filter object.
// TODO: add offset/limit support
func (api *ethAPI) GetLogs(ctx context.Context, filter web3Types.FilterQuery) ([]web3Types.Log, error) {
	logger := logrus.WithField("filter", filter)
	emptyLogs := []web3Types.Log{}

	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	// Set `limit` parameter to default max value if not specified or exceeds max limit size
	if filter.Limit == nil || uint64(*filter.Limit) > store.MaxLogLimit {
		defaultLimit := uint(*defaultLogLimit)
		filter.Limit = &defaultLimit
	}

	if err := api.validateEthLogFilter(w3c, &filter); err != nil {
		logger.WithError(err).Debug("Invalid log filter parameter for eth_getLogs rpc request")
		return emptyLogs, err
	}

	if !store.EthStoreConfig().IsChainLogDisabled() && !util.IsInterfaceValNil(api.handler) {
		chainId, err := api.ChainId(ctx)
		if err != nil {
			return emptyLogs, errors.WithMessage(err, "failed to get ETH chain id")
		}

		if sfilter, ok := store.ParseEthLogFilter(w3c, uint32(*chainId), &filter); ok {
			isStoreHit := false
			defer func(isHit *bool) {
				metricKey := getEthStoreHitRatioMetricKey("eth_getLogs")
				ethHitStatsCollector.CollectHitStats(metricKey, *isHit)
			}(&isStoreHit)

			if logs, err := api.handler.GetLogs(ctx, *sfilter); err == nil {
				// return empty slice rather than nil to comply with fullnode
				if logs == nil {
					logs = emptyLogs
				}

				logger.Debug("Loading eth data for eth_getLogs hit in the ethstore")

				isStoreHit = true
				return logs, nil
			}

			logger.WithError(err).Debug("Loading eth data for eth_getLogs hit missed from the ethstore")

			// Logs already pruned from database? If so, we'd rather not delegate this request to
			// the fullnode, as it might crush our fullnode.
			if errors.Is(err, store.ErrAlreadyPruned) {
				logger.Info("Event logs data already pruned for eth_getLogs to return")
				return emptyLogs, errors.New("failed to get stale event logs (data too old)")
			}
		}
	}

	logger.Debug("Delegating eth_getLogs rpc request to fullnode")

	return w3c.Eth.Logs(filter)
}

// GetBlockTransactionCountByHash returns the total number of transactions in the given block.
func (api *ethAPI) GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (*hexutil.Big, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	count, err := w3c.Eth.BlockTransactionCountByHash(blockHash)
	return (*hexutil.Big)(count), err
}

// GetBlockTransactionCountByNumber returns the number of transactions in the block with the given
// block number.
func (api *ethAPI) GetBlockTransactionCountByNumber(ctx context.Context, blockNum web3Types.BlockNumber) (
	*hexutil.Big, error,
) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputBlockMetric.Update1(&blockNum, "eth_getBlockTransactionCountByNumber", w3c)
	count, err := w3c.Eth.BlockTransactionCountByNumber(blockNum)
	return (*hexutil.Big)(count), err
}

// Syncing returns an object with data about the sync status or false.
// https://openethereum.github.io/JSONRPC-eth-module#eth_syncing
func (api *ethAPI) Syncing(ctx context.Context) (web3Types.SyncStatus, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return web3Types.SyncStatus{}, err
	}

	return w3c.Eth.Syncing()
}

// Hashrate returns the number of hashes per second that the node is mining with.
// Only applicable when the node is mining.
func (api *ethAPI) Hashrate(ctx context.Context) (*hexutil.Big, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	hashrate, err := w3c.Eth.Hashrate()
	return (*hexutil.Big)(hashrate), err
}

// Coinbase returns the client coinbase address..
func (api *ethAPI) Coinbase(ctx context.Context) (common.Address, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return common.Address{}, err
	}

	return w3c.Eth.Author()
}

// Mining returns true if client is actively mining new blocks.
func (api *ethAPI) Mining(ctx context.Context) (bool, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return false, err
	}

	return w3c.Eth.IsMining()
}

// MaxPriorityFeePerGas returns a fee per gas that is an estimate of how much you can pay as
// a priority fee, or "tip", to get a transaction included in the current block.
func (api *ethAPI) MaxPriorityFeePerGas(ctx context.Context) (*hexutil.Big, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	priorityFee, err := w3c.Eth.MaxPriorityFeePerGas()
	return (*hexutil.Big)(priorityFee), err
}

// Accounts returns a list of addresses owned by client.
func (api *ethAPI) Accounts(ctx context.Context) ([]common.Address, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	return w3c.Eth.Accounts()
}

// SubmitHashrate used for submitting mining hashrate.
func (api *ethAPI) SubmitHashrate(
	ctx context.Context, hashrate *hexutil.Big, clientId common.Hash,
) (bool, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return false, err
	}

	return w3c.Eth.SubmitHashrate((*big.Int)(hashrate), clientId)
}

// GetUncleByBlockHashAndIndex returns information about the 'Uncle' of a block by hash and
// the Uncle index position.
func (api *ethAPI) GetUncleByBlockHashAndIndex(
	ctx context.Context, hash common.Hash, index hexutil.Uint,
) (*web3Types.Block, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	return w3c.Eth.UncleByBlockHashAndIndex(hash, index)
}

// GetTransactionByBlockHashAndIndex returns information about a transaction by block hash and
// transaction index position.
func (api *ethAPI) GetTransactionByBlockHashAndIndex(
	ctx context.Context, hash common.Hash, index hexutil.Uint,
) (*web3Types.Transaction, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	return w3c.Eth.TransactionByBlockHashAndIndex(hash, uint(index))
}

// GetTransactionByBlockNumberAndIndex returns information about a transaction by block number and
// transaction index position.
func (api *ethAPI) GetTransactionByBlockNumberAndIndex(
	ctx context.Context, blockNum web3Types.BlockNumber, index hexutil.Uint,
) (*web3Types.Transaction, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputBlockMetric.Update1(&blockNum, "eth_getTransactionByBlockNumberAndIndex", w3c)
	return w3c.Eth.TransactionByBlockNumberAndIndex(blockNum, uint(index))
}

func (api *ethAPI) validateEthLogFilter(w3c *web3go.Client, filter *web3Types.FilterQuery) error {
	var filterFlag store.LogFilterType // filter type set flag bitwise

	if filter.FromBlock != nil || filter.ToBlock != nil { // check if block range provided
		filterFlag |= store.LogFilterTypeBlockRange
	}

	if filter.BlockHash != nil { // check if block hash provided
		filterFlag |= store.LogFilterTypeBlockHash
	}

	if bits.OnesCount(uint(filterFlag)) > 1 { // different types of log filters are mutual exclusion
		return errInvalidLogFilter
	}

	if filter.BlockHash == nil { // normalize block number only if block hash not provided
		defaultBlockNo := rpc.LatestBlockNumber

		for _, ptrBlockNum := range []**rpc.BlockNumber{&filter.FromBlock, &filter.ToBlock} {
			if *ptrBlockNum == nil {
				*ptrBlockNum = &defaultBlockNo
			}

			blockNum, err := util.NormalizeEthBlockNumber(w3c, *ptrBlockNum)
			if err != nil {
				return errors.WithMessage(err, "failed to normalize block number")
			}
			*ptrBlockNum = blockNum
		}

		if *filter.FromBlock > *filter.ToBlock {
			return errors.New("invalid block range (from block larger than to block)")
		}

		if count := *filter.ToBlock - *filter.FromBlock + 1; uint64(count) > store.MaxLogBlockRange {
			return errors.Errorf("block range exceeds maximum value %v", store.MaxLogBlockRange)
		}
	}

	return nil
}

// The following RPC methods are not supported yet by the fullnode:
// `eth_feeHistory`
// `eth_getFilterChanges`
// `eth_getFilterLogs`
// `eth_newBlockFilter`
// `eth_newFilter`
// `eth_newPendingTransactionFilter`
// `eth_uninstallFilter`
