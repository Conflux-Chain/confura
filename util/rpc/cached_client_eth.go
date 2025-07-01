package rpc

import (
	"context"
	"math/big"
	"sync"

	"github.com/Conflux-Chain/confura-data-cache/nearhead"
	cacheRpc "github.com/Conflux-Chain/confura-data-cache/rpc"
	cacheSync "github.com/Conflux-Chain/confura-data-cache/sync"
	cacheTypes "github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	lruCache "github.com/Conflux-Chain/confura/util/rpc/cache"
	viperutil "github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/go-rpc-provider/interfaces"
	providers "github.com/openweb3/go-rpc-provider/provider_wrapper"
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/client"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	_ util.EthBlockNumberResolver = (*cachedEthBlockNumberResolver)(nil)
)

type NearHeadConfig struct {
	Enabled bool
	Cache   nearhead.Config
	Sync    cacheSync.EthConfig
}

type EthNearHeadCache struct {
	*nearhead.EthCache
	stopSync context.CancelFunc
}

func newDummyEthNearHeadCache() *EthNearHeadCache {
	return &EthNearHeadCache{
		EthCache: nearhead.NewEthCache(nearhead.Config{}),
	}
}

func (c *EthNearHeadCache) Close() {
	if c.stopSync != nil {
		c.stopSync()
	}
}

func NewEthNearHeadCacheFromViper(url string) (*EthNearHeadCache, error) {
	var conf NearHeadConfig
	if err := viperutil.UnmarshalKey("nearhead", &conf); err != nil {
		return nil, errors.WithMessage(err, "failed to unmarshal near head config")
	}

	if !conf.Enabled {
		logrus.Info("Near head cache is disabled by configuration.")
		return nil, nil
	}

	cache := nearhead.NewEthCache(conf.Cache)

	conf.Sync.Extract.RpcEndpoint = url
	syncer, err := cacheSync.NewEthNearHeadSyncer(conf.Sync, cache)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create near head syncer")
	}

	// Start near head sync.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		wg := sync.WaitGroup{}
		wg.Add(1)
		syncer.Run(ctx, &wg)
		wg.Wait()
	}()

	return &EthNearHeadCache{EthCache: cache, stopSync: cancel}, nil
}

func MustNewEthDataCacheClientFromViper() cacheRpc.Interface {
	url := viper.GetString("requestControl.ethCache.dataCacheRpcUrlProto")
	if len(url) == 0 {
		logrus.Info("ETH data cache client URL not configured, use noop client.")
		return cacheRpc.NotFoundImpl
	}

	client, err := cacheRpc.NewClientProto(url, ethClientCfg.RequestTimeout)
	if err != nil {
		logrus.WithField("url", url).WithError(err).Fatal("Failed to create ETH data cache client")
	}
	return client
}

type cachedEthBlockNumberResolver struct {
	nodeName string
	eth      *client.RpcEthClient
}

func newCachedEthBlockNumberResolver(nodeName string, eth *client.RpcEthClient) cachedEthBlockNumberResolver {
	return cachedEthBlockNumberResolver{nodeName: nodeName, eth: eth}
}

// Resolve implements the `util.EthBlockNumberResolver` interface.
// It resolves block tags (e.g., "latest", "pending") into concrete block numbers.
func (r cachedEthBlockNumberResolver) Resolve(blockNum web3Types.BlockNumber) (uint64, error) {
	// Positive block numbers are already concrete.
	if blockNum > 0 {
		return uint64(blockNum), nil
	}

	// Resolve block tags (e.g., EarliestBlockNumber, PendingBlockNumber, LatestBlockNumber)
	// using the LRU cache to avoid direct RPC calls if possible.
	realBlockNum, _, err := lruCache.EthDefault.GetBlockNumber(r.nodeName, r.eth, blockNum)
	if err != nil {
		return 0, errors.WithMessagef(err, "failed to resolve block number for tag %v", blockNum)
	}
	return realBlockNum.Uint64(), nil
}

// Web3goClient wraps web3go.Client with additional caching and resolution capabilities.
type Web3goClient struct {
	*web3go.Client
	util.EthBlockNumberResolver

	URL   string
	Eth   cachedRpcEthClient
	Trace cachedRpcTraceClient

	nearhead *EthNearHeadCache
}

func NewWeb3goClient(
	url string,
	client *web3go.Client,
	dataCache cacheRpc.Interface,
	nearHeadCache *EthNearHeadCache,
) (*Web3goClient, error) {
	if nearHeadCache == nil {
		nearHeadCache = newDummyEthNearHeadCache()
	}

	nodeName := Url2NodeName(url)
	resolver := newCachedEthBlockNumberResolver(nodeName, client.Eth)
	commonCacheFields := commonClientCacheFields{
		nodeName:  nodeName,
		resolver:  resolver,
		dataCache: dataCache,
		nearhead:  nearHeadCache,
	}

	return &Web3goClient{
		URL:                    url,
		Client:                 client,
		EthBlockNumberResolver: resolver,
		Eth: cachedRpcEthClient{
			RpcEthClient:            client.Eth,
			commonClientCacheFields: commonCacheFields,
		},
		Trace: cachedRpcTraceClient{
			RpcTraceClient:          client.Trace,
			commonClientCacheFields: commonCacheFields,
		},
		nearhead: nearHeadCache,
	}, nil
}

func (w3c Web3goClient) NodeName() string {
	return Url2NodeName(w3c.URL)
}

func (w3c Web3goClient) Close() {
	w3c.nearhead.Close()
	w3c.Client.Close()
}

// commonClientCacheFields holds common fields for cached RPC clients.
type commonClientCacheFields struct {
	nodeName  string
	dataCache cacheRpc.Interface
	nearhead  *EthNearHeadCache
	resolver  util.EthBlockNumberResolver
}

type cachedRpcEthClient struct {
	*client.RpcEthClient
	commonClientCacheFields
}

func (c cachedRpcEthClient) ChainId() (*uint64, error) {
	return queryLRUCacheAndMetric("eth_chainId", func() (*uint64, bool, error) {
		return lruCache.EthDefault.GetChainId(c.RpcEthClient)
	})
}

func (c cachedRpcEthClient) GasPrice() (*big.Int, error) {
	return queryLRUCacheAndMetric("eth_gasPrice", func() (*big.Int, bool, error) {
		return lruCache.EthDefault.GetGasPrice(c.RpcEthClient)
	})
}

func (c cachedRpcEthClient) BlockNumber() (*big.Int, error) {
	return queryLRUCacheAndMetric("eth_blockNumber", func() (*big.Int, bool, error) {
		return lruCache.EthDefault.GetBlockNumber(c.nodeName, c.RpcEthClient)
	})
}

func (c cachedRpcEthClient) Call(callRequest web3Types.CallRequest, blockNum *web3Types.BlockNumberOrHash) ([]byte, error) {
	res, loaded, err := lruCache.EthDefault.Call(c.nodeName, c.RpcEthClient, callRequest, blockNum)
	if err != nil {
		return nil, err
	}

	metrics.Registry.Client.ExpiryCacheHit("eth_call").Mark(loaded)
	if res.RpcError != nil {
		return nil, res.RpcError
	}
	return res.Data.([]byte), nil
}

func (c cachedRpcEthClient) NetVersion() (string, error) {
	return queryLRUCacheAndMetric("net_version", func() (string, bool, error) {
		return lruCache.EthDefault.GetNetVersion(c.RpcEthClient)
	})
}

func (c cachedRpcEthClient) ClientVersion() (string, error) {
	return queryLRUCacheAndMetric("web3_clientVersion", func() (string, bool, error) {
		return lruCache.EthDefault.GetClientVersion(c.RpcEthClient)
	})
}

func (c cachedRpcEthClient) Logs(filter web3Types.FilterQuery) ([]web3Types.Log, error) {
	if filter.BlockHash != nil {
		return c.getLogsByBlockHash(filter)
	}

	fromBlock, toBlock, err := c.resolveBlockRange(filter)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to resolve block numbers")
	}

	return c.getLogsByBlockRange(filter, fromBlock, toBlock)
}

func (c cachedRpcEthClient) getLogsByBlockRange(filter web3Types.FilterQuery, fromBlock, toBlock uint64) ([]web3Types.Log, error) {
	ethLogs, err := c.nearhead.GetLogsByBlockRange(fromBlock, toBlock, nearhead.FilterOpt{
		Addresses: filter.Addresses,
		Topics:    filter.Topics,
	})
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get logs from near head cache")
	}

	metrics.Registry.Client.NearHeadCacheHit("eth_getLogs").Mark(ethLogs != nil)
	if ethLogs == nil {
		return c.RpcEthClient.Logs(filter)
	}

	// This shouldn't happen, sanity check
	if ethLogs.FromBlock < fromBlock || ethLogs.ToBlock > toBlock {
		return nil, errors.New("bad event logs from near head cache")
	}
	return c.assembleLogsFromCacheAndRPC(filter, ethLogs, fromBlock, toBlock)
}

// assembleLogsFromCacheAndRPC stitches together logs from the near head cache and RPC.
func (c cachedRpcEthClient) assembleLogsFromCacheAndRPC(
	filter web3Types.FilterQuery,
	ethLogs *nearhead.EthLogs,
	fromBlock, toBlock uint64,
) ([]web3Types.Log, error) {
	result := ethLogs.Logs

	// Handle the block range before the near head cached range.
	if fromBlock < ethLogs.FromBlock {
		logs, err := c.fetchLogsFromFullNode(filter, fromBlock, ethLogs.FromBlock-1)
		if err != nil {
			return nil, errors.WithMessagef(
				err, "failed to get logs from full node for leading range [%d, %d]", fromBlock, ethLogs.FromBlock-1,
			)
		}
		result = append(logs, result...)
	}

	// Handle the block range after the cached range.
	if toBlock > ethLogs.ToBlock {
		logs, err := c.fetchLogsFromFullNode(filter, ethLogs.ToBlock+1, toBlock)
		if err != nil {
			return nil, errors.WithMessagef(
				err, "failed to get logs from full node for trailing range [%d, %d]", ethLogs.ToBlock+1, toBlock,
			)
		}
		result = append(result, logs...)
	}

	return result, nil
}

func (c cachedRpcEthClient) fetchLogsFromFullNode(filter web3Types.FilterQuery, from, to uint64) ([]web3Types.Log, error) {
	fromBlock := web3Types.BlockNumber(from)
	toBlock := web3Types.BlockNumber(to)
	filter.FromBlock = &fromBlock
	filter.ToBlock = &toBlock

	return c.RpcEthClient.Logs(filter)
}

// resolveBlockRange resolves the 'from' and 'to' block numbers from the filter.
func (c cachedRpcEthClient) resolveBlockRange(filter web3Types.FilterQuery) (uint64, uint64, error) {
	defaultBlock := web3Types.LatestBlockNumber
	fromBlock := filter.FromBlock
	if fromBlock == nil {
		fromBlock = &defaultBlock
	}

	toBlock := filter.ToBlock
	if toBlock == nil {
		toBlock = &defaultBlock
	}

	fromNumber, err := c.resolver.Resolve(*fromBlock)
	if err != nil {
		return 0, 0, err
	}

	toNumber, err := c.resolver.Resolve(*toBlock)
	if err != nil {
		return 0, 0, err
	}

	return fromNumber, toNumber, nil
}

func (c cachedRpcEthClient) getLogsByBlockHash(filter web3Types.FilterQuery) ([]web3Types.Log, error) {
	ethLogs, err := c.nearhead.GetLogsByBlockHash(*filter.BlockHash, nearhead.FilterOpt{
		Addresses: filter.Addresses,
		Topics:    filter.Topics,
	})
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get logs from near head cache")
	}

	metrics.Registry.Client.NearHeadCacheHit("eth_getLogs").Mark(ethLogs != nil)
	if ethLogs != nil {
		return ethLogs.Logs, nil
	}
	return c.RpcEthClient.Logs(filter)
}

func (c cachedRpcEthClient) BlockByHash(blockHash common.Hash, isFull bool) (*web3Types.Block, error) {
	return loadLazyIfNoError(c.LazyBlockByHash(blockHash, isFull))
}

func (c cachedRpcEthClient) LazyBlockByHash(blockHash common.Hash, isFull bool) (res cacheTypes.Lazy[*web3Types.Block], err error) {
	block := c.nearhead.GetBlock(cacheTypes.BlockHashOrNumberWithHash(blockHash), isFull)
	metrics.Registry.Client.NearHeadCacheHit("eth_getBlockByHash").Mark(block != nil)
	if block != nil {
		return cacheTypes.NewLazy(block)
	}

	lazyBlock, err := c.dataCache.GetBlock(cacheTypes.BlockHashOrNumberWithHash(blockHash), isFull)
	if err != nil {
		return lazyBlock, err
	}
	return resolveLazyWithFallback(c, "eth_getBlockByHash", lazyBlock, blockHash, isFull)
}

func (c cachedRpcEthClient) BlockByNumber(blockNumber web3Types.BlockNumber, isFull bool) (*web3Types.Block, error) {
	return loadLazyIfNoError(c.LazyBlockByNumber(blockNumber, isFull))
}

func (c cachedRpcEthClient) LazyBlockByNumber(blockNumber web3Types.BlockNumber, isFull bool) (res cacheTypes.Lazy[*web3Types.Block], err error) {
	rpcBlockNumOrHash := web3Types.BlockNumberOrHashWithNumber(blockNumber)
	cacheBlockNumOrHash, err := convert2CacheBlockHashOrNumber(c.resolver, rpcBlockNumOrHash)
	if err != nil {
		return res, err
	}

	block := c.nearhead.GetBlock(cacheBlockNumOrHash, isFull)
	metrics.Registry.Client.NearHeadCacheHit("eth_getBlockByNumber").Mark(block != nil)
	if block != nil {
		return cacheTypes.NewLazy(block)
	}

	lazyBlock, err := c.dataCache.GetBlock(cacheBlockNumOrHash, isFull)
	if err != nil {
		return lazyBlock, err
	}
	return resolveLazyWithFallback(c, "eth_getBlockByNumber", lazyBlock, blockNumber, isFull)
}

func (c cachedRpcEthClient) BlockTransactionCountByHash(blockHash common.Hash) (*big.Int, error) {
	block := c.nearhead.GetBlock(cacheTypes.BlockHashOrNumberWithHash(blockHash), false)
	metrics.Registry.Client.NearHeadCacheHit("eth_getBlockTransactionCountByHash").Mark(block != nil)
	if block != nil {
		numTxs := len(block.Transactions.Hashes())
		return big.NewInt(int64(numTxs)), nil
	}

	count, err := c.dataCache.GetBlockTransactionCount(cacheTypes.BlockHashOrNumberWithHash(blockHash))
	if err != nil {
		return nil, err
	}

	metrics.Registry.Client.DataCacheHit("eth_getBlockTransactionCountByHash").Mark(count >= 0)
	if count >= 0 {
		return big.NewInt(count), nil
	}
	return c.RpcEthClient.BlockTransactionCountByHash(blockHash)
}

func (c cachedRpcEthClient) BlockTransactionCountByNumber(blockNum web3Types.BlockNumber) (*big.Int, error) {
	rpcBlockNumOrHash := web3Types.BlockNumberOrHashWithNumber(blockNum)
	cacheBlockNumOrHash, err := convert2CacheBlockHashOrNumber(c.resolver, rpcBlockNumOrHash)
	if err != nil {
		return nil, err
	}

	block := c.nearhead.GetBlock(cacheBlockNumOrHash, false)
	metrics.Registry.Client.NearHeadCacheHit("eth_getBlockTransactionCountByNumber").Mark(block != nil)
	if block != nil {
		numTxs := len(block.Transactions.Transactions())
		return big.NewInt(int64(numTxs)), nil
	}

	count, err := c.dataCache.GetBlockTransactionCount(cacheTypes.BlockHashOrNumberWithNumber(uint64(blockNum)))
	if err != nil {
		return nil, err
	}

	metrics.Registry.Client.DataCacheHit("eth_getBlockTransactionCountByNumber").Mark(count >= 0)
	if count >= 0 {
		return big.NewInt(count), nil
	}
	return c.RpcEthClient.BlockTransactionCountByNumber(blockNum)
}

func (c cachedRpcEthClient) SendRawTransaction(rawTx []byte) (common.Hash, error) {
	txnHash, err := c.RpcEthClient.SendRawTransaction(rawTx)
	if err != nil {
		return common.Hash{}, err
	}

	lruCache.EthDefault.AddPendingTransaction(txnHash)
	return txnHash, nil
}

func (c cachedRpcEthClient) TransactionByHash(txHash common.Hash) (*web3Types.TransactionDetail, error) {
	return loadLazyIfNoError(c.LazyTransactionByHash(txHash))
}

func (c cachedRpcEthClient) LazyTransactionByHash(txHash common.Hash) (res cacheTypes.Lazy[*web3Types.TransactionDetail], err error) {
	pendingTxn, loaded, expired := lruCache.EthDefault.GetPendingTransaction(txHash)
	if !expired {
		if txn, ok := pendingTxn.Get(); ok {
			metrics.Registry.Client.ExpiryCacheHit("eth_getTransactionByHash").Mark(true)
			return cacheTypes.NewLazy(txn)
		}
	}
	metrics.Registry.Client.ExpiryCacheHit("eth_getTransactionByHash").Mark(false)

	txn := c.nearhead.GetTransactionByHash(txHash)
	metrics.Registry.Client.NearHeadCacheHit("eth_getTransactionByHash").Mark(txn != nil)
	if txn != nil {
		return cacheTypes.NewLazy(txn)
	}

	lazyTxn, err := c.dataCache.GetTransactionByHash(txHash)
	if err != nil {
		return
	}

	lazyTxn, err = resolveLazyWithFallback(c, "eth_getTransactionByHash", lazyTxn, txHash)
	if err != nil {
		return
	}

	if loaded { // Check if transaction is mined
		txn, err := lazyTxn.Load()
		if err != nil {
			return res, err
		}

		if txn != nil && txn.BlockHash != nil && txn.BlockNumber != nil {
			lruCache.EthDefault.RemovePendingTransaction(txHash)
		} else {
			pendingTxn.MarkChecked()
			pendingTxn.Set(txn)
		}
	}
	return lazyTxn, nil
}

func (c cachedRpcEthClient) TransactionByBlockHashAndIndex(blockHash common.Hash, index uint) (*web3Types.TransactionDetail, error) {
	return loadLazyIfNoError(c.LazyTransactionByBlockHashAndIndex(blockHash, index))
}

func (c cachedRpcEthClient) LazyTransactionByBlockHashAndIndex(
	blockHash common.Hash, index uint) (cacheTypes.Lazy[*web3Types.TransactionDetail], error) {
	lazyTxn, err := c.dataCache.GetTransactionByIndex(cacheTypes.BlockHashOrNumberWithHash(blockHash), uint32(index))
	if err != nil {
		return lazyTxn, err
	}
	return resolveLazyWithFallback(c, "eth_getTransactionByBlockHashAndIndex", lazyTxn, blockHash, index)
}

func (c cachedRpcEthClient) TransactionByBlockNumberAndIndex(
	blockNum web3Types.BlockNumber, index uint) (*web3Types.TransactionDetail, error) {
	return loadLazyIfNoError(c.LazyTransactionByBlockNumberAndIndex(blockNum, index))
}

func (c cachedRpcEthClient) LazyTransactionByBlockNumberAndIndex(
	blockNum web3Types.BlockNumber, index uint) (cacheTypes.Lazy[*web3Types.TransactionDetail], error) {
	lazyTxn, err := c.dataCache.GetTransactionByIndex(cacheTypes.BlockHashOrNumberWithNumber(uint64(blockNum)), uint32(index))
	if err != nil {
		return lazyTxn, err
	}
	return resolveLazyWithFallback(c, "eth_getTransactionByBlockNumberAndIndex", lazyTxn, blockNum, index)
}

func (c cachedRpcEthClient) TransactionReceipt(txHash common.Hash) (*web3Types.Receipt, error) {
	return loadLazyIfNoError(c.LazyTransactionReceipt(txHash))
}

func (c cachedRpcEthClient) LazyTransactionReceipt(txHash common.Hash) (res cacheTypes.Lazy[*web3Types.Receipt], err error) {
	pendingTxn, _, expired := lruCache.EthDefault.GetPendingTransaction(txHash)
	metrics.Registry.Client.ExpiryCacheHit("eth_getTransactionReceipt").Mark(!expired)
	if !expired {
		return res, nil
	}

	receipt := c.nearhead.GetTransactionReceipt(txHash)
	metrics.Registry.Client.NearHeadCacheHit("eth_getTransactionReceipt").Mark(receipt != nil)
	if receipt != nil {
		return cacheTypes.NewLazy(receipt)
	}

	lazyReceipt, err := c.dataCache.GetTransactionReceipt(txHash)
	if err != nil {
		return lazyReceipt, err
	}

	lazyReceipt, err = resolveLazyWithFallback(c, "eth_getTransactionReceipt", lazyReceipt, txHash)
	if err != nil {
		return lazyReceipt, err
	}

	if expired { // Time to check if transaction is mined
		if !lazyReceipt.IsEmptyOrNull() {
			lruCache.EthDefault.RemovePendingTransaction(txHash)
		} else {
			pendingTxn.MarkChecked()
		}
	}
	return lazyReceipt, nil
}

func (c cachedRpcEthClient) BlockReceipts(blockNrOrHash *web3Types.BlockNumberOrHash) (res []*web3Types.Receipt, _ error) {
	receipts, err := loadLazyIfNoError(c.LazyBlockReceipts(blockNrOrHash))
	if err != nil {
		return nil, err
	}

	for i := range receipts {
		res = append(res, &receipts[i])
	}
	return res, nil
}

func (c cachedRpcEthClient) LazyBlockReceipts(blockNrOrHash *web3Types.BlockNumberOrHash) (res cacheTypes.Lazy[[]web3Types.Receipt], err error) {
	if blockNrOrHash == nil { // defaulting to LatestBlockNumber.
		tmp := web3Types.BlockNumberOrHashWithNumber(web3Types.LatestBlockNumber)
		blockNrOrHash = &tmp
	}

	blockNumOrHash, err := convert2CacheBlockHashOrNumber(c.resolver, *blockNrOrHash)
	if err != nil {
		return res, errors.WithMessage(err, "failed to convert cache block type")
	}

	receipts := c.nearhead.GetBlockReceipts(blockNumOrHash)
	metrics.Registry.Client.NearHeadCacheHit("eth_getBlockReceipts").Mark(receipts != nil)
	if receipts != nil {
		receiptsCopy := make([]web3Types.Receipt, len(receipts))
		for i := range receipts {
			receiptsCopy[i] = *receipts[i]
		}
		return cacheTypes.NewLazy(receiptsCopy)
	}

	lazyReceipt, err := c.dataCache.GetBlockReceipts(blockNumOrHash)
	if err != nil {
		return lazyReceipt, err
	}
	return resolveLazyWithFallback(c, "eth_getBlockReceipts", lazyReceipt, blockNrOrHash)
}

type cachedRpcTraceClient struct {
	*client.RpcTraceClient
	commonClientCacheFields
}

func (c cachedRpcTraceClient) Transactions(transactionHash common.Hash) ([]web3Types.LocalizedTrace, error) {
	return loadLazyIfNoError(c.LazyTransactions(transactionHash))
}

func (c cachedRpcTraceClient) LazyTransactions(transactionHash common.Hash) (cacheTypes.Lazy[[]web3Types.LocalizedTrace], error) {
	traces := c.nearhead.GetTransactionTraces(transactionHash)
	metrics.Registry.Client.NearHeadCacheHit("trace_transaction").Mark(traces != nil)
	if traces != nil {
		return cacheTypes.NewLazy(traces)
	}

	lazyTraces, err := c.dataCache.GetTransactionTraces(transactionHash)
	if err != nil {
		return lazyTraces, err
	}
	return resolveLazyWithFallback(c, "trace_transaction", lazyTraces, transactionHash)
}

func (c *cachedRpcTraceClient) Blocks(blockNumber web3Types.BlockNumberOrHash) ([]web3Types.LocalizedTrace, error) {
	return loadLazyIfNoError(c.LazyBlocks(blockNumber))
}

func (c cachedRpcTraceClient) LazyBlocks(
	blockNrOrHash web3Types.BlockNumberOrHash) (res cacheTypes.Lazy[[]web3Types.LocalizedTrace], err error) {
	blockNumOrHash, err := convert2CacheBlockHashOrNumber(c.resolver, blockNrOrHash)
	if err != nil {
		return res, errors.WithMessage(err, "failed to convert cache block type")
	}

	traces := c.nearhead.GetBlockTraces(blockNumOrHash)
	metrics.Registry.Client.NearHeadCacheHit("trace_block").Mark(traces != nil)
	if traces != nil {
		return cacheTypes.NewLazy(traces)
	}

	lazyTraces, err := c.dataCache.GetBlockTraces(blockNumOrHash)
	if err != nil {
		return lazyTraces, err
	}
	return resolveLazyWithFallback(c, "trace_block", lazyTraces, blockNrOrHash)
}

// convert2CacheBlockHashOrNumber converts web3Types.BlockNumberOrHash to cacheTypes.BlockHashOrNumber.
func convert2CacheBlockHashOrNumber(
	resolver util.EthBlockNumberResolver, blockNrOrHash web3Types.BlockNumberOrHash) (res cacheTypes.BlockHashOrNumber, err error) {
	if hash, ok := blockNrOrHash.Hash(); ok {
		return cacheTypes.BlockHashOrNumberWithHash(hash), nil
	}

	blockNum, _ := blockNrOrHash.Number()
	if blockNum > 0 {
		return cacheTypes.BlockHashOrNumberWithNumber(uint64(blockNum)), nil
	}

	// Resolve block tag to uint64
	realBlockNum, err := resolver.Resolve(blockNum)
	if err != nil {
		tag, _ := blockNum.MarshalText()
		return res, errors.WithMessagef(err, "failed to resolve block number for tag %v", string(tag))
	}
	return cacheTypes.BlockHashOrNumberWithNumber(realBlockNum), nil
}

// resolveLazyWithFallback resolves a lazy value from data cache with fallback to RPC.
// It also records data cache hit/miss metrics and logs RPC fallback.
func resolveLazyWithFallback[T any](
	p interfaces.Provider, method string, lazy cacheTypes.Lazy[T], args ...any) (cacheTypes.Lazy[T], error) {
	cacheHit := !lazy.IsEmptyOrNull()
	defer metrics.Registry.Client.DataCacheHit(method).Mark(cacheHit)

	if cacheHit {
		return lazy, nil
	}

	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		logrus.WithFields(logrus.Fields{
			"method": method,
			"args":   args,
		}).Debug("Data cache miss, falling back to RPC")
	}
	return providers.Call[cacheTypes.Lazy[T]](p, method, args...)
}

// loadLazyIfNoError loads a lazy value if there is no error
func loadLazyIfNoError[T any](lazy cacheTypes.Lazy[T], err error) (res T, _ error) {
	if err != nil {
		return res, err
	}
	return lazy.Load()
}

// queryLRUCacheAndMetric is a helper to query the LRU cache and record metrics.
// It takes the RPC method name and a function to get the value from LRU cache.
func queryLRUCacheAndMetric[T any](method string, getter func() (T, bool, error)) (T, error) {
	val, loaded, err := getter()
	if err != nil {
		return val, err
	}

	metrics.Registry.Client.ExpiryCacheHit(method).Mark(loaded)
	return val, nil
}
