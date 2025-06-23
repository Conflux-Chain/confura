package rpc

import (
	"math/big"

	cacheRpc "github.com/Conflux-Chain/confura-data-cache/rpc"
	cacheTypes "github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/confura/util/metrics"
	lruCache "github.com/Conflux-Chain/confura/util/rpc/cache"
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

type cacheBlockTypeConverter func(*web3Types.BlockNumberOrHash) (cacheTypes.BlockHashOrNumber, error)

func newCacheBlockTypeConverter(nodeName string, eth *client.RpcEthClient) cacheBlockTypeConverter {
	return func(blockNrOrHash *web3Types.BlockNumberOrHash) (cacheTypes.BlockHashOrNumber, error) {
		return convert2CacheBlockHashOrNumber(nodeName, eth, blockNrOrHash)
	}
}

func MustNewEthDataCacheClientFromViper() cacheRpc.Interface {
	url := viper.GetString("requestControl.ethCache.dataCacheRpcUrlProto")
	if len(url) == 0 {
		return nil
	}
	client, err := cacheRpc.NewClientProto(url, ethClientCfg.RequestTimeout)
	if err != nil {
		logrus.WithField("url", url).WithError(err).Fatal("Failed to create ETH data cache client")
	}
	return client
}

type Web3goClient struct {
	*web3go.Client

	Eth   cachedRpcEthClient
	Trace cachedRpcTraceClient
	URL   string
}

func NewWeb3goClient(url string, client *web3go.Client, caches ...cacheRpc.Interface) *Web3goClient {
	cache := cacheRpc.NotFoundImpl
	if len(caches) > 0 && caches[0] != nil {
		cache = caches[0]
	}

	nodeName := Url2NodeName(url)
	return &Web3goClient{
		Client: client,
		URL:    url,
		Eth: cachedRpcEthClient{
			RpcEthClient: client.Eth,
			nodeName:     nodeName,
			dataCache:    cache,
		},
		Trace: cachedRpcTraceClient{
			RpcTraceClient: client.Trace,
			dataCache:      cache,
			converter:      newCacheBlockTypeConverter(nodeName, client.Eth),
		},
	}
}

func (w3c Web3goClient) NodeName() string {
	return Url2NodeName(w3c.URL)
}

type cachedRpcEthClient struct {
	*client.RpcEthClient
	nodeName  string
	dataCache cacheRpc.Interface
}

func (c cachedRpcEthClient) ChainId() (*uint64, error) {
	chainId, loaded, err := lruCache.EthDefault.GetChainId(c.RpcEthClient)
	if err != nil {
		return nil, err
	}

	metrics.Registry.Client.ExpiryCacheHit("eth_chainId").Mark(loaded)
	return (*uint64)(chainId), nil
}

func (c cachedRpcEthClient) GasPrice() (*big.Int, error) {
	gasPrice, loaded, err := lruCache.EthDefault.GetGasPrice(c.RpcEthClient)
	if err != nil {
		return nil, err
	}

	metrics.Registry.Client.ExpiryCacheHit("eth_gasPrice").Mark(loaded)
	return (*big.Int)(gasPrice), nil
}

func (c cachedRpcEthClient) BlockNumber() (*big.Int, error) {
	blockNum, loaded, err := lruCache.EthDefault.GetBlockNumber(c.nodeName, c.RpcEthClient)
	if err != nil {
		return nil, err
	}

	metrics.Registry.Client.ExpiryCacheHit("eth_blockNumber").Mark(loaded)
	return (*big.Int)(blockNum), nil
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
	version, loaded, err := lruCache.EthDefault.GetNetVersion(c.RpcEthClient)
	if err != nil {
		return "", err
	}

	metrics.Registry.Client.ExpiryCacheHit("net_version").Mark(loaded)
	return version, nil
}

func (c cachedRpcEthClient) ClientVersion() (string, error) {
	version, loaded, err := lruCache.EthDefault.GetClientVersion(c.RpcEthClient)
	if err != nil {
		return "", err
	}

	metrics.Registry.Client.ExpiryCacheHit("web3_clientVersion").Mark(loaded)
	return version, nil
}

func (c cachedRpcEthClient) BlockByHash(blockHash common.Hash, isFull bool) (*web3Types.Block, error) {
	return loadLazyIfNoError(c.LazyBlockByHash(blockHash, isFull))
}

func (c cachedRpcEthClient) LazyBlockByHash(blockHash common.Hash, isFull bool) (res cacheTypes.Lazy[*web3Types.Block], err error) {
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
	cacheBlockNumOrHash, err := convert2CacheBlockHashOrNumber(c.nodeName, c.RpcEthClient, &rpcBlockNumOrHash)
	if err != nil {
		return res, err
	}

	lazyBlock, err := c.dataCache.GetBlock(cacheBlockNumOrHash, isFull)
	if err != nil {
		return lazyBlock, err
	}
	return resolveLazyWithFallback(c, "eth_getBlockByNumber", lazyBlock, blockNumber, isFull)
}

func (c cachedRpcEthClient) BlockTransactionCountByHash(blockHash common.Hash) (*big.Int, error) {
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
	lazyTxn, err := c.dataCache.GetTransactionByHash(txHash)
	if err != nil {
		return
	}

	lazyTxn, err = resolveLazyWithFallback(c, "eth_getTransactionByHash", lazyTxn, txHash)
	if err != nil {
		return
	}

	pendingTxn, _, expired, err := lruCache.EthDefault.GetPendingTransaction(txHash)
	if err != nil {
		return res, err
	}
	if expired { // Check if transaction is mined
		txn, err := lazyTxn.Load()
		if err != nil {
			return res, err
		}

		pendingTxn.MarkChecked()
		if txn != nil && txn.BlockHash != nil && txn.BlockNumber != nil {
			lruCache.EthDefault.RemovePendingTransaction(txHash)
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
	pendingTxn, loaded, expired, err := lruCache.EthDefault.GetPendingTransaction(txHash)
	if err != nil {
		return res, err
	}

	metrics.Registry.Client.ExpiryCacheHit("eth_getTransactionReceipt").Mark(loaded && !expired)
	if loaded && !expired {
		return res, nil
	}

	lazyReceipt, err := c.dataCache.GetTransactionReceipt(txHash)
	if err != nil {
		return lazyReceipt, err
	}

	lazyReceipt, err = resolveLazyWithFallback(c, "eth_getTransactionReceipt", lazyReceipt, txHash)
	if err != nil {
		return lazyReceipt, err
	}

	if expired { // Check if transaction is mined
		receipt, err := lazyReceipt.Load()
		if err != nil {
			return res, err
		}

		pendingTxn.MarkChecked()
		if receipt != nil {
			lruCache.EthDefault.RemovePendingTransaction(txHash)
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
	blockNumOrHash, err := convert2CacheBlockHashOrNumber(c.nodeName, c.RpcEthClient, blockNrOrHash)
	if err != nil {
		return res, err
	}

	lazyReceipt, err := c.dataCache.GetBlockReceipts(blockNumOrHash)
	if err != nil {
		return lazyReceipt, err
	}
	return resolveLazyWithFallback(c, "eth_getBlockReceipts", lazyReceipt, blockNrOrHash)
}

type cachedRpcTraceClient struct {
	*client.RpcTraceClient
	dataCache cacheRpc.Interface
	converter cacheBlockTypeConverter
}

func (c cachedRpcTraceClient) Transactions(transactionHash common.Hash) ([]web3Types.LocalizedTrace, error) {
	return loadLazyIfNoError(c.LazyTransactions(transactionHash))
}

func (c cachedRpcTraceClient) LazyTransactions(transactionHash common.Hash) (cacheTypes.Lazy[[]web3Types.LocalizedTrace], error) {
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
	blockNumOrHash, err := c.converter(&blockNrOrHash)
	if err != nil {
		return res, err
	}

	lazyTraces, err := c.dataCache.GetBlockTraces(blockNumOrHash)
	if err != nil {
		return lazyTraces, err
	}
	return resolveLazyWithFallback(c, "trace_block", lazyTraces, blockNrOrHash)
}

func convert2CacheBlockHashOrNumber(
	nodeName string, eth *client.RpcEthClient, blockNrOrHash *web3Types.BlockNumberOrHash) (res cacheTypes.BlockHashOrNumber, err error) {
	if blockNrOrHash == nil {
		tmp := web3Types.BlockNumberOrHashWithNumber(web3Types.LatestBlockNumber)
		blockNrOrHash = &tmp
	}

	if hash, ok := blockNrOrHash.Hash(); ok {
		return cacheTypes.BlockHashOrNumberWithHash(hash), nil
	}

	blockNum, _ := blockNrOrHash.Number()
	if blockNum >= 0 {
		return cacheTypes.BlockHashOrNumberWithNumber(uint64(blockNum)), nil
	}

	// Normalize block number to uint64
	normalizedBlockNum, _, err := lruCache.EthDefault.GetBlockNumber(nodeName, eth, blockNum)
	if err != nil {
		return res, errors.WithMessage(err, "failed to normalize block number")
	}
	return cacheTypes.BlockHashOrNumberWithNumber(normalizedBlockNum.Uint64()), nil
}

// resolveLazyWithFallback resolves a lazy value from data cache with fallback to RPC
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
