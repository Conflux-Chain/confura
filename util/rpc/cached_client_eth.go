package rpc

import (
	"math/big"

	cacheRpc "github.com/Conflux-Chain/confura-data-cache/rpc"
	cacheTypes "github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/confura/util/metrics"
	lruCache "github.com/Conflux-Chain/confura/util/rpc/cache"
	"github.com/ethereum/go-ethereum/common"
	providers "github.com/openweb3/go-rpc-provider/provider_wrapper"
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/client"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func MustNewEthDataCacheClientFromViper() cacheRpc.Interface {
	url := viper.GetString("requestControl.ethCache.cacheServiceUrl")
	if len(url) == 0 {
		return nil
	}
	client, err := cacheRpc.NewClient(url, providers.Option{
		RetryCount:           ethClientCfg.Retry,
		RetryInterval:        ethClientCfg.RetryInterval,
		RequestTimeout:       ethClientCfg.RequestTimeout,
		MaxConnectionPerHost: ethClientCfg.MaxConnsPerHost,
	})
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

	return &Web3goClient{
		Client: client,
		URL:    url,
		Eth: cachedRpcEthClient{
			RpcEthClient: client.Eth,
			nodeName:     Url2NodeName(url),
			dataCache:    cache,
		},
		Trace: cachedRpcTraceClient{
			RpcTraceClient: client.Trace,
			dataCache:      cache,
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

	metrics.Registry.Client.LruCacheHit("eth_chainId").Mark(loaded)
	return (*uint64)(chainId), nil
}

func (c cachedRpcEthClient) GasPrice() (*big.Int, error) {
	gasPrice, loaded, err := lruCache.EthDefault.GetGasPrice(c.RpcEthClient)
	if err != nil {
		return nil, err
	}

	metrics.Registry.Client.LruCacheHit("eth_gasPrice").Mark(loaded)
	return (*big.Int)(gasPrice), nil
}

func (c cachedRpcEthClient) BlockNumber() (*big.Int, error) {
	blockNum, loaded, err := lruCache.EthDefault.GetBlockNumber(c.nodeName, c.RpcEthClient)
	if err != nil {
		return nil, err
	}

	metrics.Registry.Client.LruCacheHit("eth_blockNumber").Mark(loaded)
	return (*big.Int)(blockNum), nil
}

func (c cachedRpcEthClient) Call(callRequest web3Types.CallRequest, blockNum *web3Types.BlockNumberOrHash) ([]byte, error) {
	res, loaded, err := lruCache.EthDefault.Call(c.nodeName, c.RpcEthClient, callRequest, blockNum)
	if err != nil {
		return nil, err
	}

	metrics.Registry.Client.LruCacheHit("eth_call").Mark(loaded)
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

	metrics.Registry.Client.LruCacheHit("net_version").Mark(loaded)
	return version, nil
}

func (c cachedRpcEthClient) ClientVersion() (string, error) {
	version, loaded, err := lruCache.EthDefault.GetClientVersion(c.RpcEthClient)
	if err != nil {
		return "", err
	}

	metrics.Registry.Client.LruCacheHit("web3_clientVersion").Mark(loaded)
	return version, nil
}

func (c cachedRpcEthClient) BlockByHash(blockHash common.Hash, isFull bool) (*web3Types.Block, error) {
	lazyBlock, err := c.LazyBlockByHash(blockHash, isFull)
	if err != nil {
		return nil, err
	}
	return lazyBlock.Load()
}

func (c cachedRpcEthClient) LazyBlockByHash(blockHash common.Hash, isFull bool) (res cacheTypes.Lazy[*web3Types.Block], err error) {
	var dataCacheLoaded bool
	val, lruCacheLoaded, err := lruCache.EthDefault.GetBlockWithFunc(c.nodeName, func() (res interface{}, err error) {
		res, dataCacheLoaded, err = c.remoteLazyBlockByHash(blockHash, isFull)
		return res, err
	}, web3Types.BlockNumberOrHashWithHash(blockHash, true), isFull)
	if err != nil {
		return res, err
	}

	metrics.Registry.Client.LruCacheHit("eth_getBlockByHash").Mark(lruCacheLoaded)
	metrics.Registry.Client.DataCacheHit("eth_getBlockByHash").Mark(dataCacheLoaded)
	return val.(cacheTypes.Lazy[*web3Types.Block]), err
}

func (c cachedRpcEthClient) remoteLazyBlockByHash(blockHash common.Hash, isFull bool) (cacheTypes.Lazy[*web3Types.Block], bool, error) {
	lazyBlock, err := c.dataCache.GetBlock(cacheTypes.BlockHashOrNumberWithHash(blockHash), isFull)
	if err != nil {
		return lazyBlock, false, err
	}
	if !lazyBlock.IsEmptyOrNull() {
		return lazyBlock, true, nil
	}
	block, err := providers.Call[cacheTypes.Lazy[*web3Types.Block]](c, "eth_getBlockByHash", blockHash, isFull)
	return block, false, err
}

func (c cachedRpcEthClient) BlockByNumber(blockNumber web3Types.BlockNumber, isFull bool) (*web3Types.Block, error) {
	lazyBlock, err := c.LazyBlockByNumber(blockNumber, isFull)
	if err != nil {
		return nil, err
	}
	return lazyBlock.Load()
}

func (c cachedRpcEthClient) LazyBlockByNumber(blockNumber web3Types.BlockNumber, isFull bool) (res cacheTypes.Lazy[*web3Types.Block], err error) {
	var dataCacheLoaded bool
	val, lruCacheLoaded, err := lruCache.EthDefault.GetBlockWithFunc(c.nodeName, func() (interface{}, error) {
		res, dataCacheLoaded, err = c.remoteLazyBlockByNumber(blockNumber, isFull)
		return res, err
	}, web3Types.BlockNumberOrHashWithNumber(blockNumber), isFull)
	if err != nil {
		return res, err
	}

	metrics.Registry.Client.LruCacheHit("eth_getBlockByNumber").Mark(lruCacheLoaded)
	metrics.Registry.Client.DataCacheHit("eth_getBlockByNumber").Mark(dataCacheLoaded)
	return val.(cacheTypes.Lazy[*web3Types.Block]), err
}

func (c cachedRpcEthClient) remoteLazyBlockByNumber(blockNumber web3Types.BlockNumber, isFull bool) (cacheTypes.Lazy[*web3Types.Block], bool, error) {
	lazyBlock, err := c.dataCache.GetBlock(cacheTypes.BlockHashOrNumberWithNumber(uint64(blockNumber)), isFull)
	if err != nil {
		return lazyBlock, false, err
	}
	if !lazyBlock.IsEmptyOrNull() {
		return lazyBlock, true, nil
	}
	block, err := providers.Call[cacheTypes.Lazy[*web3Types.Block]](c, "eth_getBlockByNumber", blockNumber, isFull)
	return block, false, err
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

func (c cachedRpcEthClient) TransactionByHash(txHash common.Hash) (*web3Types.TransactionDetail, error) {
	txn, err := c.dataCache.GetTransactionByHash(txHash)
	if err != nil {
		return nil, err
	}

	metrics.Registry.Client.DataCacheHit("eth_getTransactionByHash").Mark(txn != nil)
	if txn != nil {
		return txn, nil
	}
	return c.RpcEthClient.TransactionByHash(txHash)
}

func (c cachedRpcEthClient) TransactionByBlockHashAndIndex(blockHash common.Hash, index uint) (*web3Types.TransactionDetail, error) {
	txn, err := c.dataCache.GetTransactionByIndex(cacheTypes.BlockHashOrNumberWithHash(blockHash), uint32(index))
	if err != nil {
		return nil, err
	}

	metrics.Registry.Client.DataCacheHit("eth_getTransactionByBlockHashAndIndex").Mark(txn != nil)
	if txn != nil {
		return txn, nil
	}
	return c.RpcEthClient.TransactionByBlockHashAndIndex(blockHash, index)
}

func (c cachedRpcEthClient) TransactionByBlockNumberAndIndex(blockNum web3Types.BlockNumber, index uint) (*web3Types.TransactionDetail, error) {
	txn, err := c.dataCache.GetTransactionByIndex(cacheTypes.BlockHashOrNumberWithNumber(uint64(blockNum)), uint32(index))
	if err != nil {
		return nil, err
	}

	metrics.Registry.Client.DataCacheHit("eth_getTransactionByBlockNumberAndIndex").Mark(txn != nil)
	if txn != nil {
		return txn, nil
	}
	return c.RpcEthClient.TransactionByBlockNumberAndIndex(blockNum, index)
}

func (c cachedRpcEthClient) TransactionReceipt(txHash common.Hash) (*web3Types.Receipt, error) {
	receipt, err := c.dataCache.GetTransactionReceipt(txHash)
	if err != nil {
		return nil, err
	}

	metrics.Registry.Client.DataCacheHit("eth_getTransactionReceipt").Mark(receipt != nil)
	if receipt != nil {
		return receipt, nil
	}
	return c.RpcEthClient.TransactionReceipt(txHash)
}

func (c cachedRpcEthClient) BlockReceipts(blockNrOrHash *web3Types.BlockNumberOrHash) (res []*web3Types.Receipt, err error) {
	lazyReceipts, err := c.LazyBlockReceipts(blockNrOrHash)
	if err != nil {
		return nil, err
	}
	receipts, err := lazyReceipts.Load()
	if err != nil {
		return nil, err
	}
	for i := range receipts {
		res = append(res, &receipts[i])
	}
	return res, nil
}

func (c cachedRpcEthClient) LazyBlockReceipts(blockNrOrHash *web3Types.BlockNumberOrHash) (cacheTypes.Lazy[[]web3Types.Receipt], error) {
	lazyReceipt, err := c.dataCache.GetBlockReceipts(convertBlockNumberOrHash(*blockNrOrHash))
	if err != nil {
		return lazyReceipt, err
	}

	metrics.Registry.Client.DataCacheHit("eth_getBlockReceipts").Mark(!lazyReceipt.IsEmptyOrNull())
	if !lazyReceipt.IsEmptyOrNull() {
		return lazyReceipt, nil
	}
	return providers.Call[cacheTypes.Lazy[[]web3Types.Receipt]](c, "eth_getBlockReceipts", blockNrOrHash)
}

type cachedRpcTraceClient struct {
	*client.RpcTraceClient
	dataCache cacheRpc.Interface
}

func (c *cachedRpcTraceClient) Transactions(transactionHash common.Hash) ([]web3Types.LocalizedTrace, error) {
	traces, err := c.dataCache.GetTransactionTraces(transactionHash)
	if err != nil {
		return traces, err
	}

	metrics.Registry.Client.DataCacheHit("trace_transaction").Mark(traces != nil)
	if traces != nil {
		return traces, nil
	}
	return c.RpcTraceClient.Transactions(transactionHash)
}

func (c *cachedRpcTraceClient) Blocks(blockNumber web3Types.BlockNumberOrHash) ([]web3Types.LocalizedTrace, error) {
	lazyTraces, err := c.LazyBlocks(blockNumber)
	if err != nil {
		return nil, err
	}
	return lazyTraces.Load()
}

func (c *cachedRpcTraceClient) LazyBlocks(blockNumber web3Types.BlockNumberOrHash) (cacheTypes.Lazy[[]web3Types.LocalizedTrace], error) {
	lazyTraces, err := c.dataCache.GetBlockTraces(convertBlockNumberOrHash(blockNumber))
	if err != nil {
		return lazyTraces, err
	}

	metrics.Registry.Client.DataCacheHit("trace_block").Mark(!lazyTraces.IsEmptyOrNull())
	if !lazyTraces.IsEmptyOrNull() {
		return lazyTraces, nil
	}
	return providers.Call[cacheTypes.Lazy[[]web3Types.LocalizedTrace]](c, "trace_block", blockNumber)
}

func convertBlockNumberOrHash(blockNrOrHash web3Types.BlockNumberOrHash) cacheTypes.BlockHashOrNumber {
	if hash, ok := blockNrOrHash.Hash(); ok {
		return cacheTypes.BlockHashOrNumberWithHash(hash)
	}
	blockNum, _ := blockNrOrHash.Number()
	return cacheTypes.BlockHashOrNumberWithNumber(uint64(blockNum))
}
