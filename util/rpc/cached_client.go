package rpc

import (
	"math/big"

	cacheRpc "github.com/Conflux-Chain/confura-data-cache/rpc"
	cacheTypes "github.com/Conflux-Chain/confura-data-cache/types"
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

func NewWeb3goClient(client *web3go.Client, dataCaches ...cacheRpc.Interface) *Web3goClient {
	var dataCache cacheRpc.Interface
	if len(dataCaches) > 0 {
		dataCache = dataCaches[0]
	}

	return &Web3goClient{
		Client: client,
		Eth: cachedRpcEthClient{
			RpcEthClient: client.Eth,
			dataCache:    dataCache,
		},
		Trace: cachedRpcTraceClient{
			RpcTraceClient: client.Trace,
			dataCache:      dataCache,
		},
	}
}

func (w3c Web3goClient) NodeName() string {
	return Url2NodeName(w3c.URL)
}

type cachedRpcEthClient struct {
	*client.RpcEthClient
	dataCache cacheRpc.Interface
}

func (c cachedRpcEthClient) BlockByHash(blockHash common.Hash, isFull bool) (*web3Types.Block, error) {
	lazyBlock, err := c.LazyBlockByHash(blockHash, isFull)
	if err != nil {
		return nil, err
	}
	return lazyBlock.Load()
}

func (c cachedRpcEthClient) LazyBlockByHash(blockHash common.Hash, isFull bool) (cacheTypes.Lazy[*web3Types.Block], error) {
	if c.dataCache != nil {
		lazyBlock, err := c.dataCache.GetBlock(cacheTypes.BlockHashOrNumberWithHash(blockHash), isFull)
		if err == nil && !lazyBlock.IsEmptyOrNull() {
			return lazyBlock, nil
		}
	}
	return providers.Call[cacheTypes.Lazy[*web3Types.Block]](c, "eth_getBlockByHash", blockHash, isFull)
}

func (c cachedRpcEthClient) BlockByNumber(blockNumber web3Types.BlockNumber, isFull bool) (*web3Types.Block, error) {
	lazyBlock, err := c.LazyBlockByNumber(blockNumber, isFull)
	if err != nil {
		return nil, err
	}
	return lazyBlock.Load()
}

func (c cachedRpcEthClient) LazyBlockByNumber(blockNumber web3Types.BlockNumber, isFull bool) (cacheTypes.Lazy[*web3Types.Block], error) {
	if c.dataCache != nil {
		lazyBlock, err := c.dataCache.GetBlock(cacheTypes.BlockHashOrNumberWithNumber(uint64(blockNumber)), isFull)
		if err == nil && !lazyBlock.IsEmptyOrNull() {
			return lazyBlock, nil
		}
	}
	return providers.Call[cacheTypes.Lazy[*web3Types.Block]](c, "eth_getBlockByNumber", blockNumber, isFull)
}

func (c cachedRpcEthClient) BlockTransactionCountByHash(blockHash common.Hash) (*big.Int, error) {
	if c.dataCache != nil {
		count, err := c.dataCache.GetBlockTransactionCount(cacheTypes.BlockHashOrNumberWithHash(blockHash))
		if err == nil && count >= 0 {
			return big.NewInt(count), nil
		}
	}
	return c.RpcEthClient.BlockTransactionCountByHash(blockHash)
}

func (c cachedRpcEthClient) BlockTransactionCountByNumber(blockNum web3Types.BlockNumber) (*big.Int, error) {
	if c.dataCache != nil {
		count, err := c.dataCache.GetBlockTransactionCount(cacheTypes.BlockHashOrNumberWithNumber(uint64(blockNum)))
		if err == nil && count >= 0 {
			return big.NewInt(count), nil
		}
	}
	return c.RpcEthClient.BlockTransactionCountByNumber(blockNum)
}

func (c cachedRpcEthClient) TransactionByHash(txHash common.Hash) (*web3Types.TransactionDetail, error) {
	if c.dataCache != nil {
		txn, err := c.dataCache.GetTransactionByHash(txHash)
		if err == nil && txn != nil {
			return txn, nil
		}
	}
	return c.RpcEthClient.TransactionByHash(txHash)
}

func (c cachedRpcEthClient) TransactionByBlockHashAndIndex(blockHash common.Hash, index uint) (*web3Types.TransactionDetail, error) {
	if c.dataCache != nil {
		txn, err := c.dataCache.GetTransactionByIndex(cacheTypes.BlockHashOrNumberWithHash(blockHash), uint32(index))
		if err == nil && txn != nil {
			return txn, nil
		}
	}
	return c.RpcEthClient.TransactionByBlockHashAndIndex(blockHash, index)
}

func (c cachedRpcEthClient) TransactionByBlockNumberAndIndex(blockNum web3Types.BlockNumber, index uint) (*web3Types.TransactionDetail, error) {
	if c.dataCache != nil {
		txn, err := c.dataCache.GetTransactionByIndex(cacheTypes.BlockHashOrNumberWithNumber(uint64(blockNum)), uint32(index))
		if err == nil && txn != nil {
			return txn, nil
		}
	}
	return c.RpcEthClient.TransactionByBlockNumberAndIndex(blockNum, index)
}

func (c cachedRpcEthClient) TransactionReceipt(txHash common.Hash) (*web3Types.Receipt, error) {
	if c.dataCache != nil {
		receipt, err := c.dataCache.GetTransactionReceipt(txHash)
		if err == nil && receipt != nil {
			return receipt, nil
		}
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
	if c.dataCache != nil {
		lazyReceipt, err := c.dataCache.GetBlockReceipts(convertBlockNumberOrHash(*blockNrOrHash))
		if err == nil && !lazyReceipt.IsEmptyOrNull() {
			return lazyReceipt, nil
		}
	}
	return providers.Call[cacheTypes.Lazy[[]web3Types.Receipt]](c, "eth_getBlockReceipts", blockNrOrHash)
}

type cachedRpcTraceClient struct {
	*client.RpcTraceClient
	dataCache cacheRpc.Interface
}

func (c *cachedRpcTraceClient) Transactions(transactionHash common.Hash) ([]web3Types.LocalizedTrace, error) {
	if c.dataCache != nil {
		traces, err := c.dataCache.GetTransactionTraces(transactionHash)
		if err == nil && traces != nil {
			return traces, nil
		}
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
	if c.dataCache != nil {
		lazyTraces, err := c.dataCache.GetBlockTraces(convertBlockNumberOrHash(blockNumber))
		if err == nil && !lazyTraces.IsEmptyOrNull() {
			return lazyTraces, nil
		}
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
