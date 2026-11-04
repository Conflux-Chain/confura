package rpc

import (
	"context"
	"math"
	"math/big"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/nearhead"
	cacheRpc "github.com/Conflux-Chain/confura-data-cache/rpc"
	cacheSync "github.com/Conflux-Chain/confura-data-cache/sync"
	cacheTypes "github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	lruCache "github.com/Conflux-Chain/confura/util/rpc/cache"
	viperutil "github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	providers "github.com/openweb3/go-rpc-provider/provider_wrapper"
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/client"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	_ util.EthBlockNumberResolver = (*ethBlockTagResolver)(nil)

	// ErrInconsistentCanonicalView means near-head and node log reads could not
	// be aligned within the local retry budget. No partial result is returned.
	ErrInconsistentCanonicalView = errors.New("inconsistent canonical view")
)

const (
	nearHeadLogRetryTimeout = time.Second
	nearHeadLogRetryBackoff = 10 * time.Millisecond
)

const (
	rpcMethodEthChainID                     = "eth_chainId"
	rpcMethodEthGasPrice                    = "eth_gasPrice"
	rpcMethodEthBlockNumber                 = "eth_blockNumber"
	rpcMethodNetVersion                     = "net_version"
	rpcMethodWeb3ClientVersion              = "web3_clientVersion"
	rpcMethodEthCall                        = "eth_call"
	rpcMethodEthBlockByHash                 = "eth_getBlockByHash"
	rpcMethodEthBlockByNumber               = "eth_getBlockByNumber"
	rpcMethodEthBlockTransactionCountByHash = "eth_getBlockTransactionCountByHash"
	rpcMethodEthBlockTransactionCountByNum  = "eth_getBlockTransactionCountByNumber"
	rpcMethodEthTransactionByHash           = "eth_getTransactionByHash"
	rpcMethodEthTransactionReceipt          = "eth_getTransactionReceipt"
	rpcMethodEthBlockReceipts               = "eth_getBlockReceipts"
	rpcMethodEthLogsByBlockHash             = "eth_getLogsByBlockHash"
	rpcMethodEthLogsByBlockRange            = "eth_getLogsByBlockRange"
	rpcMethodEthTxnByBlockHashAndIndex      = "eth_getTransactionByBlockHashAndIndex"
	rpcMethodEthTxnByBlockNumAndIndex       = "eth_getTransactionByBlockNumberAndIndex"
	rpcMethodTraceGet                       = "trace_get"
	rpcMethodTraceTransaction               = "trace_transaction"
	rpcMethodTraceBlock                     = "trace_block"
)

// EthNearHeadCache uses data-cache's readers unchanged. Cache-internal locking,
// topic filtering and returned-buffer ownership require fixes in that package.
type EthNearHeadCache struct {
	*nearhead.EthCache
	stopSync context.CancelFunc
	syncDone sync.WaitGroup
}

func newDummyEthNearHeadCache() *EthNearHeadCache {
	return &EthNearHeadCache{
		EthCache: nearhead.NewEthCache(nearhead.Config{}),
	}
}

func (c *EthNearHeadCache) Close() {
	if c.stopSync != nil {
		c.stopSync()
		c.syncDone.Wait()
	}
}

func NewEthNearHeadCacheFromViper(url string) (*EthNearHeadCache, error) {
	var conf struct {
		Enabled                  bool
		cacheSync.NearHeadConfig `mapstructure:",squash"`
	}
	if err := viperutil.UnmarshalKey("nearhead", &conf); err != nil {
		return nil, errors.WithMessage(err, "failed to unmarshal near head config")
	}

	if !conf.Enabled {
		logrus.Info("Near head cache is disabled by configuration.")
		return nil, nil
	}

	conf.Adapter.URL = url

	// Keep polling, reorg writes and adapter ownership in data-cache. Internal
	// cache fixes belong there; Confura only coordinates its full-node reads.
	ctx, cancel := context.WithCancel(context.Background())
	cache := &EthNearHeadCache{stopSync: cancel}
	ethCache, err := cacheSync.StartNearHead(ctx, &cache.syncDone, conf.NearHeadConfig)
	if err != nil {
		cancel()
		return nil, errors.WithMessage(err, "failed to sync near head data")
	}
	cache.EthCache = ethCache
	return cache, nil
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

type ethBlockTagResolver struct {
	nodeName string
	eth      *client.RpcEthClient
}

func newEthBlockTagResolver(nodeName string, eth *client.RpcEthClient) ethBlockTagResolver {
	return ethBlockTagResolver{nodeName: nodeName, eth: eth}
}

// Resolve implements the `util.EthBlockNumberResolver` interface.
// It resolves block tags (e.g., "latest", "pending") into concrete block numbers.
func (r ethBlockTagResolver) Resolve(blockNum web3Types.BlockNumber) (uint64, error) {
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
	Eth   ethCacheClient
	Trace traceCacheClient

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
	resolver := newEthBlockTagResolver(nodeName, client.Eth)
	cacheContext := cacheClientContext{
		nodeName:  nodeName,
		resolver:  resolver,
		dataCache: dataCache,
		nearhead:  nearHeadCache,
	}

	return &Web3goClient{
		URL:                    url,
		Client:                 client,
		EthBlockNumberResolver: resolver,
		Eth: ethCacheClient{
			RpcEthClient:       client.Eth,
			cacheClientContext: cacheContext,
		},
		Trace: traceCacheClient{
			RpcTraceClient:     client.Trace,
			cacheClientContext: cacheContext,
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

// cacheClientContext groups the shared cache dependencies for the ETH and trace clients.
type cacheClientContext struct {
	nodeName  string
	dataCache cacheRpc.Interface
	nearhead  *EthNearHeadCache
	resolver  util.EthBlockNumberResolver
}

func (f cacheClientContext) resolveCacheBlockRef(
	blockNrOrHash web3Types.BlockNumberOrHash,
) (cacheBlockRef, error) {
	return resolveCacheBlockRef(f.resolver, blockNrOrHash)
}

func (f cacheClientContext) resolveCacheBlockNumberRef(
	blockNumber web3Types.BlockNumber,
) (cacheBlockRef, error) {
	return f.resolveCacheBlockRef(web3Types.BlockNumberOrHashWithNumber(blockNumber))
}

type ethCacheClient struct {
	*client.RpcEthClient
	cacheClientContext
}

// -----------------------------------------------------------------------------
// Simple methods backed only by expiry cache
// -----------------------------------------------------------------------------

func (c ethCacheClient) ChainId() (*uint64, error) {
	return readWithExpiryCache(rpcMethodEthChainID, func() (*uint64, bool, error) {
		return lruCache.EthDefault.GetChainId(c.RpcEthClient)
	})
}

func (c ethCacheClient) GasPrice() (*big.Int, error) {
	return readWithExpiryCache(rpcMethodEthGasPrice, func() (*big.Int, bool, error) {
		return lruCache.EthDefault.GetGasPrice(c.RpcEthClient)
	})
}

func (c ethCacheClient) BlockNumber() (*big.Int, error) {
	return readWithExpiryCache(rpcMethodEthBlockNumber, func() (*big.Int, bool, error) {
		return lruCache.EthDefault.GetBlockNumber(c.nodeName, c.RpcEthClient)
	})
}

func (c ethCacheClient) NetVersion() (string, error) {
	return readWithExpiryCache(rpcMethodNetVersion, func() (string, bool, error) {
		return lruCache.EthDefault.GetNetVersion(c.RpcEthClient)
	})
}

func (c ethCacheClient) ClientVersion() (string, error) {
	return readWithExpiryCache(rpcMethodWeb3ClientVersion, func() (string, bool, error) {
		return lruCache.EthDefault.GetClientVersion(c.RpcEthClient)
	})
}

func (c ethCacheClient) Call(
	callRequest web3Types.CallRequest,
	blockNum *web3Types.BlockNumberOrHash,
	overrides *web3Types.StateOverride,
	blockOverrides *web3Types.BlockOverrides,
) ([]byte, error) {
	m := newRPCCacheTracker(rpcMethodEthCall)
	defer m.report()

	res, loaded, err := lruCache.EthDefault.Call(
		c.nodeName,
		c.RpcEthClient,
		callRequest,
		blockNum,
		overrides,
		blockOverrides,
	)
	if err != nil {
		return nil, m.withError(err)
	}

	m.withExpiryCacheHit(loaded).withFullNodeHit(!loaded)

	if res.RpcError != nil {
		return nil, res.RpcError
	}
	return res.Data.([]byte), nil
}

// -----------------------------------------------------------------------------
// Block queries
// -----------------------------------------------------------------------------

func (c ethCacheClient) BlockByHash(blockHash common.Hash, isFull bool) (*web3Types.Block, error) {
	lazy, err := c.LazyBlockByHash(blockHash, isFull)
	if err != nil {
		return nil, err
	}
	return lazy.Load()
}

func (c ethCacheClient) LazyBlockByHash(
	blockHash common.Hash, isFull bool,
) (cacheTypes.Lazy[*web3Types.Block], error) {
	tracker := newRPCCacheTracker(rpcMethodEthBlockByHash)
	defer tracker.report()

	blockRef := cacheTypes.BlockHashOrNumberWithHash(blockHash)
	return readThroughLazyCacheChain(
		tracker,
		func() (res cacheTypes.Lazy[*web3Types.Block], err error) {
			block := c.nearhead.GetBlock(blockRef, isFull)
			if block.Result != nil {
				res, err = cacheTypes.NewLazy(block.Result)
			}
			return res, err
		},
		func() (cacheTypes.Lazy[*web3Types.Block], error) {
			return c.dataCache.GetBlock(blockRef, isFull)
		},
		func() (cacheTypes.Lazy[*web3Types.Block], error) {
			return providers.Call[cacheTypes.Lazy[*web3Types.Block]](
				c, rpcMethodEthBlockByHash, blockHash, isFull,
			)
		},
	)
}

func (c ethCacheClient) BlockByNumber(
	blockNumber web3Types.BlockNumber, isFull bool,
) (*web3Types.Block, error) {
	lazy, err := c.LazyBlockByNumber(blockNumber, isFull)
	if err != nil {
		return nil, err
	}
	return lazy.Load()
}

func (c ethCacheClient) LazyBlockByNumber(
	blockNumber web3Types.BlockNumber,
	isFull bool,
) (res cacheTypes.Lazy[*web3Types.Block], err error) {
	blockRef, err := c.resolveCacheBlockNumberRef(blockNumber)
	if err != nil {
		return res, errors.WithMessage(err, "failed to convert to cache block type")
	}

	tracker := newRPCCacheTracker(rpcMethodEthBlockByNumber)
	defer tracker.report()

	var nearheadCoverage [2]uint64
	return readThroughLazyCacheChain(
		tracker,
		func() (res cacheTypes.Lazy[*web3Types.Block], err error) {
			nearHeadBlock := c.nearhead.GetBlock(blockRef.cacheRef, isFull)
			if nearHeadBlock.Result != nil {
				res, err = cacheTypes.NewLazy(nearHeadBlock.Result)
			}
			nearheadCoverage = nearHeadBlock.Coverage
			return res, err
		},
		func() (res cacheTypes.Lazy[*web3Types.Block], err error) {
			if blockRef.shouldReadFromDataCache(nearheadCoverage) {
				res, err = c.dataCache.GetBlock(blockRef.cacheRef, isFull)
			}
			return res, err
		},
		func() (cacheTypes.Lazy[*web3Types.Block], error) {
			return providers.Call[cacheTypes.Lazy[*web3Types.Block]](
				c, rpcMethodEthBlockByNumber, blockNumber, isFull,
			)
		},
	)
}

// -----------------------------------------------------------------------------
// Block transaction count
// -----------------------------------------------------------------------------

func (c ethCacheClient) BlockTransactionCountByHash(blockHash common.Hash) (*big.Int, error) {
	tracker := newRPCCacheTracker(rpcMethodEthBlockTransactionCountByHash)
	defer tracker.report()

	blockRef := cacheTypes.BlockHashOrNumberWithHash(blockHash)
	return readThroughCacheChain(
		tracker,
		func() (*big.Int, bool, error) {
			nearheadBlock := c.nearhead.GetBlock(blockRef, false)
			if block := nearheadBlock.Result; block != nil {
				return big.NewInt(txCountFromBlock(block)), true, nil
			}
			return nil, false, nil
		},
		func() (*big.Int, bool, error) {
			count, err := c.dataCache.GetBlockTransactionCount(blockRef)
			if err != nil {
				return nil, false, err
			}
			if count >= 0 {
				return big.NewInt(count), true, nil
			}
			return nil, false, nil
		},
		func() (*big.Int, error) {
			return c.RpcEthClient.BlockTransactionCountByHash(blockHash)
		},
	)
}

func (c ethCacheClient) BlockTransactionCountByNumber(blockNum web3Types.BlockNumber) (*big.Int, error) {
	blockRef, err := c.resolveCacheBlockNumberRef(blockNum)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to convert to cache block type")
	}

	tracker := newRPCCacheTracker(rpcMethodEthBlockTransactionCountByNum)
	defer tracker.report()

	var nearheadCoverage [2]uint64
	return readThroughCacheChain(
		tracker,
		func() (*big.Int, bool, error) {
			nearHeadBlock := c.nearhead.GetBlock(blockRef.cacheRef, false)
			if block := nearHeadBlock.Result; block != nil {
				return big.NewInt(txCountFromBlock(block)), true, nil
			}
			nearheadCoverage = nearHeadBlock.Coverage
			return nil, false, nil
		},
		func() (*big.Int, bool, error) {
			if !blockRef.shouldReadFromDataCache(nearheadCoverage) {
				return nil, false, nil
			}

			count, err := c.dataCache.GetBlockTransactionCount(blockRef.cacheRef)
			if err != nil {
				return nil, false, err
			}
			if count >= 0 {
				return big.NewInt(count), true, nil
			}
			return nil, false, nil
		},
		func() (*big.Int, error) {
			return c.RpcEthClient.BlockTransactionCountByNumber(blockNum)
		},
	)
}

// -----------------------------------------------------------------------------
// Transaction
// -----------------------------------------------------------------------------

func (c ethCacheClient) TransactionByHash(txHash common.Hash) (*web3Types.TransactionDetail, error) {
	lazy, err := c.LazyTransactionByHash(txHash)
	if err != nil {
		return nil, err
	}
	return lazy.Load()
}

func (c ethCacheClient) LazyTransactionByHash(
	txHash common.Hash,
) (cacheTypes.Lazy[*web3Types.TransactionDetail], error) {
	tracker := newRPCCacheTracker(rpcMethodEthTransactionByHash)
	defer tracker.report()

	var fullNodeHit, expiryHit bool
	pendingState := newPendingTransactionState(txHash)

	res, err := readThroughLazyCacheChain(
		tracker,
		func() (res cacheTypes.Lazy[*web3Types.TransactionDetail], err error) {
			if txn := c.nearhead.GetTransactionByHash(txHash); txn != nil {
				res, err = cacheTypes.NewLazy(txn)
			}
			return res, err
		},
		func() (cacheTypes.Lazy[*web3Types.TransactionDetail], error) {
			return c.dataCache.GetTransactionByHash(txHash)
		},
		func() (lazyTxn cacheTypes.Lazy[*web3Types.TransactionDetail], err error) {
			// Check the pending transaction cache before hitting the full node.
			if txn, hit := pendingState.getPendingTransaction(); hit {
				expiryHit = true
				return cacheTypes.NewLazy(txn)
			}

			lazyTxn, err = providers.Call[cacheTypes.Lazy[*web3Types.TransactionDetail]](
				c, rpcMethodEthTransactionByHash, txHash,
			)
			if err != nil {
				return lazyTxn, err
			}

			// Refresh expired pending state after a full-node lookup.
			if pendingState.loaded {
				txn, err := lazyTxn.Load()
				if err != nil {
					return lazyTxn, errors.WithMessage(err, "failed to load lazy transaction")
				}

				pendingState.refresh(txn)
			}

			fullNodeHit = true
			return lazyTxn, nil
		},
	)

	tracker.withExpiryCacheHit(expiryHit).withFullNodeHit(fullNodeHit)
	return res, err
}

// -----------------------------------------------------------------------------
// Transaction receipt
// -----------------------------------------------------------------------------

func (c ethCacheClient) TransactionReceipt(txHash common.Hash) (*web3Types.Receipt, error) {
	lazy, err := c.LazyTransactionReceipt(txHash)
	if err != nil {
		return nil, err
	}
	return lazy.Load()
}

func (c ethCacheClient) LazyTransactionReceipt(
	txHash common.Hash,
) (cacheTypes.Lazy[*web3Types.Receipt], error) {
	tracker := newRPCCacheTracker(rpcMethodEthTransactionReceipt)
	defer tracker.report()

	var fullNodeHit, expiryHit bool
	pendingState := newPendingTransactionState(txHash)

	res, err := readThroughLazyCacheChain(
		tracker,
		func() (res cacheTypes.Lazy[*web3Types.Receipt], err error) {
			if receipt := c.nearhead.GetTransactionReceipt(txHash); receipt != nil {
				res, err = cacheTypes.NewLazy(receipt)
			}
			return res, err
		},
		func() (cacheTypes.Lazy[*web3Types.Receipt], error) {
			return c.dataCache.GetTransactionReceipt(txHash)
		},
		func() (lazyReceipt cacheTypes.Lazy[*web3Types.Receipt], err error) {
			// If the transaction is still pending and not expired, the receipt is expected to be empty.
			if pendingState.hasPendingTransaction() {
				expiryHit = true
				return lazyReceipt, nil
			}

			lazyReceipt, err = providers.Call[cacheTypes.Lazy[*web3Types.Receipt]](c, rpcMethodEthTransactionReceipt, txHash)
			if err != nil {
				return lazyReceipt, err
			}

			// Refresh expired pending state after a full-node lookup.
			if pendingState.loaded && !lazyReceipt.IsEmptyOrNull() {
				pendingState.remove()
			}

			fullNodeHit = true
			return lazyReceipt, nil
		},
	)

	tracker.withExpiryCacheHit(expiryHit).withFullNodeHit(fullNodeHit)
	return res, err
}

// -----------------------------------------------------------------------------
// Block receipts
// -----------------------------------------------------------------------------

func (c ethCacheClient) BlockReceipts(
	blockNrOrHash *web3Types.BlockNumberOrHash,
) (res []*web3Types.Receipt, _ error) {
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

func (c ethCacheClient) LazyBlockReceipts(
	blockNrOrHash *web3Types.BlockNumberOrHash,
) (res cacheTypes.Lazy[[]web3Types.Receipt], err error) {
	blockRef, rpcBlockRef, err := c.resolveCacheBlockRefOrLatest(blockNrOrHash)
	if err != nil {
		return res, errors.WithMessage(err, "failed to convert cache block type")
	}

	tracker := newRPCCacheTracker(rpcMethodEthBlockReceipts)
	defer tracker.report()

	var nearheadCoverage [2]uint64
	return readThroughLazyCacheChain(
		tracker,
		func() (res cacheTypes.Lazy[[]web3Types.Receipt], err error) {
			nearHeadReceipts := c.nearhead.GetBlockReceipts(blockRef.cacheRef)
			if nearHeadReceipts.Result != nil {
				res, err = cacheTypes.NewLazy(cloneReceipts(nearHeadReceipts.Result))
			}
			nearheadCoverage = nearHeadReceipts.Coverage
			return res, err
		},
		func() (res cacheTypes.Lazy[[]web3Types.Receipt], err error) {
			if blockRef.shouldReadFromDataCache(nearheadCoverage) {
				res, err = c.dataCache.GetBlockReceipts(blockRef.cacheRef)
			}
			return res, err
		},
		func() (cacheTypes.Lazy[[]web3Types.Receipt], error) {
			return providers.Call[cacheTypes.Lazy[[]web3Types.Receipt]](c, rpcMethodEthBlockReceipts, rpcBlockRef)
		},
	)
}

// =============================================================================
// Logs
// =============================================================================

func (c ethCacheClient) Logs(filter web3Types.FilterQuery) ([]web3Types.Log, error) {
	if filter.BlockHash != nil {
		return c.logsByBlockHash(filter)
	}
	return c.logsByBlockRange(filter)
}

func (c ethCacheClient) logsByBlockHash(filter web3Types.FilterQuery) ([]web3Types.Log, error) {
	tracker := newRPCCacheTracker(rpcMethodEthLogsByBlockHash).withNearHeadHit(false).withFullNodeHit(false)
	defer tracker.report()

	ethLogs, err := c.nearhead.GetLogsByBlockHash(
		*filter.BlockHash,
		nearhead.FilterOpt{
			Addresses: filter.Addresses,
			Topics:    filter.Topics,
		},
	)
	if err != nil {
		err = errors.WithMessage(err, "failed to get logs from near head cache")
		return nil, tracker.withError(err)
	}

	if ethLogs != nil {
		tracker.withNearHeadHit(true)
		return ethLogs.Logs, nil
	}

	logs, err := c.RpcEthClient.Logs(filter)
	if err != nil {
		return nil, tracker.withError(err)
	}

	tracker.withFullNodeHit(true)
	return logs, nil
}

func (c ethCacheClient) logsByBlockRange(filter web3Types.FilterQuery) ([]web3Types.Log, error) {
	tracker := newRPCCacheTracker(rpcMethodEthLogsByBlockRange).withNearHeadHit(false).withFullNodeHit(false)
	defer tracker.report()

	fromBlock, toBlock, err := c.resolveBlockRange(filter)
	if err != nil {
		return nil, tracker.withError(errors.WithMessage(err, "failed to resolve block range"))
	}
	// Resolve moving tags once. Every attempt, including a cache miss after a
	// conflict, must query the same heights without changing the caller's filter.
	from, to := web3Types.BlockNumber(fromBlock), web3Types.BlockNumber(toBlock)
	filter.FromBlock, filter.ToBlock = &from, &to

	// The local window controls admission to another attempt, not RPC duration.
	// Keep the configured client context/timeouts. An admitted attempt may finish
	// after this window and still return a coherent result or its original error.
	var deadline time.Time
	started := time.Now()
	for attempts := 1; ; attempts++ {
		logs, err := c.logsByBlockRangeOnce(filter, fromBlock, toBlock, tracker)
		conflict, ok := err.(*nearHeadLogConflict)
		if !ok {
			if err == nil && attempts > 1 {
				logrus.WithField("attempts", attempts).Debug("Near-head log consistency recovered")
			}
			return logs, tracker.withError(err)
		}
		if deadline.IsZero() {
			deadline = time.Now().Add(nearHeadLogRetryTimeout)
		}
		logger := logrus.WithFields(logrus.Fields{
			"fromBlock": fromBlock,
			"toBlock":   toBlock,
			"boundary":  conflict.boundary,
			"reason":    conflict.reason,
			"attempts":  attempts,
			"elapsed":   time.Since(started),
		})
		if remaining := time.Until(deadline); remaining > 0 {
			logger.Debug("Retrying near-head logs after canonical view conflict")
			time.Sleep(min(nearHeadLogRetryBackoff, remaining))
		}
		if !time.Now().Before(deadline) {
			logger.WithField("elapsed", time.Since(started)).Info("Near-head log consistency retry window exhausted")
			return nil, tracker.withError(ErrInconsistentCanonicalView)
		}
	}
}

// nearHeadLogConflict carries fixed diagnostic reasons only. It is consumed by
// the retry loop; clients receive only ErrInconsistentCanonicalView on exhaustion.
type nearHeadLogConflict struct {
	reason   string
	boundary uint64
}

func (e *nearHeadLogConflict) Error() string { return e.reason }

// nearHeadLogReader keeps the metadata/log read sequence explicit and testable.
// Cache-internal snapshot and ownership guarantees remain data-cache's concern.
type nearHeadLogReader interface {
	GetBlock(cacheTypes.BlockHashOrNumber, bool) nearhead.EthCacheResultWithCoverage[*web3Types.Block]
	GetLogsByBlockRange(uint64, uint64, nearhead.FilterOpt) (*nearhead.EthLogs, error)
}

// readNearHeadLogs scans logs at most once. A lightweight metadata lookup
// captures the cached end first, so a partial hit needs no second log scan.
func readNearHeadLogs(
	cache nearHeadLogReader,
	fromBlock, toBlock uint64,
	opt nearhead.FilterOpt,
) (*nearhead.EthLogs, common.Hash, error) {
	if fromBlock > toBlock {
		return nil, common.Hash{}, errors.New("failed to get logs from near head cache: invalid block range params")
	}
	// In the pinned cache, true returns the cached block directly; false copies
	// it and builds a transaction-hash list. We only read metadata, never mutate.
	probe := cache.GetBlock(cacheTypes.BlockHashOrNumberWithNumber(toBlock), true)
	start, end := probe.Coverage[0], probe.Coverage[1] // end is exclusive
	if start >= end || toBlock < start || fromBlock >= end {
		return nil, common.Hash{}, nil
	}
	end = min(toBlock, end-1)
	ref := cacheTypes.BlockHashOrNumberWithNumber(end)
	before := probe.Result
	if end != toBlock {
		before = cache.GetBlock(ref, true).Result
	}
	// Copy the hash before the scan; the cache returns a shared block pointer.
	var hash common.Hash
	if before != nil {
		hash = before.Hash
	}
	snapshot, err := cache.GetLogsByBlockRange(fromBlock, toBlock, opt)
	if err != nil {
		return nil, common.Hash{}, errors.WithMessage(err, "failed to get logs from near head cache")
	}
	if snapshot == nil {
		return nil, common.Hash{}, &nearHeadLogConflict{"near-head coverage disappeared", end}
	}
	if snapshot.FromBlock < fromBlock || snapshot.ToBlock > toBlock || snapshot.FromBlock > snapshot.ToBlock {
		return nil, common.Hash{}, errors.New("bad event logs from near head cache: unexpected block range")
	}
	if snapshot.FromBlock == fromBlock && snapshot.ToBlock == toBlock {
		// A complete result has one source and needs no boundary validation.
		return snapshot, common.Hash{}, nil
	}
	if before == nil || hash == (common.Hash{}) {
		return nil, common.Hash{}, &nearHeadLogConflict{"near-head boundary unavailable", end}
	}
	after := cache.GetBlock(ref, true).Result
	if snapshot.ToBlock != end || after == nil || after.Hash != hash {
		return nil, common.Hash{}, &nearHeadLogConflict{"near-head cache changed while reading logs", end}
	}
	// Start eviction can extend the node prefix. The actual returned range is
	// authoritative as long as its end still matches the captured boundary.
	return snapshot, hash, nil
}

// logsByBlockRangeOnce owns all data for one attempt. A conflict discards both
// node segments and the cache result; no logs carry over to the next attempt.
func (c ethCacheClient) logsByBlockRangeOnce(
	filter web3Types.FilterQuery,
	fromBlock, toBlock uint64,
	tracker *rpcCacheTracker,
) ([]web3Types.Log, error) {
	opt := nearhead.FilterOpt{Addresses: filter.Addresses, Topics: filter.Topics}
	snapshot, hash, err := readNearHeadLogs(c.nearhead, fromBlock, toBlock, opt)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		tracker.withFullNodeHit(true)
		return c.fetchLogsFromFullNode(filter, fromBlock, toBlock)
	}
	tracker.withNearHeadHit(true)
	if snapshot.FromBlock == fromBlock && snapshot.ToBlock == toBlock {
		return snapshot.Logs, nil
	}
	end := snapshot.ToBlock

	// A block hash commits to its ancestry. For an internally coherent cache
	// and node view, matching the cached end aligns both the older node prefix
	// and the newer node suffix. Check that same height around BOTH node reads;
	// no start/parent-hash or query-upper-bound lookup is needed here. This is
	// optimistic alignment, not a node snapshot or protection against ABA or
	// independently inconsistent responses within the node's Logs RPC.
	if err := c.checkNearHeadLogBoundary(end, hash); err != nil {
		return nil, err
	}
	logs := make([]web3Types.Log, 0, len(snapshot.Logs))
	if fromBlock < snapshot.FromBlock {
		tracker.withFullNodeHit(true)
		prefix, err := c.fetchLogsFromFullNode(filter, fromBlock, snapshot.FromBlock-1)
		if err != nil {
			return nil, err
		}
		logs = append(logs, prefix...)
	}
	logs = append(logs, snapshot.Logs...)
	if toBlock > end {
		tracker.withFullNodeHit(true)
		suffix, err := c.fetchLogsFromFullNode(filter, end+1, toBlock)
		if err != nil {
			return nil, err
		}
		logs = append(logs, suffix...)
	}
	if err := c.checkNearHeadLogBoundary(end, hash); err != nil {
		return nil, err
	}
	return logs, nil
}

func (c ethCacheClient) checkNearHeadLogBoundary(number uint64, hash common.Hash) error {
	block, err := c.RpcEthClient.BlockByNumber(web3Types.BlockNumber(number), false)
	if err != nil {
		return err
	}
	if block == nil || block.Number == nil || !block.Number.IsUint64() ||
		block.Number.Uint64() != number || block.Hash != hash {
		return &nearHeadLogConflict{"node boundary differs from near-head cache", number}
	}
	return nil
}

func (c ethCacheClient) fetchLogsFromFullNode(
	filter web3Types.FilterQuery,
	from, to uint64,
) ([]web3Types.Log, error) {
	fromBlock := web3Types.BlockNumber(from)
	toBlock := web3Types.BlockNumber(to)
	filter.FromBlock = &fromBlock
	filter.ToBlock = &toBlock

	return c.RpcEthClient.Logs(filter)
}

// resolveBlockRange resolves the 'from' and 'to' block numbers from the filter.
func (c ethCacheClient) resolveBlockRange(filter web3Types.FilterQuery) (uint64, uint64, error) {
	fromBlock := latestBlockNumberOrDefault(filter.FromBlock)
	toBlock := latestBlockNumberOrDefault(filter.ToBlock)

	fromNumber, err := c.resolver.Resolve(fromBlock)
	if err != nil {
		return 0, 0, err
	}

	toNumber, err := c.resolver.Resolve(toBlock)
	if err != nil {
		return 0, 0, err
	}

	return fromNumber, toNumber, nil
}

// =============================================================================
// Transaction sending and indexing
// =============================================================================

func (c ethCacheClient) SendRawTransaction(rawTx []byte) (common.Hash, error) {
	txnHash, err := c.RpcEthClient.SendRawTransaction(rawTx)
	if err != nil {
		return common.Hash{}, err
	}

	lruCache.EthDefault.AddPendingTransaction(txnHash)
	return txnHash, nil
}

func (c ethCacheClient) TransactionByBlockHashAndIndex(
	blockHash common.Hash,
	index uint,
) (*web3Types.TransactionDetail, error) {
	lazy, err := c.LazyTransactionByBlockHashAndIndex(blockHash, index)
	if err != nil {
		return nil, err
	}
	return lazy.Load()
}

func (c ethCacheClient) LazyTransactionByBlockHashAndIndex(
	blockHash common.Hash,
	index uint,
) (cacheTypes.Lazy[*web3Types.TransactionDetail], error) {
	tracker := newRPCCacheTracker(rpcMethodEthTxnByBlockHashAndIndex)
	defer tracker.report()

	cacheBlockRef := cacheTypes.BlockHashOrNumberWithHash(blockHash)
	return readThroughLazyCacheChain(
		tracker,
		func() (res cacheTypes.Lazy[*web3Types.TransactionDetail], err error) {
			nearheadBlock := c.nearhead.GetBlock(cacheBlockRef, true)
			if block := nearheadBlock.Result; block != nil {
				if txns := block.Transactions.Transactions(); index < uint(len(txns)) {
					res, err = cacheTypes.NewLazy(&txns[index])
				}
			}
			return res, err
		},
		func() (cacheTypes.Lazy[*web3Types.TransactionDetail], error) {
			if uint64(index) > math.MaxUint32 {
				return cacheTypes.Lazy[*web3Types.TransactionDetail]{}, nil
			}
			return c.dataCache.GetTransactionByIndex(cacheBlockRef, uint32(index))
		},
		func() (cacheTypes.Lazy[*web3Types.TransactionDetail], error) {
			return providers.Call[cacheTypes.Lazy[*web3Types.TransactionDetail]](
				c, rpcMethodEthTxnByBlockHashAndIndex, blockHash, hexutil.Uint(index),
			)
		},
	)
}

func (c ethCacheClient) TransactionByBlockNumberAndIndex(
	blockNum web3Types.BlockNumber,
	index uint,
) (*web3Types.TransactionDetail, error) {
	lazy, err := c.LazyTransactionByBlockNumberAndIndex(blockNum, index)
	if err != nil {
		return nil, err
	}
	return lazy.Load()
}

func (c ethCacheClient) LazyTransactionByBlockNumberAndIndex(
	blockNum web3Types.BlockNumber,
	index uint,
) (res cacheTypes.Lazy[*web3Types.TransactionDetail], err error) {
	blockRef, err := c.resolveCacheBlockNumberRef(blockNum)
	if err != nil {
		return res, errors.WithMessage(err, "failed to convert to cache block type")
	}

	tracker := newRPCCacheTracker(rpcMethodEthTxnByBlockNumAndIndex)
	defer tracker.report()

	var nearheadCoverage [2]uint64
	return readThroughLazyCacheChain(
		tracker,
		func() (res cacheTypes.Lazy[*web3Types.TransactionDetail], err error) {
			nearHeadBlock := c.nearhead.GetBlock(blockRef.cacheRef, true)
			if block := nearHeadBlock.Result; block != nil {
				if txns := block.Transactions.Transactions(); index < uint(len(txns)) {
					res, err = cacheTypes.NewLazy(&txns[index])
				}
			}
			nearheadCoverage = nearHeadBlock.Coverage
			return res, err
		},
		func() (res cacheTypes.Lazy[*web3Types.TransactionDetail], err error) {
			if uint64(index) <= math.MaxUint32 && blockRef.shouldReadFromDataCache(nearheadCoverage) {
				res, err = c.dataCache.GetTransactionByIndex(blockRef.cacheRef, uint32(index))
			}
			return res, err
		},
		func() (cacheTypes.Lazy[*web3Types.TransactionDetail], error) {
			return providers.Call[cacheTypes.Lazy[*web3Types.TransactionDetail]](
				c, rpcMethodEthTxnByBlockNumAndIndex, blockNum, hexutil.Uint(index),
			)
		},
	)
}

// =============================================================================
// Trace client
// =============================================================================

type traceCacheClient struct {
	*client.RpcTraceClient
	cacheClientContext
}

func (c *traceCacheClient) Trace(transactionHash common.Hash, indexes []uint) (*web3Types.LocalizedTrace, error) {
	lazy, err := c.LazyTrace(transactionHash, indexes)
	if err != nil {
		return nil, err
	}
	return lazy.Load()
}

func (c traceCacheClient) LazyTrace(
	transactionHash common.Hash,
	indexes []uint,
) (res cacheTypes.Lazy[*web3Types.LocalizedTrace], _ error) {
	if len(indexes) == 0 {
		return res, nil
	}
	// Conflux eSpace uses the first index as an offset in transaction traces.
	index := indexes[0]
	tracker := newRPCCacheTracker(rpcMethodTraceGet)
	defer tracker.report()

	return readThroughLazyCacheChain(
		tracker,
		func() (res cacheTypes.Lazy[*web3Types.LocalizedTrace], err error) {
			// The pinned cache converts index to int for its bounds check.
			if index > uint(math.MaxInt) {
				return res, nil
			}
			if trace := c.nearhead.GetTrace(transactionHash, index); trace != nil {
				res, err = cacheTypes.NewLazy(trace)
			}
			return res, err
		},
		func() (cacheTypes.Lazy[*web3Types.LocalizedTrace], error) {
			// The pinned gRPC client narrows index to uint32 on the wire.
			if uint64(index) > math.MaxUint32 {
				return cacheTypes.Lazy[*web3Types.LocalizedTrace]{}, nil
			}
			return c.dataCache.GetTrace(transactionHash, index)
		},
		func() (cacheTypes.Lazy[*web3Types.LocalizedTrace], error) {
			hexIndexes := make([]hexutil.Uint, len(indexes))
			for i, idx := range indexes {
				hexIndexes[i] = hexutil.Uint(idx)
			}
			return providers.Call[cacheTypes.Lazy[*web3Types.LocalizedTrace]](
				c, rpcMethodTraceGet, transactionHash, hexIndexes,
			)
		},
	)
}

func (c traceCacheClient) Transactions(transactionHash common.Hash) ([]web3Types.LocalizedTrace, error) {
	lazy, err := c.LazyTransactions(transactionHash)
	if err != nil {
		return nil, err
	}
	return lazy.Load()
}

func (c traceCacheClient) LazyTransactions(
	transactionHash common.Hash,
) (cacheTypes.Lazy[[]web3Types.LocalizedTrace], error) {
	tracker := newRPCCacheTracker(rpcMethodTraceTransaction)
	defer tracker.report()

	return readThroughLazyCacheChain(
		tracker,
		func() (res cacheTypes.Lazy[[]web3Types.LocalizedTrace], err error) {
			if traces := c.nearhead.GetTransactionTraces(transactionHash); traces != nil {
				res, err = cacheTypes.NewLazy(traces)
			}
			return res, err
		},
		func() (cacheTypes.Lazy[[]web3Types.LocalizedTrace], error) {
			return c.dataCache.GetTransactionTraces(transactionHash)
		},
		func() (cacheTypes.Lazy[[]web3Types.LocalizedTrace], error) {
			return providers.Call[cacheTypes.Lazy[[]web3Types.LocalizedTrace]](
				c, rpcMethodTraceTransaction, transactionHash,
			)
		},
	)
}

func (c *traceCacheClient) Blocks(blockNumber web3Types.BlockNumberOrHash) ([]web3Types.LocalizedTrace, error) {
	lazy, err := c.LazyBlocks(blockNumber)
	if err != nil {
		return nil, err
	}
	return lazy.Load()
}

func (c traceCacheClient) LazyBlocks(
	blockNrOrHash web3Types.BlockNumberOrHash,
) (res cacheTypes.Lazy[[]web3Types.LocalizedTrace], err error) {
	blockRef, err := c.resolveCacheBlockRef(blockNrOrHash)
	if err != nil {
		return res, errors.WithMessage(err, "failed to convert cache block type")
	}

	tracker := newRPCCacheTracker(rpcMethodTraceBlock)
	defer tracker.report()

	var nearheadCoverage [2]uint64
	return readThroughLazyCacheChain(
		tracker,
		func() (res cacheTypes.Lazy[[]web3Types.LocalizedTrace], err error) {
			nearheadBlock := c.nearhead.GetBlockTraces(blockRef.cacheRef)
			if traces := nearheadBlock.Result; traces != nil {
				res, err = cacheTypes.NewLazy(traces)
			}
			nearheadCoverage = nearheadBlock.Coverage
			return res, err
		},
		func() (res cacheTypes.Lazy[[]web3Types.LocalizedTrace], err error) {
			if blockRef.shouldReadFromDataCache(nearheadCoverage) {
				res, err = c.dataCache.GetBlockTraces(blockRef.cacheRef)
			}
			return res, err
		},
		func() (cacheTypes.Lazy[[]web3Types.LocalizedTrace], error) {
			return providers.Call[cacheTypes.Lazy[[]web3Types.LocalizedTrace]](
				c, rpcMethodTraceBlock, blockNrOrHash,
			)
		},
	)
}

// toCacheBlockRef converts web3Types.BlockNumberOrHash to cacheTypes.BlockHashOrNumber.
func toCacheBlockRef(
	resolver util.EthBlockNumberResolver,
	blockNrOrHash web3Types.BlockNumberOrHash,
) (res cacheTypes.BlockHashOrNumber, err error) {
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

type cacheBlockRef struct {
	cacheRef    cacheTypes.BlockHashOrNumber
	isBlockNum  bool
	resolvedNum uint64
}

func resolveCacheBlockRef(
	resolver util.EthBlockNumberResolver,
	blockNrOrHash web3Types.BlockNumberOrHash,
) (ref cacheBlockRef, err error) {
	cacheRef, err := toCacheBlockRef(resolver, blockNrOrHash)
	if err != nil {
		return ref, err
	}

	_, ok, blockNum := cacheRef.HashOrNumber()
	return cacheBlockRef{
		cacheRef:    cacheRef,
		isBlockNum:  !ok,
		resolvedNum: blockNum,
	}, nil
}

func (r cacheBlockRef) shouldReadFromDataCache(coverage [2]uint64) bool {
	return r.isBlockNum && shouldUseHistoricalDataCache(r.resolvedNum, coverage)
}

func (c ethCacheClient) resolveCacheBlockRefOrLatest(
	blockNrOrHash *web3Types.BlockNumberOrHash,
) (cacheBlockRef, web3Types.BlockNumberOrHash, error) {
	ref := latestBlockRefOrDefault(blockNrOrHash)
	resolved, err := c.resolveCacheBlockRef(ref)
	return resolved, ref, err
}

type pendingTransactionState struct {
	hash    common.Hash
	entry   pendingTransactionEntry
	loaded  bool
	expired bool
}

type pendingTransactionEntry interface {
	Get() (*web3Types.TransactionDetail, bool)
	Set(*web3Types.TransactionDetail)
}

func newPendingTransactionState(hash common.Hash) pendingTransactionState {
	txn, loaded, expired := lruCache.EthDefault.GetPendingTransaction(hash)
	return pendingTransactionState{
		hash:    hash,
		entry:   txn,
		loaded:  loaded,
		expired: expired,
	}
}

func (s pendingTransactionState) getPendingTransaction() (*web3Types.TransactionDetail, bool) {
	if !s.loaded || s.expired || s.entry == nil {
		return nil, false
	}
	return s.entry.Get()
}

func (s pendingTransactionState) hasPendingTransaction() bool {
	_, ok := s.getPendingTransaction()
	return ok
}

func (s pendingTransactionState) refresh(txn *web3Types.TransactionDetail) {
	if txn == nil {
		return
	}
	if txn.BlockHash != nil {
		s.remove()
		return
	}
	if s.entry != nil {
		s.entry.Set(txn)
	}
}

func (s pendingTransactionState) remove() {
	lruCache.EthDefault.RemovePendingTransaction(s.hash)
}

// =============================================================================
// Metrics tracking for cache hits
// =============================================================================

// rpcCacheTracker collects cache-hit states during a single RPC flow.
// Metrics are reported only when the call finishes without error.
type rpcCacheTracker struct {
	method string

	nearHeadHit    *bool
	dataCacheHit   *bool
	fullNodeHit    *bool
	expiryCacheHit *bool

	err error
}

func newRPCCacheTracker(method string) *rpcCacheTracker {
	return &rpcCacheTracker{method: method}
}

func (m *rpcCacheTracker) withNearHeadHit(hit bool) *rpcCacheTracker {
	m.nearHeadHit = &hit
	return m
}

func (m *rpcCacheTracker) withDataCacheHit(hit bool) *rpcCacheTracker {
	m.dataCacheHit = &hit
	return m
}

func (m *rpcCacheTracker) withFullNodeHit(hit bool) *rpcCacheTracker {
	m.fullNodeHit = &hit
	return m
}

func (m *rpcCacheTracker) withExpiryCacheHit(hit bool) *rpcCacheTracker {
	m.expiryCacheHit = &hit
	return m
}

func (m *rpcCacheTracker) withError(err error) error {
	m.err = err
	return err
}

func (m *rpcCacheTracker) report() {
	if m.err != nil {
		return
	}

	if m.expiryCacheHit != nil {
		metrics.Registry.Client.ExpiryCacheHit(m.method).Mark(*m.expiryCacheHit)
	}
	if m.nearHeadHit != nil {
		metrics.Registry.Client.NearHeadCacheHit(m.method).Mark(*m.nearHeadHit)
	}
	if m.dataCacheHit != nil {
		metrics.Registry.Client.DataCacheHit(m.method).Mark(*m.dataCacheHit)
	}
	if m.fullNodeHit != nil {
		metrics.Registry.Client.FullNodeHit(m.method).Mark(*m.fullNodeHit)
	}
}

// =============================================================================
// Generic helpers
// =============================================================================

// shouldUseHistoricalDataCache reports whether a given block number is old
// enough to fall back to the historical data cache according to near-head coverage.
func shouldUseHistoricalDataCache(blockNum uint64, coverage [2]uint64) bool {
	return blockNum > 0 && blockNum < coverage[0]
}

func latestBlockNumberOrDefault(blockNum *web3Types.BlockNumber) web3Types.BlockNumber {
	if blockNum != nil {
		return *blockNum
	}
	return web3Types.LatestBlockNumber
}

func latestBlockRefOrDefault(blockNrOrHash *web3Types.BlockNumberOrHash) web3Types.BlockNumberOrHash {
	if blockNrOrHash != nil {
		return *blockNrOrHash
	}
	return web3Types.BlockNumberOrHashWithNumber(web3Types.LatestBlockNumber)
}

// txCountFromBlock extracts transaction count from either a hash-only block
// or a full-transaction block payload.
func txCountFromBlock(block *web3Types.Block) int64 {
	if block == nil {
		return 0
	}
	if txs := block.Transactions.Transactions(); len(txs) > 0 {
		return int64(len(txs))
	}
	return int64(len(block.Transactions.Hashes()))
}

// cloneReceipts converts []*Receipt into []Receipt to match the return type
// expected by the lazy helper.
func cloneReceipts(receipts []*web3Types.Receipt) []web3Types.Receipt {
	out := make([]web3Types.Receipt, len(receipts))
	for i := range receipts {
		out[i] = *receipts[i]
	}
	return out
}

// readWithExpiryCache is used by simple RPC methods backed only by the expiry cache.
func readWithExpiryCache[T any](method string, loader func() (T, bool, error)) (v T, err error) {
	m := newRPCCacheTracker(method)
	defer m.report()

	value, loaded, err := loader()
	if err != nil {
		return v, m.withError(err)
	}

	m.withExpiryCacheHit(loaded).withFullNodeHit(!loaded)
	return value, nil
}

// readThroughCacheChain executes a generic read-through flow:
//
//  1. Near-head cache
//  2. Data cache
//  3. Full node
func readThroughCacheChain[T any](
	tracker *rpcCacheTracker,
	fromNearHead func() (T, bool, error),
	fromDataCache func() (T, bool, error),
	fromFullNode func() (T, error),
) (res T, err error) {
	tracker.withNearHeadHit(false).withFullNodeHit(false).withDataCacheHit(false)

	// Try near-head cache
	value, hit, err := fromNearHead()
	if err != nil {
		logrus.WithError(err).
			WithField("method", tracker.method).
			Info("Failed to read from ETH near-head cache")
	} else if hit {
		tracker.withNearHeadHit(true)
		return value, nil
	}

	// Try data cache if applicable
	value, hit, err = fromDataCache()
	if err != nil {
		logrus.WithError(err).
			WithField("method", tracker.method).
			Info("Failed to read from ETH data cache")
	} else if hit {
		tracker.withDataCacheHit(true)
		return value, nil
	}

	// Fall back to full node
	value, err = fromFullNode()
	if err != nil {
		return value, tracker.withError(err)
	}

	tracker.withFullNodeHit(true)
	return value, nil
}

// readThroughLazyCacheChain implements a generic lazy read-through flow.
func readThroughLazyCacheChain[T any](
	tracker *rpcCacheTracker,
	fromNearHead func() (cacheTypes.Lazy[T], error),
	fromDataCache func() (cacheTypes.Lazy[T], error),
	fromFullNode func() (cacheTypes.Lazy[T], error),
) (cacheTypes.Lazy[T], error) {
	return readThroughCacheChain(
		tracker,
		func() (res cacheTypes.Lazy[T], isHit bool, err error) {
			res, err = fromNearHead()
			if err != nil {
				return res, false, err
			}
			return res, !res.IsEmptyOrNull(), err
		},
		func() (res cacheTypes.Lazy[T], isHit bool, err error) {
			res, err = fromDataCache()
			if err != nil {
				return res, false, err
			}
			return res, !res.IsEmptyOrNull(), err
		},
		func() (res cacheTypes.Lazy[T], err error) {
			return fromFullNode()
		},
	)
}
