package cache

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/ethereum/go-ethereum/common"
	"github.com/mcuadros/go-defaults"
	"github.com/openweb3/go-rpc-provider/utils"
	"github.com/openweb3/web3go/client"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	EthDefault *EthCache = newEthCache(newEthCacheConfig())
)

type EthCacheConfig struct {
	NetVersionExpiration      time.Duration `default:"1m"`
	ClientVersionExpiration   time.Duration `default:"1m"`
	ChainIdExpiration         time.Duration `default:"8760h"`
	BlockNumberExpiration     time.Duration `default:"1s"`
	PriceExpiration           time.Duration `default:"3s"`
	CallCacheExpiration       time.Duration `default:"1s"`
	CallCacheSize             int           `default:"1024"`
	PendingTxnCacheExpiration time.Duration `default:"3m"`
	PendingTxnCacheSize       int           `default:"1024"`
	PendingTxnCheckExemption  time.Duration `default:"3s"`
	PendingTxnCheckInterval   time.Duration `default:"1s"`
}

// newEthCacheConfig returns a EthCacheConfig with default values.
func newEthCacheConfig() EthCacheConfig {
	var cfg EthCacheConfig
	defaults.SetDefaults(&cfg)
	return cfg
}

func MustInitFromViper() {
	config := newEthCacheConfig()
	viper.MustUnmarshalKey("requestControl.ethCache", &config)

	EthDefault = newEthCache(config)
}

// EthCache memory cache for some evm space RPC methods
type EthCache struct {
	EthCacheConfig
	netVersionCache    *expiryCache
	clientVersionCache *expiryCache
	chainIdCache       *expiryCache
	priceCache         *expiryCache
	blockNumberCache   *keyExpiryLruCaches
	callCache          *keyExpiryLruCaches
	pendingTxnCache    *keyExpiryLruCaches
}

func newEthCache(cfg EthCacheConfig) *EthCache {
	return &EthCache{
		EthCacheConfig:     cfg,
		netVersionCache:    newExpiryCache(cfg.NetVersionExpiration),
		clientVersionCache: newExpiryCache(cfg.ClientVersionExpiration),
		chainIdCache:       newExpiryCache(cfg.ChainIdExpiration),
		priceCache:         newExpiryCache(cfg.PriceExpiration),
		blockNumberCache:   newKeyExpiryLruCaches(cfg.BlockNumberExpiration, 1000),
		callCache:          newKeyExpiryLruCaches(cfg.CallCacheExpiration, cfg.CallCacheSize),
		pendingTxnCache:    newKeyExpiryLruCaches(cfg.PendingTxnCacheExpiration, cfg.PendingTxnCacheSize),
	}
}

func (cache *EthCache) GetNetVersion(eth *client.RpcEthClient) (string, bool, error) {
	return cache.GetNetVersionWithFunc(eth.NetVersion)
}

func (cache *EthCache) GetNetVersionWithFunc(rawGetter func() (string, error)) (string, bool, error) {
	val, loaded, err := cache.netVersionCache.getOrUpdate(func() (any, error) {
		return rawGetter()
	})
	if err != nil {
		return "", false, err
	}
	return val.(string), loaded, nil
}

func (cache *EthCache) GetClientVersion(eth *client.RpcEthClient) (string, bool, error) {
	return cache.GetClientVersionWithFunc(eth.ClientVersion)
}

func (cache *EthCache) GetClientVersionWithFunc(rawGetter func() (string, error)) (string, bool, error) {
	val, loaded, err := cache.clientVersionCache.getOrUpdate(func() (any, error) {
		return rawGetter()
	})
	if err != nil {
		return "", false, err
	}
	return val.(string), loaded, nil
}

func (cache *EthCache) GetChainId(eth *client.RpcEthClient) (*uint64, bool, error) {
	return cache.GetChainIdWithFunc(eth.ChainId)
}

func (cache *EthCache) GetChainIdWithFunc(rawGetter func() (*uint64, error)) (*uint64, bool, error) {
	val, loaded, err := cache.chainIdCache.getOrUpdate(func() (any, error) {
		return rawGetter()
	})
	if err != nil {
		return nil, false, err
	}
	return val.(*uint64), loaded, nil
}

func (cache *EthCache) GetGasPrice(eth *client.RpcEthClient) (*big.Int, bool, error) {
	return cache.GetGasPriceWithFunc(eth.GasPrice)
}

func (cache *EthCache) GetGasPriceWithFunc(rawGetter func() (*big.Int, error)) (*big.Int, bool, error) {
	val, loaded, err := cache.priceCache.getOrUpdate(func() (any, error) {
		return rawGetter()
	})
	if err != nil {
		return nil, false, err
	}
	return val.(*big.Int), loaded, nil
}

func (cache *EthCache) GetBlockNumber(
	nodeName string, eth *client.RpcEthClient, blockNums ...types.BlockNumber) (*big.Int, bool, error) {
	blockNum := types.LatestBlockNumber
	if len(blockNums) > 0 {
		blockNum = blockNums[0]
	}
	if blockNum > 0 {
		return big.NewInt(blockNum.Int64()), true, nil
	}
	return cache.GetBlockNumberWithFunc(nodeName, blockNum, func() (*big.Int, error) {
		if blockNum == types.LatestBlockNumber {
			return eth.BlockNumber()
		}
		block, err := eth.BlockByNumber(blockNum, false)
		if err != nil {
			return nil, err
		}
		if block == nil {
			return nil, errors.New("block not found")
		}
		return block.Number, nil
	})
}

func (cache *EthCache) GetBlockNumberWithFunc(
	nodeName string,
	blockNum types.BlockNumber,
	rawGetter func() (*big.Int, error),
) (*big.Int, bool, error) {
	cacheKey := fmt.Sprintf("%s::%d", nodeName, blockNum.Int64())
	val, loaded, err := cache.blockNumberCache.getOrUpdate(cacheKey, func() (any, error) {
		return rawGetter()
	})
	if err != nil {
		return nil, false, err
	}
	return val.(*big.Int), loaded, nil
}

// RPCResult represents the result of an RPC call,
// containing the response data or a potential JSON-RPC error.
type RPCResult struct {
	Data     any
	RpcError error
}

func (cache *EthCache) Call(
	nodeName string,
	eth *client.RpcEthClient,
	callRequest types.CallRequest,
	blockNum *types.BlockNumberOrHash,
) (RPCResult, bool, error) {
	return cache.CallWithFunc(nodeName, callRequest, blockNum,
		func() ([]byte, error) {
			return eth.Call(callRequest, blockNum)
		},
	)
}

func (cache *EthCache) CallWithFunc(
	nodeName string,
	callRequest types.CallRequest,
	blockNum *types.BlockNumberOrHash,
	rawGetter func() ([]byte, error),
) (RPCResult, bool, error) {
	cacheKey, err := generateCallCacheKey(nodeName, callRequest, blockNum)
	if err != nil {
		// This should rarely happen, but if it does, we don't want to fail the entire request due to cache error.
		// The error is logged and the request is forwarded to the node directly.
		logrus.WithFields(logrus.Fields{
			"nodeName": nodeName,
			"callReq":  callRequest,
			"blockNum": blockNum,
		}).WithError(err).Error("Failed to generate cache key for `eth_call`")
		val, err := rawGetter()
		return RPCResult{Data: val}, false, err
	}

	val, loaded, err := cache.callCache.getOrUpdate(cacheKey, func() (any, error) {
		data, err := rawGetter()
		// Cache RPC JSON errors or successful results
		if err == nil || utils.IsRPCJSONError(err) {
			return RPCResult{Data: data, RpcError: err}, nil
		}
		// Propagate other non JSON-RPC errors
		return nil, err
	})
	if err != nil {
		return RPCResult{}, false, err
	}
	return val.(RPCResult), loaded, nil
}

func generateCallCacheKey(nodeName string, callRequest types.CallRequest, blockNum *types.BlockNumberOrHash) (string, error) {
	// Create a map of parameters to be serialized
	params := map[string]any{
		"nodeName":    nodeName,
		"callRequest": callRequest,
		"blockNum":    blockNum,
	}

	// Serialize the parameters to JSON
	jsonBytes, err := json.Marshal(params)
	if err != nil {
		return "", err
	}

	// Generate MD5 hash
	hash := md5.New()
	hash.Write(jsonBytes)

	// Convert hash to a hexadecimal string
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (cache *EthCache) AddPendingTransaction(txnHash common.Hash) {
	cache.pendingTxnCache.getOrUpdate(txnHash.String(), func() (any, error) {
		return &ethPendingTxn{createdAt: time.Now()}, nil
	})
}

func (cache *EthCache) GetPendingTransaction(txHash common.Hash) (pendingTxn *ethPendingTxn, loaded, expired bool) {
	v, ok := cache.pendingTxnCache.get(txHash.String())
	if !ok {
		return nil, false, false
	}

	pendingTxn = v.(*ethPendingTxn)
	expired = pendingTxn.shouldCheckNow(cache.PendingTxnCheckExemption, cache.PendingTxnCheckInterval)
	return pendingTxn, true, expired
}

func (cache *EthCache) RemovePendingTransaction(txHash common.Hash) bool {
	return cache.pendingTxnCache.del(txHash.String())
}

type ethPendingTxn struct {
	val           atomic.Value
	createdAt     time.Time    // Creation time
	lastCheckedAt atomic.Int64 // Last check timestamp of mined status
}

// MarkChecked marks the mined status as checked
func (t *ethPendingTxn) MarkChecked() {
	t.lastCheckedAt.Store(time.Now().UnixMilli())
}

// Set updates the pending transaction detail
func (t *ethPendingTxn) Set(txn *types.TransactionDetail) {
	t.val.Store(txn)
}

// Get gets the pending transaction detail
func (t *ethPendingTxn) Get() (*types.TransactionDetail, bool) {
	v, ok := t.val.Load().(*types.TransactionDetail)
	return v, ok
}

// ShouldCheckNow returns whether it's time to check if the pending transaction is mined.
func (t *ethPendingTxn) shouldCheckNow(exemption, checkInterval time.Duration) bool {
	return time.Since(t.createdAt) > exemption &&
		time.Since(time.UnixMilli(t.lastCheckedAt.Load())) > checkInterval
}
