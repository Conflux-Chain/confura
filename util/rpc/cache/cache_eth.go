package cache

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"time"

	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/mcuadros/go-defaults"
	"github.com/openweb3/go-rpc-provider/utils"
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	EthDefault *EthCache = newEthCache(newEthCacheConfig())
)

type EthCacheConfig struct {
	NetVersionExpiration    time.Duration `default:"1m"`
	ClientVersionExpiration time.Duration `default:"1m"`
	ChainIdExpiration       time.Duration `default:"8760h"`
	BlockNumberExpiration   time.Duration `default:"1s"`
	PriceExpiration         time.Duration `default:"3s"`
	CallCacheExpiration     time.Duration `default:"1s"`
	CallCacheSize           int           `default:"128"`
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
	netVersionCache    *expiryCache
	clientVersionCache *expiryCache
	chainIdCache       *expiryCache
	priceCache         *expiryCache
	blockNumberCache   *nodeExpiryCaches
	callCache          *keyExpiryLruCaches
}

func newEthCache(cfg EthCacheConfig) *EthCache {
	return &EthCache{
		netVersionCache:    newExpiryCache(cfg.NetVersionExpiration),
		clientVersionCache: newExpiryCache(cfg.ClientVersionExpiration),
		chainIdCache:       newExpiryCache(cfg.ChainIdExpiration),
		priceCache:         newExpiryCache(cfg.PriceExpiration),
		blockNumberCache:   newNodeExpiryCaches(cfg.BlockNumberExpiration),
		callCache:          newKeyExpiryLruCaches(cfg.CallCacheExpiration, cfg.CallCacheSize),
	}
}

func (cache *EthCache) GetNetVersion(client *web3go.Client) (string, bool, error) {
	val, loaded, err := cache.netVersionCache.getOrUpdate(func() (interface{}, error) {
		return client.Eth.NetVersion()
	})

	if err != nil {
		return "", false, err
	}

	return val.(string), loaded, nil
}

func (cache *EthCache) GetClientVersion(client *web3go.Client) (string, bool, error) {
	val, loaded, err := cache.clientVersionCache.getOrUpdate(func() (interface{}, error) {
		return client.Eth.ClientVersion()
	})

	if err != nil {
		return "", false, err
	}

	return val.(string), loaded, nil
}

func (cache *EthCache) GetChainId(client *web3go.Client) (*hexutil.Uint64, bool, error) {
	val, loaded, err := cache.chainIdCache.getOrUpdate(func() (interface{}, error) {
		return client.Eth.ChainId()
	})

	if err != nil {
		return nil, false, err
	}

	return (*hexutil.Uint64)(val.(*uint64)), loaded, nil
}

func (cache *EthCache) GetGasPrice(client *web3go.Client) (*hexutil.Big, bool, error) {
	val, loaded, err := cache.priceCache.getOrUpdate(func() (interface{}, error) {
		return client.Eth.GasPrice()
	})

	if err != nil {
		return nil, false, err
	}

	return (*hexutil.Big)(val.(*big.Int)), loaded, nil
}

func (cache *EthCache) GetBlockNumber(nodeName string, client *web3go.Client) (*hexutil.Big, bool, error) {
	val, loaded, err := cache.blockNumberCache.getOrUpdate(nodeName, func() (interface{}, error) {
		return client.Eth.BlockNumber()
	})

	if err != nil {
		return nil, false, err
	}

	return (*hexutil.Big)(val.(*big.Int)), loaded, nil
}

// RPCResult represents the result of an RPC call,
// containing the response data or a potential JSON-RPC error.
type RPCResult struct {
	Data     interface{}
	RpcError error
}

func (cache *EthCache) Call(
	nodeName string,
	client *web3go.Client,
	callRequest types.CallRequest,
	blockNum *types.BlockNumberOrHash,
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
		val, err := client.Eth.Call(callRequest, blockNum)
		return RPCResult{Data: val}, false, err
	}

	val, loaded, err := cache.callCache.getOrUpdate(cacheKey, func() (interface{}, error) {
		data, err := client.Eth.Call(callRequest, blockNum)
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

	// Return the cached result
	cachedResult, ok := val.(RPCResult)
	if !ok { // This should rarely happen, but just in case.
		logrus.WithFields(logrus.Fields{
			"nodeName": nodeName,
			"callReq":  callRequest,
			"blockNum": blockNum,
			"cacheKey": cacheKey,
			"cacheVal": val,
		}).Error("Unexpected cache value type for `eth_call`")
		return RPCResult{}, false, errors.Errorf("unexpected cache value type")
	}

	return cachedResult, loaded, nil
}

func generateCallCacheKey(nodeName string, callRequest types.CallRequest, blockNum *types.BlockNumberOrHash) (string, error) {
	// Create a map of parameters to be serialized
	params := map[string]interface{}{
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
