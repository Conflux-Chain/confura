package cache

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	cacheTypes "github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
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
	CallCacheSize             int           `default:"128"`
	PendingTxnCacheExpiration time.Duration `default:"3s"`
	PendingTxnCacheSize       int           `default:"1024"`
	TxnCacheExpiration        time.Duration `default:"1s"`
	TxnCacheSize              int           `default:"1024"`
	ReceiptCacheExpiration    time.Duration `default:"1s"`
	ReceiptCacheSize          int           `default:"1024"`
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
	blockNumberCache   *keyExpiryLruCaches
	callCache          *keyExpiryLruCaches
	pendingTxnCache    *keyExpiryLruCaches
	txnCache           *keyExpiryLruCaches
	receiptCache       *keyExpiryLruCaches
}

func newEthCache(cfg EthCacheConfig) *EthCache {
	return &EthCache{
		netVersionCache:    newExpiryCache(cfg.NetVersionExpiration),
		clientVersionCache: newExpiryCache(cfg.ClientVersionExpiration),
		chainIdCache:       newExpiryCache(cfg.ChainIdExpiration),
		priceCache:         newExpiryCache(cfg.PriceExpiration),
		blockNumberCache:   newKeyExpiryLruCaches(cfg.BlockNumberExpiration, 1000),
		callCache:          newKeyExpiryLruCaches(cfg.CallCacheExpiration, cfg.CallCacheSize),
		pendingTxnCache:    newKeyExpiryLruCaches(cfg.PendingTxnCacheExpiration, cfg.PendingTxnCacheSize),
		txnCache:           newKeyExpiryLruCaches(cfg.TxnCacheExpiration, cfg.TxnCacheSize),
		receiptCache:       newKeyExpiryLruCaches(cfg.ReceiptCacheExpiration, cfg.ReceiptCacheSize),
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
	if blockNum >= 0 {
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

type lazyRlpDecodedTxn struct {
	encoded []byte
	val     atomic.Value
}

func newLazyRlpDecodedTxn(data []byte) *lazyRlpDecodedTxn {
	return &lazyRlpDecodedTxn{encoded: data}
}

func (lazy *lazyRlpDecodedTxn) Load() (*types.Transaction, error) {
	if len(lazy.encoded) == 0 {
		return nil, nil
	}

	if v, ok := lazy.val.Load().(*types.Transaction); ok {
		return v, nil
	}

	var txn types.Transaction
	if err := txn.UnmarshalBinary(lazy.encoded); err != nil {
		return nil, errors.WithMessage(err, "failed to unmarshal transaction")
	}

	lazy.val.Store(&txn)
	return &txn, nil
}

func (cache *EthCache) AddPendingTransaction(txHash common.Hash, rawTxnData []byte) {
	cache.pendingTxnCache.getOrUpdate(txHash.String(), func() (any, error) {
		return newLazyRlpDecodedTxn(rawTxnData), nil
	})
}

func (cache *EthCache) GetPendingTransaction(txHash common.Hash) (*types.TransactionDetail, bool, error) {
	v, ok := cache.pendingTxnCache.get(txHash.String())
	if !ok {
		return nil, false, nil // not found in cache
	}

	txn, err := v.(*lazyRlpDecodedTxn).Load()
	if err != nil {
		return nil, false, errors.WithMessagef(err, "failed to load transaction")
	}

	// Build the transaction detail
	txnDetail, err := buildTransactionDetail(txn)
	if err != nil {
		return nil, false, err
	}
	return txnDetail, true, nil
}

func (cache *EthCache) GetTransactionByHashWithFunc(
	nodeName string,
	txHash common.Hash,
	rawGetter func() (cacheTypes.Lazy[*types.TransactionDetail], error),
) (res cacheTypes.Lazy[*types.TransactionDetail], loaded bool, err error) {
	pendingCacheHit := false
	cacheKey := fmt.Sprintf("%s::%s", nodeName, txHash)

	val, loaded, err := cache.txnCache.getOrUpdate(cacheKey, func() (any, error) {
		txn, ok, err := cache.GetPendingTransaction(txHash)
		if err != nil {
			return res, err
		}
		if ok {
			pendingCacheHit = true
			return cacheTypes.NewLazy(txn)
		}
		return rawGetter()
	})
	if err != nil {
		return res, false, err
	}
	return val.(cacheTypes.Lazy[*types.TransactionDetail]), loaded || pendingCacheHit, nil
}

func (cache *EthCache) GetTransactionReceiptWithFunc(
	nodeName string,
	txHash common.Hash,
	rawGetter func() (cacheTypes.Lazy[*types.Receipt], error),
) (res cacheTypes.Lazy[*types.Receipt], loaed bool, err error) {
	if _, ok := cache.pendingTxnCache.get(txHash.String()); ok {
		// Pending transaction does not have receipt yet.
		return res, true, nil
	}

	cacheKey := fmt.Sprintf("%s::%s", nodeName, txHash)
	val, loaded, err := cache.receiptCache.getOrUpdate(cacheKey, func() (any, error) {
		return rawGetter()
	})
	if err != nil {
		return res, false, err
	}

	return val.(cacheTypes.Lazy[*types.Receipt]), loaded, nil
}

func buildTransactionDetail(txn *types.Transaction) (*types.TransactionDetail, error) {
	chainId := txn.ChainId()
	txnType := uint64(txn.Type())
	sigV, sigR, sigS := txn.RawSignatureValues()

	// Recover the sender address
	singer := ethTypes.LatestSignerForChainID(chainId)
	from, err := ethTypes.Sender(singer, txn)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to recover sender of pending transaction")
	}

	return &types.TransactionDetail{
		// Transaction is not included in a block yet, so `BlockHash`, `BlockNumber`, `TransactionIndex`
		// and `Status` fields are left empty by default.
		ChainID:              chainId,
		Nonce:                txn.Nonce(),
		GasPrice:             txn.GasPrice(),
		MaxFeePerGas:         txn.GasFeeCap(),
		MaxPriorityFeePerGas: txn.GasTipCap(),
		Gas:                  txn.Gas(),
		From:                 from,
		To:                   txn.To(),
		Value:                txn.Value(),
		Input:                txn.Data(),
		V:                    sigR,
		R:                    sigV,
		S:                    sigS,
		Accesses:             txn.AccessList(),
		Hash:                 txn.Hash(),
		Type:                 &txnType,
	}, nil
}
