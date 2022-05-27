package cache

import (
	"math/big"
	"time"

	"github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/util/rpc"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/openweb3/web3go"
)

type EthCache struct {
	netVersionCache    *expiryCache
	clientVersionCache *expiryCache
	chainIdCache       *expiryCache
	priceCache         *expiryCache
	blockNumberCache   *nodeExpiryCaches
}

func NewEth() *EthCache {
	return &EthCache{
		netVersionCache:    newExpiryCache(time.Minute),
		clientVersionCache: newExpiryCache(time.Minute),
		chainIdCache:       newExpiryCache(time.Hour * 24 * 365 * 100),
		priceCache:         newExpiryCache(3 * time.Second),
		blockNumberCache:   newNodeExpiryCaches(time.Second),
	}
}

func (cache *EthCache) GetNetVersion(client *web3go.Client) (string, error) {
	val, err := cache.netVersionCache.getOrUpdate(func() (interface{}, error) {
		return client.Eth.NetVersion()
	})

	if err != nil {
		return "", err
	}

	return val.(string), nil
}

func (cache *EthCache) GetClientVersion(client *web3go.Client) (string, error) {
	val, err := cache.clientVersionCache.getOrUpdate(func() (interface{}, error) {
		return client.Eth.ClientVersion()
	})

	if err != nil {
		return "", err
	}

	return val.(string), nil
}

func (cache *EthCache) GetChainId(client *web3go.Client) (*hexutil.Uint64, error) {
	val, err := cache.chainIdCache.getOrUpdate(func() (interface{}, error) {
		return client.Eth.ChainId()
	})

	if err != nil {
		return nil, err
	}

	return (*hexutil.Uint64)(val.(*uint64)), nil
}

func (cache *EthCache) GetGasPrice(client *web3go.Client) (*hexutil.Big, error) {
	val, err := cache.priceCache.getOrUpdate(func() (interface{}, error) {
		return client.Eth.GasPrice()
	})

	if err != nil {
		return nil, err
	}

	return (*hexutil.Big)(val.(*big.Int)), nil
}

func (cache *EthCache) GetBlockNumber(client *node.Web3goClient) (*hexutil.Big, error) {
	nodeName := rpc.Url2NodeName(client.URL)

	val, err := cache.blockNumberCache.getOrUpdate(nodeName, func() (interface{}, error) {
		return client.Eth.BlockNumber()
	})

	if err != nil {
		return nil, err
	}

	return (*hexutil.Big)(val.(*big.Int)), nil
}
