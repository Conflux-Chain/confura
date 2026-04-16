package rpc

import (
	"github.com/Conflux-Chain/confura/util/rpc/cache"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

const (
	rpcMethodCfxChainID       = "cfx_clientVersion"
	rpcMethodCfxGasPrice      = "cfx_gasPrice"
	rpcMethodCfxStatus        = "cfx_status"
	rpcMethodCfxEpochNumber   = "cfx_epochNumber"
	rpcMethodCfxBestBlockHash = "cfx_bestBlockHash"
)

type CfxCoreClient struct {
	sdk.ClientOperator
	nodeName string
}

func NewCfxCoreClient(client sdk.ClientOperator) *CfxCoreClient {
	return &CfxCoreClient{
		ClientOperator: client,
		nodeName:       Url2NodeName(client.GetNodeURL()),
	}
}

func (c CfxCoreClient) GetClientVersion() (string, error) {
	return readWithExpiryCache(rpcMethodCfxChainID, func() (string, bool, error) {
		return cache.CfxDefault.GetClientVersion(c.ClientOperator)
	})
}

func (c CfxCoreClient) GetGasPrice() (*hexutil.Big, error) {
	return readWithExpiryCache(rpcMethodCfxGasPrice, func() (*hexutil.Big, bool, error) {
		return cache.CfxDefault.GetGasPrice(c.ClientOperator)
	})
}

func (c CfxCoreClient) GetStatus() (types.Status, error) {
	return readWithExpiryCache(rpcMethodCfxStatus, func() (types.Status, bool, error) {
		return cache.CfxDefault.GetStatus(c.nodeName, c.ClientOperator)
	})
}

func (c CfxCoreClient) GetEpochNumber(epochs ...*types.Epoch) (*hexutil.Big, error) {
	var epoch *types.Epoch
	if len(epochs) > 0 {
		epoch = epochs[0]
	}

	return readWithExpiryCache(rpcMethodCfxEpochNumber, func() (*hexutil.Big, bool, error) {
		return cache.CfxDefault.GetEpochNumber(c.nodeName, c.ClientOperator, epoch)
	})
}

func (c CfxCoreClient) GetBestBlockHash() (types.Hash, error) {
	return readWithExpiryCache(rpcMethodCfxBestBlockHash, func() (types.Hash, bool, error) {
		return cache.CfxDefault.GetBestBlockHash(c.nodeName, c.ClientOperator)
	})
}
