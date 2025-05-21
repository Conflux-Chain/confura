package rpc

import (
	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/Conflux-Chain/confura/util/rpc/cache"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
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
	version, loaded, err := cache.CfxDefault.GetClientVersion(c.ClientOperator)
	if err != nil {
		return "", err
	}

	metrics.Registry.Client.ExpiryCacheHit("cfx_clientVersion").Mark(loaded)
	return version, nil
}

func (c CfxCoreClient) GetGasPrice() (*hexutil.Big, error) {
	gasPrice, loaded, err := cache.CfxDefault.GetGasPrice(c.ClientOperator)
	if err != nil {
		return nil, err
	}

	metrics.Registry.Client.ExpiryCacheHit("cfx_gasPrice").Mark(loaded)
	return gasPrice, nil
}

func (c CfxCoreClient) GetStatus() (types.Status, error) {
	status, loaded, err := cache.CfxDefault.GetStatus(c.nodeName, c.ClientOperator)
	if err != nil {
		return types.Status{}, err
	}

	metrics.Registry.Client.ExpiryCacheHit("cfx_status").Mark(loaded)
	return status, nil
}

func (c CfxCoreClient) GetEpochNumber(epochs ...*types.Epoch) (*hexutil.Big, error) {
	var epoch *types.Epoch
	if len(epochs) > 0 {
		epoch = epochs[0]
	}

	epochNumber, loaded, err := cache.CfxDefault.GetEpochNumber(c.nodeName, c.ClientOperator, epoch)
	if err != nil {
		return nil, err
	}
	metrics.Registry.Client.ExpiryCacheHit("cfx_epochNumber").Mark(loaded)
	return epochNumber, nil
}

func (c CfxCoreClient) GetBestBlockHash() (types.Hash, error) {
	blockHash, loaded, err := cache.CfxDefault.GetBestBlockHash(c.nodeName, c.ClientOperator)
	if err != nil {
		return blockHash, err
	}

	metrics.Registry.Client.ExpiryCacheHit("cfx_bestBlockHash").Mark(loaded)
	return blockHash, nil
}
