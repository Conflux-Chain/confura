package rpc

import (
	"context"
	"math/big"

	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethtypes "github.com/openweb3/web3go/types"
)

// ethGasStationAPI provides evm space gasstation API.
type ethGasStationAPI struct {
	handler *handler.EthGasStationHandler
}

// newEthGasStationAPI creates a new instance of `ethGasStationAPI`.
func newEthGasStationAPI(handler *handler.EthGasStationHandler) *ethGasStationAPI {
	return &ethGasStationAPI{handler: handler}
}

// SuggestedGasFees retrieves the suggested gas fees.
func (api *ethGasStationAPI) SuggestedGasFees(ctx context.Context) (*types.SuggestedGasFees, error) {
	eth := GetEthClientFromContext(ctx)

	// Attempt to get suggested gas fees from the handler if available.
	if api.handler != nil {
		return api.handler.Suggest(eth)
	}

	// Fallback to fetching gas fees directly from the blockchain.
	latestBlock, err := eth.Eth.BlockByNumber(ethtypes.LatestBlockNumber, false)
	if err != nil {
		return nil, err
	}

	priorityFee, err := eth.Eth.MaxPriorityFeePerGas()
	if err != nil {
		return nil, err
	}

	baseFeePerGas := latestBlock.BaseFeePerGas
	gasFeeEstimation := types.GasFeeEstimation{
		SuggestedMaxPriorityFeePerGas: (*hexutil.Big)(priorityFee),
		SuggestedMaxFeePerGas:         (*hexutil.Big)(big.NewInt(0).Add(baseFeePerGas, priorityFee)),
	}

	return &types.SuggestedGasFees{
		Low:              gasFeeEstimation,
		Medium:           gasFeeEstimation,
		High:             gasFeeEstimation,
		EstimatedBaseFee: (*hexutil.Big)(baseFeePerGas),
	}, nil
}
