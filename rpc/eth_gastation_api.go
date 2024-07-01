package rpc

import (
	"context"
	"math/big"

	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/types"
	logutil "github.com/Conflux-Chain/go-conflux-util/log"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethtypes "github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
)

// ethGasStationAPI provides evm space gasstation API.
type ethGasStationAPI struct {
	handler  *handler.EthGasStationHandler
	etLogger *logutil.ErrorTolerantLogger
}

// newEthGasStationAPI creates a new instance of `ethGasStationAPI`.
func newEthGasStationAPI(handler *handler.EthGasStationHandler) *ethGasStationAPI {
	return &ethGasStationAPI{
		handler:  handler,
		etLogger: logutil.NewErrorTolerantLogger(logutil.DefaultETConfig),
	}
}

// SuggestedGasFees retrieves the suggested gas fees.
func (api *ethGasStationAPI) SuggestedGasFees(ctx context.Context) (*types.SuggestedGasFees, error) {
	eth := GetEthClientFromContext(ctx)

	// Attempt to get suggested gas fees from the handler if available.
	if api.handler != nil {
		gasFee, err := api.handler.Suggest(eth)
		api.etLogger.Log(
			logrus.StandardLogger(), err, "Failed to get suggested gas fees from handler",
		)
		return gasFee, err
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
