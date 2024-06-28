package rpc

import (
	"context"
	"math/big"

	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/types"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	logutil "github.com/Conflux-Chain/go-conflux-util/log"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/sirupsen/logrus"
)

// cfxGasStationAPI provides core space gasstation API.
type cfxGasStationAPI struct {
	handler  *handler.CfxGasStationHandler
	etLogger *logutil.ErrorTolerantLogger
}

// newCfxGasStationAPI creates a new instance of `cfxGasStationAPI`.
func newCfxGasStationAPI(handler *handler.CfxGasStationHandler) *cfxGasStationAPI {
	return &cfxGasStationAPI{
		handler:  handler,
		etLogger: logutil.NewErrorTolerantLogger(logutil.DefaultETConfig),
	}
}

// SuggestedGasFees retrieves the suggested gas fees.
func (api *cfxGasStationAPI) SuggestedGasFees(ctx context.Context) (*types.SuggestedGasFees, error) {
	cfx := GetCfxClientFromContext(ctx)

	// Attempt to get suggested gas fees from the handler if available.
	if api.handler != nil {
		gasFee, err := api.handler.Suggest(cfx)
		api.etLogger.Log(
			logrus.StandardLogger(), err, "Failed to get suggested gas fees from handler",
		)
		return gasFee, err
	}

	// Fallback to fetching gas fees directly from the blockchain.
	latestBlock, err := cfx.GetBlockSummaryByEpoch(cfxtypes.EpochLatestState)
	if err != nil {
		return nil, err
	}

	priorityFee, err := cfx.GetMaxPriorityFeePerGas()
	if err != nil {
		return nil, err
	}

	baseFeePerGas := latestBlock.BaseFeePerGas.ToInt()
	gasFeeEstimation := types.GasFeeEstimation{
		SuggestedMaxPriorityFeePerGas: priorityFee,
		SuggestedMaxFeePerGas:         (*hexutil.Big)(big.NewInt(0).Add(baseFeePerGas, priorityFee.ToInt())),
	}

	return &types.SuggestedGasFees{
		Low:              gasFeeEstimation,
		Medium:           gasFeeEstimation,
		High:             gasFeeEstimation,
		EstimatedBaseFee: (*hexutil.Big)(baseFeePerGas),
	}, nil
}

// TODO: Deprecate it if not used by the community any more.
func (api *cfxGasStationAPI) Price(ctx context.Context) (*types.GasStationPrice, error) {
	// Use oracle gas price from the blockchain.
	cfx := GetCfxClientFromContext(ctx)
	price, err := cfx.GetGasPrice()
	if err != nil {
		return nil, err
	}

	return &types.GasStationPrice{
		Fast:    price,
		Fastest: price,
		SafeLow: price,
		Average: price,
	}, nil
}
