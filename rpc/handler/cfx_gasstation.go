package handler

import (
	"math/big"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/types"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
)

// GasFeeMultiplier defines multipliers for different priority levels.
type GasFeeMultiplier struct {
	Low    float64 `default:"0.75"`
	Medium float64 `default:"1"`
	High   float64 `default:"1.25"`
}

// GasStationConfig holds the gas fee multipliers.
type GasStationConfig struct {
	Multiplier GasFeeMultiplier
}

// CfxGasStationHandler handles RPC requests for gas price estimation.
type CfxGasStationHandler struct {
	conf GasStationConfig
}

// MustNewCfxGasStationHandlerFromViper initializes handler from viper config.
func MustNewCfxGasStationHandlerFromViper(cp *node.CfxClientProvider) *CfxGasStationHandler {
	var cfg GasStationConfig
	viper.MustUnmarshalKey("gasStation", &cfg)
	return &CfxGasStationHandler{conf: cfg}
}

// multiplyBigIntByFloat multiplies a big.Int by a float64 safely with high precision.
func multiplyBigIntByFloat(i *big.Int, f float64) *big.Int {
	bf := new(big.Float).SetPrec(256).SetInt(i)
	bf.Mul(bf, new(big.Float).SetFloat64(f))
	result := new(big.Int)
	bf.Int(result) // truncate toward zero
	return result
}

// SuggestGasFee estimates gas fees for low/medium/high priority.
func (h *CfxGasStationHandler) SuggestGasFee(cfx sdk.ClientOperator) (*types.SuggestedGasFees, error) {
	latestBlock, err := cfx.GetBlockSummaryByEpoch(cfxtypes.EpochLatestState)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get latest block")
	}
	baseFeePerGas := latestBlock.BaseFeePerGas.ToInt()

	stdPriorityFeePerGas, err := cfx.GetMaxPriorityFeePerGas()
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get max priority fee per gas")
	}

	// helper to build estimation
	buildEstimation := func(multiplier float64) types.GasFeeEstimation {
		priorityFee := multiplyBigIntByFloat(stdPriorityFeePerGas.ToInt(), multiplier)
		return types.GasFeeEstimation{
			SuggestedMaxPriorityFeePerGas: (*hexutil.Big)(priorityFee),
			SuggestedMaxFeePerGas:         (*hexutil.Big)(new(big.Int).Add(baseFeePerGas, priorityFee)),
		}
	}

	return &types.SuggestedGasFees{
		Low:              buildEstimation(h.conf.Multiplier.Low),
		Medium:           buildEstimation(h.conf.Multiplier.Medium),
		High:             buildEstimation(h.conf.Multiplier.High),
		EstimatedBaseFee: (*hexutil.Big)(baseFeePerGas),
	}, nil
}

// SuggestGasPrice returns gas prices multiplied by configured multipliers.
func (h *CfxGasStationHandler) SuggestGasPrice(cfx sdk.ClientOperator) (*types.GasStationPrice, error) {
	stdGasPrice, err := cfx.GetGasPrice()
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get gas price")
	}

	mediumGasPrice := multiplyBigIntByFloat(stdGasPrice.ToInt(), h.conf.Multiplier.Medium)
	highGasPrice := multiplyBigIntByFloat(stdGasPrice.ToInt(), h.conf.Multiplier.High)

	return &types.GasStationPrice{
		Average: stdGasPrice,
		SafeLow: stdGasPrice,
		Fast:    (*hexutil.Big)(mediumGasPrice),
		Fastest: (*hexutil.Big)(highGasPrice),
	}, nil
}
