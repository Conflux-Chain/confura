package types

import (
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// GasFeeTrend represents the trend of gas fees.
type GasFeeTrend string

const (
	GasFeeTrendUp   GasFeeTrend = "up"
	GasFeeTrendDown GasFeeTrend = "down"
)

// Get estimated CIP-1559 gas fees
type SuggestedGasFees struct {
	// Estimated values for transactions by level of urgency.
	Low, Medium, High GasFeeEstimation
	// The current estimated base fee per gas on the network.
	EstimatedBaseFee *hexutil.Big `json:"estimatedBaseFee,omitempty"`
	// The current congestion on the network, represented as a number between 0 and 1.
	// A lower network congestion score (eg., 0.1), indicates that fewer transactions are being submitted,
	// so it’s cheaper to validate transactions.
	NetworkCongestion float64 `json:"networkCongestion,omitempty"`
	// The range of priority fees per gas for recent transactions on the network.
	LatestPriorityFeeRange []*hexutil.Big `json:"latestPriorityFeeRange,omitempty"`
	// The range of priority fees per gas for transactions on the network over a historical period.
	HistoricalPriorityFeeRange []*hexutil.Big `json:"historicalPriorityFeeRange,omitempty"`
	// The range of base fees per gas on the network over a historical period.
	HistoricalBaseFeeRange []*hexutil.Big `json:"historicalBaseFeeRange,omitempty"`
	// The current trend in priority fees or base fees on the network, either up or down (whether
	// it’s getting more expensive or cheaper).
	PriorityFeeTrend GasFeeTrend `json:"priorityFeeTrend,omitempty"`
	BaseFeeTrend     GasFeeTrend `json:"baseFeeTrend,omitempty"`
}

// GasFeeEstimation represents the estimated gas fees for a transaction.
type GasFeeEstimation struct {
	// The maximum suggested priority fee per gas to pay to have transactions included in a block.
	SuggestedMaxPriorityFeePerGas *hexutil.Big
	// The maximum suggested total fee per gas to pay, including both the base fee and the priority fee.
	SuggestedMaxFeePerGas *hexutil.Big
	// The minimum and maximum estimated wait time (in milliseconds) for a transaction to be included
	// in a block at the suggested gas price.
	MinWaitTimeEstimate uint64 `json:"minWaitTimeEstimate,omitempty"`
	MaxWaitTimeEstimate uint64 `json:"maxWaitTimeEstimate,omitempty"`
}

type GasStationPrice struct {
	Fast    *hexutil.Big `json:"fast"`    // Recommended fast gas price in drip
	Fastest *hexutil.Big `json:"fastest"` // Recommended fastest gas price in drip
	SafeLow *hexutil.Big `json:"safeLow"` // Recommended safe gas price in drip
	Average *hexutil.Big `json:"average"` // Recommended average gas price in drip
}
