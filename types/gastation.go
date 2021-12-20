package types

import (
	"github.com/ethereum/go-ethereum/common/hexutil"
)

type GasStationPrice struct {
	Fast    *hexutil.Big `json:"fast"`    // Recommended fast gas price in drip
	Fastest *hexutil.Big `json:"fastest"` // Recommended fastest gas price in drip
	SafeLow *hexutil.Big `json:"safeLow"` // Recommended safe gas price in drip
	Average *hexutil.Big `json:"average"` // Recommended average gas price in drip
}
