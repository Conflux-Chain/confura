package store

import (
	"time"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	web3Types "github.com/openweb3/web3go/types"

	"github.com/pkg/errors"
)

const (
	// Log filter constants
	MaxLogBlockHashesSize  int    = 16
	MaxLogFilterAddrCount         = 16
	MaxLogFilterTopicCount        = 16
	MaxLogEpochRange       uint64 = 1000
	MaxLogBlockRange       uint64 = 1000
	MaxLogLimit            uint64 = 10000 // adjust max log limit accordingly
)

var (
	TimeoutGetLogs = 3 * time.Second

	ErrGetLogsQuerySetTooLarge = errors.New(
		"query set is too large, please narrow down your filter condition",
	)

	ErrGetLogsResultSetTooLarge = errors.Errorf(
		"result set to be queried is too large with more than %v logs, %v",
		MaxLogLimit, "please narrow down your filter condition",
	)

	ErrGetLogsTimeout = errors.Errorf(
		"query timeout with duration exceeds %v(s)", TimeoutGetLogs,
	)
)

type LogFilter struct {
	BlockFrom uint64
	BlockTo   uint64
	Contracts VariadicValue
	Topics    []VariadicValue // event hash and indexed data 1, 2, 3

	original interface{} // original log filter
}

// Cfx returns original core space log filter
func (f LogFilter) Cfx() *types.LogFilter {
	original, ok := f.original.(*types.LogFilter)
	if ok {
		return original
	}

	return nil
}

func ParseCfxLogFilter(blockFrom, blockTo uint64, filter *types.LogFilter) LogFilter {
	var vvs []VariadicValue

	for _, hashes := range filter.Topics {
		vvs = append(vvs, newVariadicValueByHashes(hashes))
	}

	return LogFilter{
		BlockFrom: blockFrom,
		BlockTo:   blockTo,
		Contracts: newVariadicValueByAddress(filter.Address),
		Topics:    vvs,
		original:  filter,
	}
}

// ParseEthLogFilter parses store log filter from eSpace log filter but also with contract address bridged to core space
func ParseEthLogFilter(blockFrom, blockTo uint64, filter *web3Types.FilterQuery, networkId uint32) LogFilter {
	sfilter := ParseEthLogFilterRaw(blockFrom, blockTo, filter)

	var contracts []string
	for i := range filter.Addresses {
		// convert eth hex40 address to cfx base32 address
		addr, _ := cfxaddress.NewFromCommon(filter.Addresses[i], networkId)
		contracts = append(contracts, addr.MustGetBase32Address())
	}

	sfilter.Contracts = NewVariadicValue(contracts...)
	return sfilter
}

// ParseEthLogFilterRaw parses store log filter from eSpace log filter without any bridge or mod
func ParseEthLogFilterRaw(blockFrom, blockTo uint64, filter *web3Types.FilterQuery) LogFilter {
	var contracts []string
	for _, addr := range filter.Addresses {
		contracts = append(contracts, addr.String())
	}

	var vvs []VariadicValue
	for _, topic := range filter.Topics {
		var hashes []string
		for _, hash := range topic {
			hashes = append(hashes, hash.Hex())
		}
		vvs = append(vvs, NewVariadicValue(hashes...))
	}

	return LogFilter{
		BlockFrom: blockFrom,
		BlockTo:   blockTo,
		Contracts: NewVariadicValue(contracts...),
		Topics:    vvs,
	}
}
