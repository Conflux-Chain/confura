package store

import (
	"math/bits"
	"time"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	web3Types "github.com/openweb3/web3go/types"

	"github.com/pkg/errors"
)

const (
	// Log filter constants
	MaxLogBlockHashesSize int    = 128
	MaxLogEpochRange      uint64 = 1000
	MaxLogBlockRange      uint64 = 1000
	MaxLogLimit           uint64 = 10000 // adjust max log limit accordingly

	// Log filter types
	LogFilterTypeBlockHash  LogFilterType = 1 << iota // 0001
	LogFilterTypeEpochRange                           // 0010
	LogFilterTypeBlockRange                           // 0100
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

type LogFilterType int

func ParseLogFilterType(filter *types.LogFilter) (LogFilterType, bool) {
	// filter type set flag bitwise
	var flag LogFilterType

	// check if epoch range provided
	if filter.FromEpoch != nil || filter.ToEpoch != nil {
		flag |= LogFilterTypeEpochRange
	}

	// check if block range provided
	if filter.FromBlock != nil || filter.ToBlock != nil {
		flag |= LogFilterTypeBlockRange
	}

	// check if block hashes provided
	if len(filter.BlockHashes) != 0 {
		flag |= LogFilterTypeBlockHash
	}

	// different types of log filters are mutual exclusion
	if bits.OnesCount(uint(flag)) > 1 {
		return flag, false
	}

	// if no explicit filter type detected, use epoch range filter type as default
	if flag == 0 {
		flag |= LogFilterTypeEpochRange
	}

	return flag, true
}

func ParseEthLogFilterType(filter *web3Types.FilterQuery) (LogFilterType, bool) {
	// filter type set flag bitwise
	var flag LogFilterType

	// check if block range provided
	if filter.FromBlock != nil || filter.ToBlock != nil {
		flag |= LogFilterTypeBlockRange
	}

	// check if block hash provided
	if filter.BlockHash != nil {
		flag |= LogFilterTypeBlockHash
	}

	// different types of log filters are mutual exclusion
	if bits.OnesCount(uint(flag)) > 1 {
		return flag, false
	}

	// if no explicit filter type detected, use block range filter type as default
	if flag == 0 {
		flag |= LogFilterTypeBlockRange
	}

	return flag, true
}

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

func ParseEthLogFilter(blockFrom, blockTo uint64, filter *web3Types.FilterQuery, networkId uint32) LogFilter {
	var contracts []string
	for i := range filter.Addresses {
		// convert eth hex40 address to cfx base32 address
		addr, _ := cfxaddress.NewFromCommon(filter.Addresses[i], networkId)
		contracts = append(contracts, addr.MustGetBase32Address())
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
