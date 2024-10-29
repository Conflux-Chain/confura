package store

import (
	"fmt"
	"time"

	citypes "github.com/Conflux-Chain/confura/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	web3Types "github.com/openweb3/web3go/types"

	"github.com/pkg/errors"
)

const (
	// max number of event logs to return
	MaxLogLimit = uint64(10000)

	// max timeout to get event logs from store
	TimeoutGetLogs = 3 * time.Second
)

var ( // common errors
	errMsgLogsQuerySetTooLarge = "the query set is too large, please narrow down your filter condition"

	errMsgLogsResultSetTooLarge = fmt.Sprintf(
		"the result set exceeds the max limit of %v logs, please narrow down your filter conditions", MaxLogLimit,
	)

	ErrGetLogsTimeout = errors.Errorf(
		"the query timed out after exceeding the maximum duration of %v seconds", TimeoutGetLogs,
	)
)

var ( // Log filter constants
	MaxLogBlockHashesSize  int
	MaxLogFilterAddrCount  int
	MaxLogFilterTopicCount int

	MaxLogEpochRange uint64
	MaxLogBlockRange uint64
)

type DataSetTooLargeError struct {
	Msg            string
	SuggestedRange *citypes.RangeUint64
}

var _ error = (*DataSetTooLargeError)(nil)

func NewQuerySetTooLargeError(suggestions ...*citypes.RangeUint64) *DataSetTooLargeError {
	return NewDataSetTooLargeError(errMsgLogsQuerySetTooLarge, suggestions...)
}

func NewResultSetTooLargeError(suggestions ...*citypes.RangeUint64) *DataSetTooLargeError {
	return NewDataSetTooLargeError(errMsgLogsResultSetTooLarge, suggestions...)
}

func NewDataSetTooLargeError(msg string, suggestions ...*citypes.RangeUint64) *DataSetTooLargeError {
	var suggestion *citypes.RangeUint64
	if len(suggestions) > 0 && suggestions[0] != nil {
		suggestion = suggestions[0]
	}
	return &DataSetTooLargeError{
		Msg:            msg,
		SuggestedRange: suggestion,
	}
}

func (e *DataSetTooLargeError) Error() string {
	if e.SuggestedRange == nil {
		return e.Msg
	}
	return fmt.Sprintf("%v: suggested filter range is %s", e.Msg, *e.SuggestedRange)
}

func initLogFilter() {
	var lfc struct {
		MaxBlockHashCount int `default:"32"`
		MaxAddressCount   int `default:"32"`
		MaxTopicCount     int `default:"32"`

		MaxSplitEpochRange uint64 `default:"1000"`
		MaxSplitBlockRange uint64 `default:"1000"`
	}

	viper.MustUnmarshalKey("requestControl.logfilter", &lfc)

	MaxLogBlockHashesSize = lfc.MaxBlockHashCount
	MaxLogFilterAddrCount = lfc.MaxAddressCount
	MaxLogFilterTopicCount = lfc.MaxTopicCount

	MaxLogEpochRange = lfc.MaxSplitEpochRange
	MaxLogBlockRange = lfc.MaxSplitBlockRange
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
