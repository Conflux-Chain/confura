package store

import (
	"fmt"
	"math"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util"
)

type LogFilterType int

const (
	// Log filter constants
	MaxLogBlockHashesSize int    = 128
	MaxLogEpochRange      uint64 = 1000
	MaxLogBlockRange      uint64 = 1000
	MaxLogLimit           uint64 = math.MaxUint16 // do not limit in early phase

	// Log filter types
	LogFilterTypeBlockHashes LogFilterType = 1 << iota // 0001
	LogFilterTypeEpochRange                            // 0010
	LogFilterTypeBlockRange                            // 0100
)

// LogFilter is used to filter logs when query in any store.
type LogFilter struct {
	Type         LogFilterType
	EpochRange   *citypes.EpochRange
	BlockRange   *citypes.EpochRange
	Contracts    VariadicValue
	BlockHashIds VariadicValue
	BlockHashes  VariadicValue
	Topics       []VariadicValue // event hash and indexed data 1, 2, 3
	Limit        uint64
}

// ParseLogFilter creates an instance of LogFilter with specified RPC log filter.
func ParseLogFilter(filter *types.LogFilter) (LogFilter, bool) {
	switch {
	case filter.FromEpoch != nil && filter.ToEpoch != nil:
		for _, epoch := range [2]*types.Epoch{filter.FromEpoch, filter.ToEpoch} {
			if _, ok := epoch.ToInt(); !ok {
				return LogFilter{}, false
			}
		}
		return NewLogFilter(LogFilterTypeEpochRange, filter), true
	case filter.FromBlock != nil && filter.ToBlock != nil:
		return NewLogFilter(LogFilterTypeBlockRange, filter), true
	case len(filter.BlockHashes) > 0:
		return NewLogFilter(LogFilterTypeBlockHashes, filter), true
	}

	return LogFilter{}, false
}

// NewLogFilter creates an instance of LogFilter with specified RPC log filter.
func NewLogFilter(filterType LogFilterType, filter *types.LogFilter) LogFilter {
	result := LogFilter{
		Type:      filterType,
		Contracts: newVariadicValueByAddress(filter.Address),
		Limit:     MaxLogLimit,
	}

	switch filterType {
	case LogFilterTypeEpochRange:
		fromEpoch, ok1 := filter.FromEpoch.ToInt()
		toEpoch, ok2 := filter.ToEpoch.ToInt()

		if ok1 && ok2 {
			result.EpochRange = &citypes.EpochRange{
				EpochFrom: fromEpoch.Uint64(),
				EpochTo:   toEpoch.Uint64(),
			}
		}
	case LogFilterTypeBlockRange:

		result.BlockRange = &citypes.EpochRange{
			EpochFrom: filter.FromBlock.ToInt().Uint64(),
			EpochTo:   filter.ToBlock.ToInt().Uint64(),
		}
	case LogFilterTypeBlockHashes:
		result.BlockHashes = newVariadicValueByHashes(filter.BlockHashes)

		blockHashIds := make([]string, 0, len(filter.BlockHashes))
		for _, bh := range filter.BlockHashes {
			hashId := util.GetShortIdOfHash(bh.String())
			blockHashIds = append(blockHashIds, fmt.Sprintf("%v", hashId))
		}
		result.BlockHashIds = NewVariadicValue(blockHashIds...)
	}

	// init topics filter
	for _, v := range filter.Topics {
		result.Topics = append(result.Topics, newVariadicValueByHashes(v))
	}

	// remove empty topic filter at tail
	for len(result.Topics) > 0 && result.Topics[len(result.Topics)-1].IsNull() {
		result.Topics = result.Topics[:len(result.Topics)-1]
	}

	// init limit filter
	if filter.Limit != nil && uint64(*filter.Limit) < MaxLogLimit {
		result.Limit = uint64(*filter.Limit)
	}

	return result
}

// VariadicValue represents an union value, including null, single value or multiple values.
type VariadicValue struct {
	count    int
	single   string
	multiple map[string]bool
}

func NewVariadicValue(values ...string) VariadicValue {
	count := len(values)
	if count == 0 {
		return VariadicValue{0, "", nil}
	}

	if count == 1 {
		return VariadicValue{1, values[0], nil}
	}

	multiple := make(map[string]bool)

	for _, v := range values {
		multiple[v] = true
	}

	count = len(multiple)
	if count == 1 {
		return VariadicValue{1, values[0], nil}
	}

	return VariadicValue{count, "", multiple}
}

func newVariadicValueByHashes(hashes []types.Hash) VariadicValue {
	count := len(hashes)
	if count == 0 {
		return VariadicValue{0, "", nil}
	}

	if count == 1 {
		return VariadicValue{1, hashes[0].String(), nil}
	}

	values := make(map[string]bool)

	for _, v := range hashes {
		values[v.String()] = true
	}

	count = len(values)
	if count == 1 {
		return VariadicValue{1, hashes[0].String(), nil}
	}

	return VariadicValue{count, "", values}
}

func newVariadicValueByAddress(addresses []types.Address) VariadicValue {
	count := len(addresses)
	if count == 0 {
		return VariadicValue{0, "", nil}
	}

	if count == 1 {
		return VariadicValue{1, addresses[0].MustGetBase32Address(), nil}
	}

	values := make(map[string]bool)

	for _, v := range addresses {
		values[v.MustGetBase32Address()] = true
	}

	count = len(values)
	if count == 1 {
		return VariadicValue{1, addresses[0].MustGetBase32Address(), nil}
	}

	return VariadicValue{count, "", values}
}

func (vv *VariadicValue) IsNull() bool {
	return vv.count == 0
}

func (vv *VariadicValue) Single() (string, bool) {
	if vv.count == 1 {
		return vv.single, true
	}

	return "", false
}

func (vv *VariadicValue) FlatMultiple() ([]string, bool) {
	if vv.count < 2 {
		return nil, false
	}

	result := make([]string, 0, vv.count)

	for k := range vv.multiple {
		result = append(result, k)
	}

	return result, true
}
