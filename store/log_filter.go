package store

import "github.com/Conflux-Chain/go-conflux-sdk/types"

// DefaultLogLimit is the maximum number of logs to query at a time.
const DefaultLogLimit = 1000

// LogFilter is used to filter logs when query in any store.
type LogFilter struct {
	EpochFrom uint64
	EpochTo   uint64
	Contracts VariadicValue
	Blocks    VariadicValue
	Topics    []VariadicValue // event hash and indexed data 1, 2, 3
	Limit     int
}

// NewLogFilter creates an instance of LogFilter with specified RPC log filter.
func NewLogFilter(epochNumFrom, epochNumTo uint64, filter *types.LogFilter) LogFilter {
	result := LogFilter{
		EpochFrom: epochNumFrom,
		EpochTo:   epochNumTo,
		Contracts: newVariadicValueByAddress(filter.Address),
		Blocks:    newVariadicValueByHashes(filter.BlockHashes),
		Limit:     DefaultLogLimit,
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
	if filter.Limit != nil && uint64(*filter.Limit) < uint64(DefaultLogLimit) {
		result.Limit = int(*filter.Limit)
	}

	return result
}

// VariadicValue represents an union value, including null, single value or multiple values.
type VariadicValue struct {
	count    int
	single   string
	multiple map[string]bool
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
		return VariadicValue{1, addresses[0].String(), nil}
	}

	values := make(map[string]bool)

	for _, v := range addresses {
		values[v.String()] = true
	}

	count = len(values)
	if count == 1 {
		return VariadicValue{1, addresses[0].String(), nil}
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
