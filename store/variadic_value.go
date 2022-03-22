package store

import "github.com/Conflux-Chain/go-conflux-sdk/types"

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

func (vv *VariadicValue) ToSlice() []string {
	if vv.count == 1 {
		return []string{vv.single}
	}

	result := make([]string, 0, vv.count)
	for k := range vv.multiple {
		result = append(result, k)
	}

	return result
}

func (vv *VariadicValue) Count() int {
	return vv.count
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
