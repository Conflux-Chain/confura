package store

import (
	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

type VariadicValuer interface {
	IsNull() bool
	Count() int
	Values() []any
}

func ToVariadicValuers[T comparable](vals ...VariadicValue[T]) []VariadicValuer {
	res := make([]VariadicValuer, 0, len(vals))
	for i := range vals {
		res = append(res, &vals[i])
	}
	return res
}

// VariadicValue represents an union value, including null, single value or multiple values.
type VariadicValue[T comparable] struct {
	count    int
	single   T
	multiple map[T]bool
}

func NewVariadicValue[T comparable](values ...T) VariadicValue[T] {
	var zero T

	count := len(values)
	if count == 0 {
		return VariadicValue[T]{0, zero, nil}
	}

	if count == 1 {
		return VariadicValue[T]{1, values[0], nil}
	}

	multiple := make(map[T]bool)

	for _, v := range values {
		multiple[v] = true
	}

	count = len(multiple)
	if count == 1 {
		return VariadicValue[T]{1, values[0], nil}
	}

	return VariadicValue[T]{count, zero, multiple}
}

func newVariadicValueByHashes(hashes []types.Hash) VariadicValue[string] {
	count := len(hashes)
	if count == 0 {
		return VariadicValue[string]{0, "", nil}
	}

	if count == 1 {
		return VariadicValue[string]{1, hashes[0].String(), nil}
	}

	values := make(map[string]bool)

	for _, v := range hashes {
		values[v.String()] = true
	}

	count = len(values)
	if count == 1 {
		return VariadicValue[string]{1, hashes[0].String(), nil}
	}

	return VariadicValue[string]{count, "", values}
}

func newVariadicValueByAddress(addresses []types.Address) VariadicValue[string] {
	count := len(addresses)
	if count == 0 {
		return VariadicValue[string]{0, "", nil}
	}

	if count == 1 {
		return VariadicValue[string]{1, addresses[0].MustGetBase32Address(), nil}
	}

	values := make(map[string]bool)

	for _, v := range addresses {
		values[v.MustGetBase32Address()] = true
	}

	count = len(values)
	if count == 1 {
		return VariadicValue[string]{1, addresses[0].MustGetBase32Address(), nil}
	}

	return VariadicValue[string]{count, "", values}
}

func (vv *VariadicValue[T]) ToSlice() []T {
	if vv.count == 1 {
		return []T{vv.single}
	}

	result := make([]T, 0, vv.count)
	for k := range vv.multiple {
		result = append(result, k)
	}

	return result
}

func (vv VariadicValue[T]) Count() int {
	return vv.count
}

func (vv VariadicValue[T]) IsNull() bool {
	return vv.count == 0
}

func (vv *VariadicValue[T]) Single() (v T, _ bool) {
	if vv.count == 1 {
		return vv.single, true
	}

	return v, false
}

func (vv *VariadicValue[T]) FlatMultiple() ([]T, bool) {
	if vv.count < 2 {
		return nil, false
	}

	result := make([]T, 0, vv.count)

	for k := range vv.multiple {
		result = append(result, k)
	}

	return result, true
}

func (vv VariadicValue[T]) Values() []any {
	vals := vv.ToSlice()
	if len(vals) == 0 {
		return nil
	}

	res := make([]any, 0, len(vals))
	for _, v := range vals {
		res = append(res, v)
	}

	return res
}
