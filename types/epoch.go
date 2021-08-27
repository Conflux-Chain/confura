package types

import (
	"errors"
	"math"
)

// Constant placehold for uninitialized (or unset) epoch number
const EpochNumberNil uint64 = math.MaxUint64

var EpochRangeNil EpochRange = EpochRange{
	EpochNumberNil, EpochNumberNil,
}

type EpochRange struct {
	EpochFrom uint64
	EpochTo   uint64
}

func (e *EpochRange) ToSlice() []uint64 {
	res := make([]uint64, 0, 2)

	res = append(res, e.EpochFrom)
	if e.EpochFrom != e.EpochTo {
		res = append(res, e.EpochTo)
	}

	return res
}

func ValidateEpochRange(er *EpochRange) error {
	if er.EpochFrom > er.EpochTo {
		return errors.New("from epoch is greater than to epoch epoch")
	}

	return nil
}
