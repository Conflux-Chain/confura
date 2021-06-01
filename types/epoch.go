package types

import (
	"math"
)

// Constant placehold for uninitialized (or unset) epoch number
const EpochNumberNil uint64 = math.MaxUint64

type EpochRange struct {
	EpochFrom uint64
	EpochTo   uint64
}
