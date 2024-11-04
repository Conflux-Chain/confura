package types

import (
	"fmt"
	"math"
)

type RangeUint64 struct {
	From uint64
	To   uint64
}

func (r RangeUint64) String() string {
	return fmt.Sprintf("[%v, %v]", r.From, r.To)
}

func (r RangeUint64) ToSlice() []uint64 {
	if r.From != r.To {
		return []uint64{r.From, r.To}
	}

	return []uint64{r.From}
}

// Constant placehold for uninitialized (or unset) epoch number
const EpochNumberNil uint64 = math.MaxUint64

var EpochRangeNil RangeUint64 = RangeUint64{From: EpochNumberNil, To: EpochNumberNil}
