package store

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

// Epoch data operation type
type EpochOpType uint8

const (
	EpochOpPush EpochOpType = iota + 1
	EpochOpPop
	EpochOpDequeueBlock
	EpochOpDequeueTx
	EpochOpDequeueLog
)

// Epoch data remove option
type EpochRemoveOption uint8

const (
	EpochRemoveAll         EpochRemoveOption = 0xff
	EpochRemoveBlock                         = 0x01 << 0
	EpochRemoveTransaction                   = 0x01 << 1
	EpochRemoveLog                           = 0x01 << 2
)

// Epoch data type
type EpochDataType uint

const (
	EpochNil EpochDataType = iota
	EpochTransaction
	EpochLog
	EpochBlock
)

var (
	// custom errors
	ErrNotFound               = errors.New("not found")
	ErrUnsupported            = errors.New("not supported")
	ErrContinousEpochRequired = errors.New("continous epoch required")

	// epoch data types
	OpEpochDataTypes = []EpochDataType{
		EpochBlock,
		EpochTransaction,
		EpochLog,
	}
	// epoch data remove options
	EpochDataTypeRemoveOptionMap = map[EpochDataType]EpochRemoveOption{
		EpochBlock:       EpochRemoveBlock,
		EpochTransaction: EpochRemoveTransaction,
		EpochLog:         EpochRemoveLog,
	}
	// epoch data dequeue options
	EpochDataTypeDequeueOptionMap = map[EpochDataType]EpochOpType{
		EpochBlock:       EpochOpDequeueBlock,
		EpochTransaction: EpochOpDequeueTx,
		EpochLog:         EpochOpDequeueLog,
	}
)

// EpochDataOpAffects to record num of changes for epoch data
type EpochDataOpAffects map[EpochDataType]int64

func (affects EpochDataOpAffects) String() string {
	strBuilder := &strings.Builder{}
	strBuilder.Grow(len(affects) * 30)

	for t, v := range affects {
		strBuilder.WriteString(fmt.Sprintf("%v:%v; ", t, v))
	}

	return strBuilder.String()
}

// Merge operation history
func (affects EpochDataOpAffects) Merge(af EpochDataOpAffects) {
	for k, v := range af {
		affects[k] += v
	}
}
