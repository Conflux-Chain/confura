package store

import (
	"fmt"
	"math"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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
	EpochDataNil EpochDataType = iota
	EpochTransaction
	EpochLog
	EpochBlock
)

var (
	// custom errors
	ErrNotFound               = errors.New("not found")
	ErrUnsupported            = errors.New("not supported")
	ErrEpochPivotSwitched     = errors.New("epoch pivot switched")
	ErrContinousEpochRequired = errors.New("continous epoch required")
	ErrAlreadyPruned          = errors.New("data already pruned")
	ErrChainReorged           = errors.New("chain re-orged")

	// operationable epoch data types
	OpEpochDataTypes = []EpochDataType{
		EpochBlock,
		EpochTransaction,
		EpochLog,
	}
)

func (edt EpochDataType) Name() string {
	switch edt {
	case EpochTransaction:
		return "tx"
	case EpochLog:
		return "log"
	case EpochBlock:
		return "block"
	}

	return "unknown"
}

func (edt EpochDataType) ToRemoveOption() EpochRemoveOption {
	switch edt {
	case EpochTransaction:
		return EpochRemoveTransaction
	case EpochLog:
		return EpochRemoveLog
	case EpochBlock:
		return EpochRemoveBlock
	default:
		logrus.WithField("name", edt.Name()).Error("remove option for data type not supported")
		return EpochRemoveOption(0x01 << 7)
	}
}

func (edt EpochDataType) ToDequeOption() EpochOpType {
	switch edt {
	case EpochTransaction:
		return EpochOpDequeueTx
	case EpochLog:
		return EpochOpDequeueLog
	case EpochBlock:
		return EpochOpDequeueBlock
	default:
		logrus.WithField("name", edt.Name()).Error("Do not to support deque option")
		return EpochOpType(math.MaxUint8)
	}
}

// EpochDataOpNumAlters to record num of alters (add or delete) for epoch data op
type EpochDataOpNumAlters map[EpochDataType]int64

// EpochDataOpAffects to record affects for epoch data op
type EpochDataOpAffects struct {
	OpType            EpochOpType          // op type
	PushUpFromEpoch   uint64               // for push op
	PushUpToEpoch     uint64               // for push op
	PopUntilEpoch     uint64               // for pop op
	DequeueUntilEpoch uint64               // for dequeue op
	NumAlters         EpochDataOpNumAlters // num of adds/deletes for epoch op
}

func NewEpochDataOpAffects(opType EpochOpType, opEpochs ...uint64) *EpochDataOpAffects {
	a := EpochDataOpAffects{
		OpType:    opType,
		NumAlters: EpochDataOpNumAlters{},
	}

	switch opType {
	case EpochOpPush:
		a.PushUpFromEpoch, a.PushUpToEpoch = opEpochs[0], opEpochs[1]
	case EpochOpPop:
		a.PopUntilEpoch = opEpochs[0]
	case EpochOpDequeueTx:
		fallthrough
	case EpochOpDequeueBlock:
		fallthrough
	case EpochOpDequeueLog:
		a.DequeueUntilEpoch = opEpochs[0]
	}

	return &a
}

func (affects EpochDataOpAffects) String() string {
	strBuilder := &strings.Builder{}
	strBuilder.Grow(len(affects.NumAlters) * 30)

	for t, v := range affects.NumAlters {
		strBuilder.WriteString(fmt.Sprintf("%v:%v;", t.Name(), v))
	}

	return strBuilder.String()
}

// Merge merges operation history into the receiver
func (affects EpochDataOpAffects) Merge(na EpochDataOpNumAlters) {
	for k, v := range na {
		affects.NumAlters[k] += v
	}
}
