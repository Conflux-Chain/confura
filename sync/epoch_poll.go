package monitor

import (
	"fmt"
	"strings"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

// PollEpoch polls specific epoch periodically.
func PollEpoch(cfx sdk.ClientOperator, epoch *types.Epoch) {
	interval := viper.GetInt64("sync.poll.interval")
	ticker := time.NewTicker(time.Millisecond * time.Duration(interval))
	defer ticker.Stop()

	initData, err := pollEpoch(cfx, epoch)
	if err != nil {
		panic(errors.WithMessage(err, "Failed to poll init epoch data"))
	}
	stack := newEpochStack(10000, initData)

	for range ticker.C {
		if err := stack.update(cfx, epoch); err != nil {
			panic(errors.WithMessage(err, "Failed to update"))
		}
	}
}

type epochData struct {
	number uint64
	blocks []types.Hash
}

func pollEpoch(cfx sdk.ClientOperator, epoch *types.Epoch) (*epochData, error) {
	num, err := cfx.GetEpochNumber(epoch)
	if err != nil {
		return nil, errors.WithMessagef(err, "Failed to get epoch %v", epoch)
	}

	blocks, err := cfx.GetBlocksByEpoch(types.NewEpochNumber(num))
	if err != nil {
		return nil, errors.WithMessagef(err, "Failed to get blocks for epoch %v", epoch)
	}

	return newEpochData(num.ToInt().Uint64(), blocks), nil
}

func newEpochData(epoch uint64, blocks []types.Hash) *epochData {
	if len(blocks) == 0 {
		panic(fmt.Sprintf("no block in epoch %v", epoch))
	}

	return &epochData{
		number: epoch,
		blocks: blocks,
	}
}

func (data *epochData) pivotBlock() types.Hash {
	return data.blocks[len(data.blocks)-1]
}

func (data *epochData) equals(other *epochData) bool {
	if other == nil {
		return false
	}

	if data == other {
		return true
	}

	if data.number != other.number {
		return false
	}

	return data.pivotBlock().String() == other.pivotBlock().String()
}

func (data *epochData) epoch() *types.Epoch {
	return types.NewEpochNumberUint64(data.number)
}

type epochStack struct {
	cap          int
	minEpoch     uint64
	maxEpoch     uint64
	epoch2Blocks map[uint64]*epochData
}

func newEpochStack(cap int, init *epochData) *epochStack {
	if cap < 1 {
		panic("cap is less than 1")
	}

	return &epochStack{
		cap:      cap,
		minEpoch: init.number,
		maxEpoch: init.number,
		epoch2Blocks: map[uint64]*epochData{
			init.number: init,
		},
	}
}

func (s *epochStack) update(cfx sdk.ClientOperator, epochToPoll *types.Epoch) error {
	newData, err := pollEpoch(cfx, epochToPoll)
	if err != nil {
		return errors.WithMessage(err, "Failed to poll new epoch")
	}

	curData, ok := s.peek()
	if !ok {
		panic("epoch stack is empty")
	}

	// epoch not changed
	if newData.equals(curData) {
		return nil
	}

	// revert based on common ancestor
	ancestor, err := s.findCommonAncestor(cfx)
	if err != nil {
		return errors.WithMessage(err, "Failed to find common ancestor")
	}

	prefix := fmt.Sprintf("[%v]", strings.ToUpper(epochToPoll.String()))
	reverted := ""
	if ancestor < s.maxEpoch {
		reverted = fmt.Sprintf("(reverted %v)", s.maxEpoch-ancestor-1)
	}
	missed := ""
	if newData.number-s.maxEpoch > 1 {
		missed = fmt.Sprintf("(missed %v)", newData.number-s.maxEpoch-1)
	}

	fmt.Printf("%v %v %v %v\n", prefix, newData.number, reverted, missed)

	for ancestor < s.maxEpoch {
		s.pop()
	}

	// reload data from ancestor+1 to newData.number - 1
	for i := ancestor + 1; i < newData.number; i++ {
		data, err := pollEpoch(cfx, types.NewEpochNumberUint64(i))
		if err != nil {
			return errors.WithMessage(err, "Failed to poll epoch to revert epochs")
		}

		if !s.push(data) {
			panic(fmt.Sprintf("failed to push data, topEpoch = %v, newEpoch = %v", s.maxEpoch, data.number))
		}
	}

	// double check the latest state again
	data, err := pollEpoch(cfx, newData.epoch())
	if err != nil {
		return errors.WithMessage(err, "Failed to poll epoch to double check latest state epoch")
	}

	if !data.equals(newData) {
		panic("failed to double check the new epoch data, pivot chain switched again in a very short time")
	}

	if !s.push(data) {
		panic(fmt.Sprintf("failed to push new epoch data, topEpoch = %v, newEpoch = %v", s.maxEpoch, data.number))
	}

	return nil
}

func (s *epochStack) pop() bool {
	if s.maxEpoch == 0 {
		return false
	}

	delete(s.epoch2Blocks, s.maxEpoch)

	if len(s.epoch2Blocks) == 0 {
		s.minEpoch = 0
		s.maxEpoch = 0
	} else {
		s.maxEpoch--
	}

	return true
}

func (s *epochStack) gc() bool {
	if len(s.epoch2Blocks) < s.cap {
		return false
	}

	delete(s.epoch2Blocks, s.minEpoch)
	s.minEpoch++

	return true
}

func (s *epochStack) push(data *epochData) bool {
	if len(s.epoch2Blocks) > 0 && s.maxEpoch+1 != data.number {
		return false
	}

	s.epoch2Blocks[data.number] = data
	s.maxEpoch = data.number

	if s.minEpoch == 0 {
		s.minEpoch = data.number
	}

	return true
}

func (s *epochStack) peek() (*epochData, bool) {
	if s.maxEpoch == 0 {
		return nil, false
	}

	return s.epoch2Blocks[s.maxEpoch], true
}

func (s *epochStack) findCommonAncestor(cfx sdk.ClientOperator) (uint64, error) {
	for ancestor := s.maxEpoch; ancestor >= s.minEpoch; ancestor-- {
		data, err := pollEpoch(cfx, types.NewEpochNumberUint64(ancestor))
		if err != nil {
			return 0, errors.WithMessage(err, "Failed to poll epoch for common ancestor check")
		}

		if s.epoch2Blocks[ancestor].equals(data) {
			return ancestor, nil
		}
	}

	return 0, errors.New("revert too long")
}
