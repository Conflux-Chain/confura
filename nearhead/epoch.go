package nearhead

import (
	"fmt"
	"math/big"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/pkg/errors"
)

var epochGapStateMined = big.NewInt(4)

// LatestEpochData wraps the latest epoch numbers for checkpoint, confirmed, state and mined.
type LatestEpochData struct {
	Checkpoint *big.Int
	Confirmed  *big.Int
	State      *big.Int
	Mined      *big.Int
}

func newLatestEpochData(cfx sdk.ClientOperator) (LatestEpochData, error) {
	epoch, err := queryEpochNumber(cfx, types.EpochLatestState)
	if err != nil {
		return LatestEpochData{}, err
	}

	return newLatestEpochDataWithStated(cfx, epoch)
}

func newLatestEpochDataWithStated(cfx sdk.ClientOperator, latestStated *big.Int) (LatestEpochData, error) {
	var err error

	data := LatestEpochData{
		State: latestStated,
		Mined: new(big.Int).Add(latestStated, epochGapStateMined),
	}

	if data.Confirmed, err = queryEpochNumber(cfx, types.EpochLatestConfirmed); err != nil {
		return LatestEpochData{}, err
	}

	if data.Checkpoint, err = queryEpochNumber(cfx, types.EpochLatestCheckpoint); err != nil {
		return LatestEpochData{}, err
	}

	return data, nil
}

func queryEpochNumber(cfx sdk.ClientOperator, epoch *types.Epoch) (*big.Int, error) {
	num, err := cfx.GetEpochNumber(epoch)
	if err != nil {
		return nil, errors.WithMessagef(err, "Failed to get epoch number for %v", epoch.String())
	}

	return num.ToInt(), nil
}

// ToInt returns the epoch number for the specified epoch if any.
func (data *LatestEpochData) ToInt(epoch *types.Epoch) (*big.Int, bool) {
	if epoch == nil {
		return nil, false
	}

	if num, ok := epoch.ToInt(); ok {
		return num, true
	}

	if types.EpochLatestCheckpoint.Equals(epoch) {
		return data.Checkpoint, true
	}

	if types.EpochLatestConfirmed.Equals(epoch) {
		return data.Confirmed, true
	}

	if types.EpochLatestState.Equals(epoch) {
		return data.State, true
	}

	if types.EpochLatestMined.Equals(epoch) {
		return data.Mined, true
	}

	return nil, false
}

// String implements the fmt.Stringer interface.
func (data LatestEpochData) String() string {
	return fmt.Sprintf("Epochs{checkpoint = %v (%v), confirmed = %v (%v), state = %v (%v), mined = %v}",
		data.Checkpoint, new(big.Int).Sub(data.Mined, data.Checkpoint),
		data.Confirmed, new(big.Int).Sub(data.Mined, data.Confirmed),
		data.State, new(big.Int).Sub(data.Mined, data.State),
		data.Mined)
}
