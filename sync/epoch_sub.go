package sync

import (
	"fmt"
	"math/big"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// EpochSubscriber is an interface to consume subscribed epochs.
type EpochSubscriber interface {
	// any object implemented this method should handle in async mode
	onEpochReceived(epoch types.WebsocketEpochResponse)
}

// MustSubEpoch subscribes the latest mined epoch.
// Note, it will block the current thread.
func MustSubEpoch(cfx *sdk.Client, subscribers ...EpochSubscriber) {
	bufferSize := viper.GetInt("sync.sub.buffer")
	epochCh := make(chan types.WebsocketEpochResponse, bufferSize)

	sub, err := cfx.SubscribeEpochs(epochCh)
	if err != nil {
		panic(errors.WithMessage(err, "Failed to subscribe epoch"))
	}
	defer sub.Unsubscribe()

	logrus.Info("Succeed to subscribe epoch")

	for {
		select {
		case err = <-sub.Err():
			logrus.WithError(err).Fatal("Failed to subscribe epoch")
			return
		case epoch := <-epochCh:
			for _, s := range subscribers {
				s.onEpochReceived(epoch)
			}
		}
	}
}

type consoleEpochSubscriber struct {
	cfx       sdk.ClientOperator
	lastEpoch *big.Int
}

// NewConsoleEpochSubscriber creates an instance of EpochSubscriber to consume epoch.
func NewConsoleEpochSubscriber(cfx sdk.ClientOperator) EpochSubscriber {
	return &consoleEpochSubscriber{cfx, nil}
}

func (sub *consoleEpochSubscriber) onEpochReceived(epoch types.WebsocketEpochResponse) {
	latestMined, err := sub.cfx.GetEpochNumber(types.EpochLatestMined)
	if err != nil {
		fmt.Println("[ERROR] failed to get epoch number:", err.Error())
		latestMined = epoch.EpochNumber
	}

	newEpoch := epoch.EpochNumber.ToInt()

	fmt.Printf("[LATEST_MINED] %v", newEpoch)
	if latestMined.ToInt().Cmp(newEpoch) != 0 {
		fmt.Printf(" (gap %v)", subBig(newEpoch, latestMined.ToInt()))
	}

	if sub.lastEpoch != nil {
		if sub.lastEpoch.Cmp(newEpoch) >= 0 {
			fmt.Printf(" (reverted %v)", subBig(newEpoch, sub.lastEpoch))
		} else if delta := subBig(newEpoch, sub.lastEpoch); delta.Cmp(common.Big1) > 0 {
			panic(fmt.Sprintf("some epoch missed in subscription, last = %v, new = %v", sub.lastEpoch, newEpoch))
		}
	}

	fmt.Println()

	sub.lastEpoch = newEpoch
}

// func addBig(x, y *big.Int) *big.Int { return new(big.Int).Add(x, y) }
func subBig(x, y *big.Int) *big.Int { return new(big.Int).Sub(x, y) }
