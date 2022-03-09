package sync

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/go-rpc-provider"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	// Sleep for a while after resub error
	resubWaitDuration time.Duration = 5 * time.Second
)

var (
	errSubAlreadyStopped = errors.New("epoch subscription already stopped")
)

// EpochSubscriber is an interface to consume subscribed epochs.
type EpochSubscriber interface {
	// any object implemented this method should handle in async mode
	onEpochReceived(epoch types.WebsocketEpochResponse)
	onEpochSubStart()
}

type epochSubChan chan types.WebsocketEpochResponse

// Epoch subscription manager
type epochSubMan struct {
	subCh       epochSubChan            // channel to receive epoch
	cfxSub      *rpc.ClientSubscription // conflux subscription stub
	cfxSdk      *sdk.Client             // conflux sdk
	subscribers []EpochSubscriber       // subscription observers
	stopped     bool                    // subscription stopped or not
}

// Create epoch subscription manager
func newEpochSubMan(cfx *sdk.Client, subscribers ...EpochSubscriber) *epochSubMan {
	bufferSize := viper.GetInt("sync.sub.buffer")

	return &epochSubMan{
		subCh:       make(epochSubChan, bufferSize),
		cfxSdk:      cfx,
		subscribers: subscribers,
		stopped:     false,
	}
}

// Start subscribing
func (subMan *epochSubMan) doSub() error {
	if subMan.stopped {
		return errSubAlreadyStopped
	}

	sub, err := subMan.cfxSdk.SubscribeEpochs(subMan.subCh, *types.EpochLatestState)
	subMan.cfxSub = sub

	return err
}

// Run subscribing to handle channel signals in block mode
func (subMan *epochSubMan) runSub(ctx context.Context) error {
	if subMan.stopped {
		return errSubAlreadyStopped
	}

	logrus.Debug("Epoch subscription starting to handle channel signals")

	// Notify all subscribers epoch sub started
	for _, s := range subMan.subscribers {
		s.onEpochSubStart()
	}

	for { // Start handling epoch subscription
		select {
		case <-ctx.Done():
			subMan.stopped = true
			return nil
		case err := <-subMan.cfxSub.Err():
			logrus.WithError(err).Error("Epoch subscription error")
			return err
		case epoch := <-subMan.subCh:
			for _, s := range subMan.subscribers {
				s.onEpochReceived(epoch)
			}
		}
	}
}

// Retry subscribing
func (subMan *epochSubMan) reSub() error {
	if subMan.stopped {
		return errSubAlreadyStopped
	}

	logrus.Debug("Epoch subscription restarting")

	subMan.close()
	return subMan.doSub()
}

// Close to reclaim resource
func (subMan *epochSubMan) close() {
	logrus.Debug("Epoch subscription closing")

	if subMan.cfxSub != nil { // unsubscribe old epoch sub
		subMan.cfxSub.Unsubscribe()
	}

	for len(subMan.subCh) > 0 { // empty channel
		<-subMan.subCh
	}
}

// MustSubEpoch subscribes the latest mined epoch.
// Note, it will block the current thread.
func MustSubEpoch(ctx context.Context, wg *sync.WaitGroup, cfx *sdk.Client, subscribers ...EpochSubscriber) {
	subMan := newEpochSubMan(cfx, subscribers...)
	if err := subMan.doSub(); err != nil {
		logrus.WithError(err).Fatal("Failed to subscribe epoch")
	}

	wg.Add(1)
	defer func() {
		subMan.close()
		wg.Done()

		logrus.Info("Epoch subscription shutdown ok")
	}()

	for {
		if err := subMan.runSub(ctx); err == nil { // blocks until sub error or stopped
			return
		}

		for err := subMan.reSub(); err != nil; { // resub until suceess or stopped
			logrus.WithError(err).Debug("Failed to resub epoch")

			tc := time.After(resubWaitDuration)
			select {
			case <-ctx.Done():
				return
			case <-tc:
				err = subMan.reSub()
			}
		}

		logrus.Warn("Epoch resub ok!")
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

func (sub *consoleEpochSubscriber) onEpochSubStart() {
	// Nothing to do for the moment (no concern)
}

// func addBig(x, y *big.Int) *big.Int { return new(big.Int).Add(x, y) }
func subBig(x, y *big.Int) *big.Int { return new(big.Int).Sub(x, y) }
