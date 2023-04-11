package test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Conflux-Chain/confura/node"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/openweb3/go-rpc-provider"
	w3types "github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
	ring "github.com/zealws/golang-ring"
)

type calibrationStatus uint32

const (
	notCalibratedYet calibrationStatus = iota
	calibratedAlready
	calibrationExpired

	pubsubValidationChBufferSize = 200 // default size of pubsub validation channel buffer
	ringBufferSize               = 200 // size of ring buffer to hold data for comparision
	minCommonSubArraySize        = 5   // required minimum size of common subarray for calibration
	maxCalibrationTries          = 500 // max calibration tries in case of endless loop
)

// PSVConfig validation config provided to PubSubValidator
type PSVConfig struct {
	FullnodeRpcEndpoint string // fullnode websocket rpc endpoint used as benchmark
	InfuraRpcEndpoint   string // infura websocket rpc endpoint used to be validated against
}

// PubSubValidator subscribes to fullnode and infura websocket endpoints to receive epoch data
// of type logs/newHeads/epochs, and then compares the epoch data to validate if the infura
// pubsub results comply to fullnode.
type PubSubValidator struct{}

type pubsubFunc func() *pubsubValidationContext
type watchFunc func(calibrate, validate, reset func() error)

func (validator *PubSubValidator) doValidate(fnPubSub, infuraPubSub pubsubFunc, watch watchFunc) {
	fnCtx, infuraCtx := fnPubSub(), infuraPubSub()
	calibTryTimes, calibStatus := 0, notCalibratedYet

	reset := func() error {
		calibTryTimes = 0
		calibStatus = calibrationExpired

		fnCtx.reset()
		infuraCtx.reset()
		return nil
	}

	calibrate := func() error {
		if calibStatus == calibratedAlready {
			return nil
		}

		if validator.doCalibrate(fnCtx, infuraCtx) {
			calibStatus = calibratedAlready
			calibTryTimes = 0

			return nil
		}

		calibTryTimes++

		if calibTryTimes >= maxCalibrationTries { // in case of endless calibration
			reset()
			return errors.New("too many retries for calibration")
		}

		return nil
	}

	validate := func() error {
		if calibStatus != calibratedAlready {
			return nil
		}

		if err := validator.validateWithContext(fnCtx, infuraCtx); err != nil {
			reset()
			return err
		}

		return nil
	}

	go watch(calibrate, validate, reset)
}

func (validator *PubSubValidator) validateWithContext(fnCtx, infuraCtx *pubsubValidationContext) error {
	fnBufSize, infuraBufSize := fnCtx.rBuf.ContentSize(), infuraCtx.rBuf.ContentSize()
	logger := logrus.WithFields(logrus.Fields{
		"fnBufSize":     fnBufSize,
		"infuraBufSize": infuraBufSize,
		"ctxChType":     fnCtx.etype,
	})

	if fnBufSize == 0 || infuraBufSize == 0 {
		logger.Debug("Pubsub validator validation skipped due to no enough data")
		return nil
	}

	fnVal := fnCtx.rBuf.Peek()
	infuraVal := infuraCtx.rBuf.Peek()

	for fnVal != nil && infuraVal != nil {
		fnVal = fnCtx.rBuf.Dequeue()
		infuraVal = infuraCtx.rBuf.Dequeue()

		if !reflect.DeepEqual(fnVal, infuraVal) {
			fnValStr, _ := json.Marshal(fnVal)
			infuraValStr, _ := json.Marshal(infuraVal)

			logrus.WithFields(logrus.Fields{
				"fnVal":     fnValStr,
				"infuraVal": infuraValStr,
			}).Error("Pubsub validator validation result not matched")

			return errResultNotMatched
		}

		fnVal = fnCtx.rBuf.Peek()
		infuraVal = infuraCtx.rBuf.Peek()
	}

	logger.Debug("Pubsub validator validation passed")

	return nil
}

func (validator *PubSubValidator) doCalibrate(fnCtx, infuraCtx *pubsubValidationContext) bool {
	fnBufSize, infuraBufSize := fnCtx.rBuf.ContentSize(), infuraCtx.rBuf.ContentSize()
	minSubSize := minCommonSubArraySize

	logger := logrus.WithFields(logrus.Fields{
		"fnBufSize":     fnBufSize,
		"infuraBufSize": infuraBufSize,
	})

	if fnBufSize < minSubSize || infuraBufSize < minSubSize {
		logger.Debug("Pubsub validator calibration skipped due to no enough data")
		return false
	}

	fnArr := fnCtx.rBuf.Values()
	infuraArr := infuraCtx.rBuf.Values()
	i, j, l := findFirstCommonSubArrayOfLength(fnArr, infuraArr, minSubSize)

	logger.WithFields(logrus.Fields{
		"fnStartPos":     i,
		"infuraStartPos": j,
		"commonLength":   l,
	}).Debug("Pubsub validator found first common subarray joint points")

	// left trim elements until first non-common element from ring buffer
	if l >= minSubSize {
		for i += l; i > 0; i-- {
			fnCtx.rBuf.Dequeue()
		}

		for j += l; j > 0; j-- {
			infuraCtx.rBuf.Dequeue()
		}

		return true
	}

	return false
}

// CfxPubSubValidator core space pubsub validator
type CfxPubSubValidator struct {
	PubSubValidator

	infura sdk.ClientOperator // infura sdk client instance
	cfx    sdk.ClientOperator // fullnode sdk client instance
}

func MustNewCfxPubSubValidator(conf *PSVConfig) *CfxPubSubValidator {
	// Prepare fullnode client instance
	cfx := rpcutil.MustNewCfxClient(conf.FullnodeRpcEndpoint)
	// Prepare infura rpc service client instance
	infura := rpcutil.MustNewCfxClient(conf.InfuraRpcEndpoint)

	return &CfxPubSubValidator{cfx: cfx, infura: infura}
}

func (validator *CfxPubSubValidator) Run(ctx context.Context, wg *sync.WaitGroup) {
	logrus.Info("Pubsub validator running to validate epoch data...")

	// newHeads pubsub validation
	validator.validatePubSubNewHeads()

	// epochs pubsub validation
	validator.validatePubSubEpochs(types.EpochLatestState)
	validator.validatePubSubEpochs(types.EpochLatestMined)
}

func (validator *CfxPubSubValidator) Destroy() {
	// Close cfx client instance
	validator.cfx.Close()
	validator.infura.Close()
}

func (validator *CfxPubSubValidator) validatePubSubNewHeads() {
	var fnCh, infuraCh chan *types.BlockHeader
	var fnCtx, infuraCtx *pubsubValidationContext

	fnPubSub := func() *pubsubValidationContext { // fullnode newHeads pubsub
		fnCh = make(chan *types.BlockHeader, pubsubValidationChBufferSize)
		fnCtx = newPubSubValidationContext(fnCh)
		go validator.pubSubNewHeadsWithContext(validator.cfx, fnCtx)

		return fnCtx
	}

	infuraPubSub := func() *pubsubValidationContext { // infura newHeads pubsub
		infuraCh = make(chan *types.BlockHeader, pubsubValidationChBufferSize)
		infuraCtx = newPubSubValidationContext(infuraCh)
		go validator.pubSubNewHeadsWithContext(validator.infura, infuraCtx)

		return infuraCtx
	}

	validator.doValidate(fnPubSub, infuraPubSub, func(calibrate, validate, reset func() error) {
		for {
			var valErr error // validation error

			if err := calibrate(); err != nil {
				logrus.WithError(err).
					Error("Pubsub validator failed to calibrate newHeads pubsub")
			}

			select {
			case fnh := <-fnCh:
				fnCtx.rBuf.Enqueue(fnh)
				valErr = validate()
			case inh := <-infuraCh:
				infuraCtx.rBuf.Enqueue(inh)
				valErr = validate()
			case err := <-fnCtx.errCh:
				logrus.WithError(err).
					Error("Pubsub validator newHeads pubsub error of fullnode")
				reset()
			case err := <-infuraCtx.errCh:
				logrus.WithError(err).
					Error("Pubsub validator newHeads pubsub error of infura")
				reset()
			}

			if valErr != nil {
				logrus.WithError(valErr).
					Error("Pubsub validator failed to validate newHeads pubsub")
			}
		}
	})
}

func (validator *CfxPubSubValidator) pubSubNewHeadsWithContext(cfx sdk.ClientOperator, psvCtx *pubsubValidationContext) {
	logger := logrus.WithField("nodeUrl", cfx.GetNodeURL())

	subFunc := func() (*rpc.ClientSubscription, chan types.BlockHeader, error) {
		nhCh := make(chan types.BlockHeader, pubsubValidationChBufferSize)
		sub, err := cfx.SubscribeNewHeads(nhCh)
		return sub, nhCh, err
	}

	for {
		csub, nhCh, err := subFunc()
		if err != nil {
			logger.WithError(err).
				Error("Pubsub validator failed to do newHeads pubsub subscription")
			time.Sleep(time.Second * 2)
			continue
		}

		psvCtx.setStatus(true)

		for psvCtx.getStatus() {
			select {
			case err = <-csub.Err():
				logger.WithError(err).
					Error("Pubsub validator newHeads pubsub subscription error")
				csub.Unsubscribe()

				if psvCtx.setStatus(false) {
					psvCtx.errCh <- err
				}
			case h := <-nhCh: // newHead received from pubsub
				psvCtx.notify(&h)
			}
		}
	}
}

func (validator *CfxPubSubValidator) validatePubSubEpochs(subEpochType *types.Epoch) {
	var fnCh, infuraCh chan *types.WebsocketEpochResponse
	var fnCtx, infuraCtx *pubsubValidationContext

	fnPubSub := func() *pubsubValidationContext { // fullnode epochs pubsub
		fnCh = make(chan *types.WebsocketEpochResponse, pubsubValidationChBufferSize)
		fnCtx = newPubSubValidationContext(fnCh)
		go validator.pubSubEpochsWithContext(validator.cfx, fnCtx, subEpochType)

		return fnCtx
	}

	infuraPubSub := func() *pubsubValidationContext { // infura epochs pubsub
		infuraCh = make(chan *types.WebsocketEpochResponse, pubsubValidationChBufferSize)
		infuraCtx = newPubSubValidationContext(infuraCh)
		go validator.pubSubEpochsWithContext(validator.infura, infuraCtx, subEpochType)

		return infuraCtx
	}

	validator.doValidate(fnPubSub, infuraPubSub, func(calibrate, validate, reset func() error) {
		logger := logrus.WithField("epoch", subEpochType)
		for {
			var valErr error // validation error

			if err := calibrate(); err != nil {
				logger.WithError(err).
					Error("Pubsub validator failed to calibrate epochs pubsub")
			}

			select {
			case fnh := <-fnCh:
				fnCtx.rBuf.Enqueue(fnh)
				valErr = validate()
			case inh := <-infuraCh:
				infuraCtx.rBuf.Enqueue(inh)
				valErr = validate()
			case err := <-fnCtx.errCh:
				logger.WithError(err).
					Error("Pubsub validator epochs pubsub error of fullnode")
				reset()
			case err := <-infuraCtx.errCh:
				logger.WithError(err).
					Error("Pubsub validator epochs pubsub error of infura")
				reset()
			}

			if valErr != nil {
				logger.WithError(valErr).
					Error("Pubsub validator failed to validate epochs pubsub")
			}
		}
	})
}

func (validator *CfxPubSubValidator) pubSubEpochsWithContext(
	cfx sdk.ClientOperator,
	psvCtx *pubsubValidationContext,
	subEpochType *types.Epoch,
) {
	logger := logrus.WithFields(logrus.Fields{
		"nodeUrl":      cfx.GetNodeURL(),
		"subEpochType": subEpochType,
	})

	subFunc := func() (*rpc.ClientSubscription, chan types.WebsocketEpochResponse, error) {
		epochCh := make(chan types.WebsocketEpochResponse, pubsubValidationChBufferSize)
		sub, err := cfx.SubscribeEpochs(epochCh, *subEpochType)
		return sub, epochCh, err
	}

	for {
		csub, epochCh, err := subFunc()
		if err != nil {
			logger.WithError(err).
				Error("Pubsub validator failed to do epochs pubsub subscription")
			time.Sleep(time.Second * 2)
			continue
		}

		psvCtx.setStatus(true)

		for psvCtx.getStatus() {
			select {
			case err = <-csub.Err():
				logger.WithError(err).
					Error("Pubsub validator epochs pubsub subscription error")
				csub.Unsubscribe()

				if psvCtx.setStatus(false) {
					psvCtx.errCh <- err
				}
			case h := <-epochCh: // epoch received from pubsub
				psvCtx.notify(&h)
			}
		}
	}
}

// EthPubSubValidator evm space pubsub validator
type EthPubSubValidator struct {
	PubSubValidator

	eth    *node.Web3goClient // fullnode sdk client instance
	infura *node.Web3goClient // infura sdk client instance
}

func MustNewEthPubSubValidator(conf *PSVConfig) *EthPubSubValidator {
	// Prepare fullnode client instance
	eth := rpcutil.MustNewEthClient(conf.FullnodeRpcEndpoint)
	// Prepare infura rpc service client instance
	infura := rpcutil.MustNewEthClient(conf.InfuraRpcEndpoint)

	return &EthPubSubValidator{
		eth: &node.Web3goClient{
			Client: eth,
			URL:    conf.FullnodeRpcEndpoint,
		},
		infura: &node.Web3goClient{
			Client: infura,
			URL:    conf.InfuraRpcEndpoint,
		}}
}

func (validator *EthPubSubValidator) Run(ctx context.Context, wg *sync.WaitGroup) {
	logrus.Info("Pubsub validator running to validate epoch data...")

	// newHeads pubsub validation
	validator.validatePubSubNewHeads()
}

func (validator *EthPubSubValidator) Destroy() {
	// Close client instance
	validator.eth.Close()
	validator.infura.Close()
}

func (validator *EthPubSubValidator) validatePubSubNewHeads() {
	var fnCh, infuraCh chan *types.BlockHeader
	var fnCtx, infuraCtx *pubsubValidationContext

	fnPubSub := func() *pubsubValidationContext { // fullnode newHeads pubsub
		fnCh = make(chan *types.BlockHeader, pubsubValidationChBufferSize)
		fnCtx = newPubSubValidationContext(fnCh)
		go validator.pubSubNewHeadsWithContext(validator.eth, fnCtx)

		return fnCtx
	}

	infuraPubSub := func() *pubsubValidationContext { // infura newHeads pubsub
		infuraCh = make(chan *types.BlockHeader, pubsubValidationChBufferSize)
		infuraCtx = newPubSubValidationContext(infuraCh)
		go validator.pubSubNewHeadsWithContext(validator.infura, infuraCtx)

		return infuraCtx
	}

	validator.doValidate(fnPubSub, infuraPubSub, func(calibrate, validate, reset func() error) {
		for {
			var valErr error // validation error

			if err := calibrate(); err != nil {
				logrus.WithError(err).
					Error("Pubsub validator failed to calibrate newHeads pubsub")
			}

			select {
			case fnh := <-fnCh:
				fnCtx.rBuf.Enqueue(fnh)
				valErr = validate()
			case inh := <-infuraCh:
				infuraCtx.rBuf.Enqueue(inh)
				valErr = validate()
			case err := <-fnCtx.errCh:
				logrus.WithError(err).
					Error("Pubsub validator newHeads pubsub error of fullnode")
				reset()
			case err := <-infuraCtx.errCh:
				logrus.WithError(err).
					Error("Pubsub validator newHeads pubsub error of infura")
				reset()
			}

			if valErr != nil {
				logrus.WithError(valErr).
					Error("Pubsub validator failed to validate newHeads pubsub")
			}
		}
	})
}

func (validator *EthPubSubValidator) pubSubNewHeadsWithContext(w3c *node.Web3goClient, psvCtx *pubsubValidationContext) {
	logger := logrus.WithField("nodeUrl", w3c.URL)

	subFunc := func() (w3types.Subscription, chan *w3types.Header, error) {
		nhCh := make(chan *w3types.Header, pubsubValidationChBufferSize)

		sub, err := w3c.Eth.SubscribeNewHead(nhCh)
		return sub, nhCh, err
	}

	for {
		csub, nhCh, err := subFunc()
		if err != nil {
			logger.WithError(err).
				Error("Pubsub validator failed to do newHeads pubsub subscription")
			time.Sleep(time.Second * 2)
			continue
		}

		psvCtx.setStatus(true)

		for psvCtx.getStatus() {
			select {
			case err = <-csub.Err():
				logger.WithError(err).
					Error("Pubsub validator newHeads pubsub subscription error")
				csub.Unsubscribe()

				if psvCtx.setStatus(false) {
					psvCtx.errCh <- err
				}
			case h := <-nhCh: // newHead received from pubsub
				psvCtx.notify(&h)
			}
		}
	}
}

type pubsubValidationContext struct {
	status  int32         // pubsub status: 0 for normal, -1 for abnormal
	etype   reflect.Type  // channel type
	channel reflect.Value // channel to receive result(s)
	errCh   chan error    // channel to notify pubsub error
	rBuf    *ring.Ring    // ring buffer to hold data for comparision
}

func newPubSubValidationContext(channel interface{}) *pubsubValidationContext {
	// check type of channel first
	chanVal := reflect.ValueOf(channel)
	if chanVal.Kind() != reflect.Chan ||
		chanVal.Type().ChanDir()&reflect.SendDir == 0 || chanVal.IsNil() {
		logrus.Fatal(
			"Pubsub validation context subscription channel must be a writable channel and not be nil",
		)
	}

	rbuf := &ring.Ring{}
	rbuf.SetCapacity(ringBufferSize)

	return &pubsubValidationContext{
		etype:   chanVal.Type().Elem(),
		channel: chanVal,
		errCh:   make(chan error, 1),
		rBuf:    rbuf,
	}
}

func (ctx *pubsubValidationContext) reset() {
	ctx.drainRead()
	ctx.resetBuf()
}

func (ctx *pubsubValidationContext) resetBuf() {
	rbuf := &ring.Ring{}
	rbuf.SetCapacity(ringBufferSize)
	ctx.rBuf = rbuf
}

func (ctx *pubsubValidationContext) drainRead() {
	cases := []reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: ctx.channel},
		{Dir: reflect.SelectDefault},
	}

	for {
		index, _, _ := reflect.Select(cases)
		if index != 0 {
			break
		}
	}
}

func (ctx *pubsubValidationContext) setStatus(ok bool) bool {
	var expect, set int32 = -1, 0
	if !ok {
		expect, set = 0, -1
	}

	return atomic.CompareAndSwapInt32(&ctx.status, expect, set)
}

func (ctx *pubsubValidationContext) getStatus() bool {
	return atomic.LoadInt32(&ctx.status) == 0
}

func (ctx *pubsubValidationContext) notify(result interface{}) bool {
	cases := []reflect.SelectCase{
		{Dir: reflect.SelectSend, Chan: ctx.channel, Send: reflect.ValueOf(result)},
		{Dir: reflect.SelectDefault},
	}

	switch index, _, _ := reflect.Select(cases); index {
	case 0: // sub.channel<-
		return true
	case 1: // never blocking for subscription queue overflow
		ctx.errCh <- rpc.ErrSubscriptionQueueOverflow
		return false
	}

	return false
}

// findFirstCommonSubArrayOfLength finds starting position of the first common subarray with at least
// specified length between two slice using brute force algorithm.
// This is a typical LCS(longest common subarray) problem, the time complexicity for this brute force
// algorithm is O(M*N*min(M,N)) where M,N are the lengths of both slices.
//
// TODO Improve the performance using "Binary Search with Rolling Hash" algorithm if necessary,
// which can be referenced to:
// https://massivealgorithms.blogspot.com/2018/12/leetcode-718-maximum-length-of-repeated.html
func findFirstCommonSubArrayOfLength(a1, a2 []interface{}, length int) (i, j, l int) {
	for i := 0; i < len(a1); i++ {
		for j := 0; j < len(a2); j++ {
			k := 0
			for (i+k) < len(a1) && (j+k) < len(a2) {
				if !reflect.DeepEqual(a1[i+k], a2[j+k]) {
					break
				}
				k++
			}

			if k >= length {
				return i, j, k
			}
		}
	}

	return 0, 0, 0
}
