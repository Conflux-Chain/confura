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
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
	ring "github.com/zealws/golang-ring"
)

const (
	// default channel buffer size for virtual filter validation
	vfValidChBufferSize = 100
)

// VFValidConfig validation config for virtual filter
type VFValidConfig struct {
	FullnodeRpcEndpoint string // fullnode rpc endpoint used as benchmark
	InfuraRpcEndpoint   string // infura rpc endpoint used to be validated against
}

// VFValidator virtual filter validator base to poll filter changes from fullnode and infura
// endpoints, and then compares the filter change to validate if the poll results comply to
// fullnode.
type VFValidator struct{ tasks *validatorTasks }

type vfFunc func() *vfValidationContext

func (validator *VFValidator) doValidate(fnFilter, infuraFilter vfFunc, watch watchFunc) {
	fnCtx, infuraCtx := fnFilter(), infuraFilter()
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

	validator.tasks.start(func() { watch(calibrate, validate, reset) })
}

func (validator *VFValidator) validateWithContext(fnCtx, infuraCtx *vfValidationContext) error {
	fnBufSize, infuraBufSize := fnCtx.rBuf.ContentSize(), infuraCtx.rBuf.ContentSize()
	logger := logrus.WithFields(logrus.Fields{
		"fnBufSize":     fnBufSize,
		"infuraBufSize": infuraBufSize,
		"ctxChType":     fnCtx.etype,
	})

	if fnBufSize == 0 || infuraBufSize == 0 {
		logger.Debug("Virtual filter validation skipped due to no enough data")
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
				"fnVal":     string(fnValStr),
				"infuraVal": string(infuraValStr),
			}).Error("Virtual filter validation result not matched")

			return errResultNotMatched
		}

		fnVal = fnCtx.rBuf.Peek()
		infuraVal = infuraCtx.rBuf.Peek()
	}

	logger.Debug("Virtual filter validation passed")

	return nil
}

func (validator *VFValidator) doCalibrate(fnCtx, infuraCtx *vfValidationContext) bool {
	fnBufSize, infuraBufSize := fnCtx.rBuf.ContentSize(), infuraCtx.rBuf.ContentSize()
	minSubSize := minCommonSubArraySize

	logger := logrus.WithFields(logrus.Fields{
		"fnBufSize":     fnBufSize,
		"infuraBufSize": infuraBufSize,
	})

	if fnBufSize < minSubSize || infuraBufSize < minSubSize {
		logger.Debug("Virtual filter validator calibration skipped due to no enough data")
		return false
	}

	fnArr := fnCtx.rBuf.Values()
	infuraArr := infuraCtx.rBuf.Values()
	i, j, l := findFirstCommonSubArrayOfLength(fnArr, infuraArr, minSubSize)

	logger.WithFields(logrus.Fields{
		"fnStartPos":     i,
		"infuraStartPos": j,
		"commonLength":   l,
	}).Debug("Virtual filter validator searching first common subarray joint points...")

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

// EthVFValidator evm space virtual filter test validator
type EthVFValidator struct {
	VFValidator
	infura *node.Web3goClient // infura rpc service client instance
	fn     *node.Web3goClient // fullnode client instance
}

func NewEthVFValidator(conf *VFValidConfig) (*EthVFValidator, error) {
	client, err := rpcutil.NewEthClient(conf.FullnodeRpcEndpoint)
	if err != nil {
		return nil, err
	}
	infura, err := rpcutil.NewEthClient(conf.InfuraRpcEndpoint)
	if err != nil {
		client.Close()
		return nil, err
	}
	return &EthVFValidator{VFValidator: VFValidator{tasks: newValidatorTasks()}, fn: &node.Web3goClient{Client: client, URL: conf.FullnodeRpcEndpoint}, infura: &node.Web3goClient{Client: infura, URL: conf.InfuraRpcEndpoint}}, nil
}

func MustNewEthVFValidator(conf *VFValidConfig) *EthVFValidator {
	v, err := NewEthVFValidator(conf)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize validator")
	}
	return v
}

func (validator *EthVFValidator) Run(ctx context.Context, wg *sync.WaitGroup) {
	validator.tasks.run(ctx, wg, func() {

		validator.validateFilterChanges()

	})
}

func (validator *EthVFValidator) Destroy() {
	validator.tasks.destroyOnce.Do(func() {
		validator.tasks.stop()

		// Close client instance
		validator.fn.Close()
		validator.infura.Close()

	})
}

func (validator *EthVFValidator) validateFilterChanges() {
	var fnCh, infuraCh chan []types.Log
	var fnCtx, infuraCtx *vfValidationContext

	fnFilter := func() *vfValidationContext { // fullnode
		fnCh = make(chan []types.Log, vfValidChBufferSize)
		fnCtx = newVFValidationContext(fnCh)
		validator.tasks.start(func() { validator.pollFilterChangesWithContext(validator.fn, fnCtx) })

		return fnCtx
	}

	infuraFilter := func() *vfValidationContext { // infura
		infuraCh = make(chan []types.Log, vfValidChBufferSize)
		infuraCtx = newVFValidationContext(infuraCh)
		validator.tasks.start(func() { validator.pollFilterChangesWithContext(validator.infura, infuraCtx) })

		return infuraCtx
	}

	validator.doValidate(fnFilter, infuraFilter, func(calibrate, validate, reset func() error) {
		for {
			if err := calibrate(); err != nil {
				logrus.WithError(err).Error("Virtual filter validator failed to calibrate filter changes")
			}

			var valErr error // validation error

			select {
			case <-validator.tasks.ctx.Done():
				return
			case fnlogs := <-fnCh:
				for i := range fnlogs {
					fnCtx.rBuf.Enqueue(&fnlogs[i])
				}
				valErr = validate()
			case inflogs := <-infuraCh:
				for i := range inflogs {
					infuraCtx.rBuf.Enqueue(&inflogs[i])
				}
				valErr = validate()
			case <-fnCtx.errCh:
				reset()
			case <-infuraCtx.errCh:
				reset()
			}

			if valErr != nil {
				logrus.WithError(valErr).Error("Virtual filter validation error")
			}
		}
	})
}

func (validator *EthVFValidator) pollFilterChangesWithContext(w3c *node.Web3goClient, vfCtx *vfValidationContext) {
	logger := logrus.WithField("nodeUrl", w3c.URL)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for validator.tasks.ctx.Err() == nil {
		fid, err := w3c.WithContext(validator.tasks.ctx).Filter.NewLogFilter(&types.FilterQuery{})
		if err == nil && fid == nil {
			err = errors.New("node returned a null filter ID")
		}
		if err != nil {
			logger.WithField("fid", fid).WithError(err).Error("Virtual filter validator failed to new log filter")
			if !validator.tasks.waitRetry(time.Second) {
				return
			}
			continue
		}

		vfCtx.setStatus(true)

		func() {
			defer func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				_, _ = w3c.WithContext(cleanupCtx).Filter.UninstallFilter(*fid)
			}()
			for {
				select {
				case <-validator.tasks.ctx.Done():
					return
				case <-ticker.C:
				}
				if !vfCtx.getStatus() {
					break
				}

				fc, err := w3c.WithContext(validator.tasks.ctx).Filter.GetFilterChanges(*fid)
				if err == nil && fc != nil {
					vfCtx.notify(fc.Logs)
					continue
				}

				logger.WithField("fid", fid).WithError(err).Error("Virtual filter validator failed to poll changes")

				if vfCtx.setStatus(false) {
					select {
					case vfCtx.errCh <- err:
					default:
					}
				}
			}
		}()
	}
}

type CfxVFValidator struct {
	VFValidator
	infura *sdk.Client // infura rpc service client instance
	fn     *sdk.Client // fullnode client instance
}

func NewCfxVFValidator(conf *VFValidConfig) (*CfxVFValidator, error) {
	client, err := rpcutil.NewCfxClient(conf.FullnodeRpcEndpoint)
	if err != nil {
		return nil, err
	}
	infura, err := rpcutil.NewCfxClient(conf.InfuraRpcEndpoint)
	if err != nil {
		client.Close()
		return nil, err
	}
	return &CfxVFValidator{VFValidator: VFValidator{tasks: newValidatorTasks()}, fn: client, infura: infura}, nil
}

func MustNewCfxVFValidator(conf *VFValidConfig) *CfxVFValidator {
	v, err := NewCfxVFValidator(conf)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize validator")
	}
	return v
}

func (validator *CfxVFValidator) Run(ctx context.Context, wg *sync.WaitGroup) {
	validator.tasks.run(ctx, wg, func() {

		validator.validateFilterChanges()

	})
}

func (validator *CfxVFValidator) Destroy() {
	validator.tasks.destroyOnce.Do(func() {
		validator.tasks.stop()

		// Close client instance
		validator.fn.Close()
		validator.infura.Close()

	})
}

func (validator *CfxVFValidator) validateFilterChanges() {
	var fnCh, infuraCh chan []*cfxtypes.SubscriptionLog
	var fnCtx, infuraCtx *vfValidationContext

	fnFilter := func() *vfValidationContext { // fullnode
		fnCh = make(chan []*cfxtypes.SubscriptionLog, vfValidChBufferSize)
		fnCtx = newVFValidationContext(fnCh)
		validator.tasks.start(func() { validator.pollFilterChangesWithContext(validator.fn, fnCtx) })

		return fnCtx
	}

	infuraFilter := func() *vfValidationContext { // infura
		infuraCh = make(chan []*cfxtypes.SubscriptionLog, vfValidChBufferSize)
		infuraCtx = newVFValidationContext(infuraCh)
		validator.tasks.start(func() { validator.pollFilterChangesWithContext(validator.infura, infuraCtx) })

		return infuraCtx
	}

	validator.doValidate(fnFilter, infuraFilter, func(calibrate, validate, reset func() error) {
		for {
			if err := calibrate(); err != nil {
				logrus.WithError(err).Error("Virtual filter validator failed to calibrate filter changes")
			}

			var valErr error // validation error

			select {
			case <-validator.tasks.ctx.Done():
				return
			case fnlogs := <-fnCh:
				for i := range fnlogs {
					fnCtx.rBuf.Enqueue(&fnlogs[i])
				}
				valErr = validate()
			case inflogs := <-infuraCh:
				for i := range inflogs {
					infuraCtx.rBuf.Enqueue(&inflogs[i])
				}
				valErr = validate()
			case <-fnCtx.errCh:
				reset()
			case <-infuraCtx.errCh:
				reset()
			}

			if valErr != nil {
				logrus.WithError(valErr).Error("Virtual filter validation error")
			}
		}
	})
}

func (validator *CfxVFValidator) pollFilterChangesWithContext(client *sdk.Client, vfCtx *vfValidationContext) {
	logger := logrus.WithField("nodeUrl", client.GetNodeURL())

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for validator.tasks.ctx.Err() == nil {
		fid, err := client.WithContext(validator.tasks.ctx).Filter().NewFilter(cfxtypes.LogFilter{})
		if err == nil && fid == nil {
			err = errors.New("node returned a null filter ID")
		}
		if err != nil {
			logger.WithField("fid", fid).WithError(err).Error("Virtual filter validator failed to new log filter")
			if !validator.tasks.waitRetry(time.Second) {
				return
			}
			continue
		}

		vfCtx.setStatus(true)

		func() {
			defer func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				_, _ = client.WithContext(cleanupCtx).Filter().UninstallFilter(*fid)
			}()
			for {
				select {
				case <-validator.tasks.ctx.Done():
					return
				case <-ticker.C:
				}
				if !vfCtx.getStatus() {
					break
				}

				fc, err := client.WithContext(validator.tasks.ctx).Filter().GetFilterChanges(*fid)
				if err == nil && fc != nil {
					vfCtx.notify(fc.Logs)
					continue
				}

				logger.WithField("fid", fid).WithError(err).Error("Virtual filter validator failed to poll changes")

				if vfCtx.setStatus(false) {
					select {
					case vfCtx.errCh <- err:
					default:
					}
				}
			}
		}()
	}
}

type vfValidationContext struct {
	status  int32         // validation status: 0 for normal, -1 for abnormal
	etype   reflect.Type  // channel type
	channel reflect.Value // channel to receive result(s)
	errCh   chan error    // channel to notify validation error
	rBuf    *ring.Ring    // ring buffer to hold data for comparison
}

func newVFValidationContext(channel interface{}) *vfValidationContext {
	// check type of channel first
	chanVal := reflect.ValueOf(channel)
	if chanVal.Kind() != reflect.Chan ||
		chanVal.Type().ChanDir()&reflect.SendDir == 0 || chanVal.IsNil() {
		logrus.Fatal(
			"Virtual filter validation context subscription channel must be a writable channel and not be nil",
		)
	}

	rbuf := &ring.Ring{}
	rbuf.SetCapacity(ringBufferSize)

	return &vfValidationContext{
		etype:   chanVal.Type().Elem(),
		channel: chanVal,
		errCh:   make(chan error, 1),
		rBuf:    rbuf,
	}
}

func (ctx *vfValidationContext) reset() {
	ctx.drainRead()
	ctx.resetBuf()
}

func (ctx *vfValidationContext) resetBuf() {
	rbuf := &ring.Ring{}
	rbuf.SetCapacity(ringBufferSize)
	ctx.rBuf = rbuf
}

func (ctx *vfValidationContext) drainRead() {
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

func (ctx *vfValidationContext) setStatus(ok bool) bool {
	var expect, set int32 = -1, 0
	if !ok {
		expect, set = 0, -1
	}

	return atomic.CompareAndSwapInt32(&ctx.status, expect, set)
}

func (ctx *vfValidationContext) getStatus() bool {
	return atomic.LoadInt32(&ctx.status) == 0
}

func (ctx *vfValidationContext) notify(result interface{}) bool {
	cases := []reflect.SelectCase{
		{Dir: reflect.SelectSend, Chan: ctx.channel, Send: reflect.ValueOf(result)},
		{Dir: reflect.SelectDefault},
	}

	switch index, _, _ := reflect.Select(cases); index {
	case 0: // sub.channel<-
		return true
	case 1: // never blocking for queue overflow
		select {
		case ctx.errCh <- errors.New("buffer queue overflow"):
		default:
		}
		return false
	}

	return false
}
