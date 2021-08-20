package test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"io/ioutil"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"
)

const (
	samplingValidationRetries       = 2                      // number of retries on sampling validation failed
	samplingValidationSleepDuration = time.Millisecond * 300 // sleeping duration before each retry
)

var (
	validEpochFromNoFilePath = ".evno" // file path to read/write epoch number from where the validation will start

	errResultNotMatched = errors.New("results not matched")
)

type epochValidationFunc func(epoch *types.Epoch) error

// EVConfig validation config provided to EpochValidator
type EVConfig struct {
	FullnodeRpcEndpoint string        // Fullnode rpc endpoint used as benchmark
	InfuraRpcEndpoint   string        // Infura rpc endpoint used to be validated against
	EpochScanFrom       uint64        // the epoch scan from
	ScanInterval        time.Duration // scan interval
	SamplingInterval    time.Duration // sampling interval
	SamplingEpochType   string        // sampling epoch type: lm(latest_mined), ls(latest_state), lc(latest_confirmed)
}

// EpochValidator pulls epoch data from fullnode and infura endpoints, and then compares the
// epoch data to validate if the infura epoch data complies to fullnode.
type EpochValidator struct {
	infura        sdk.ClientOperator // infura rpc service client instance
	cfx           sdk.ClientOperator // fullnode client instance
	conf          *EVConfig          // validation configuration
	samplingEpoch *types.Epoch       // the epoch type for sampling validatioin
}

func init() {
	if len(os.Getenv("HOME")) > 0 {
		validEpochFromNoFilePath = fmt.Sprintf("%v/.evno", os.Getenv("HOME"))
	}
}

func MustNewEpochValidator(conf *EVConfig) *EpochValidator {
	// Prepare fullnode client instance
	cfx := util.MustNewCfxClient(conf.FullnodeRpcEndpoint)
	// Prepare infura rpc service client instance
	infura := util.MustNewCfxClient(conf.InfuraRpcEndpoint)

	validator := &EpochValidator{
		cfx:    cfx,
		infura: infura,
		conf:   conf,
	}

	switch strings.ToLower(conf.SamplingEpochType) {
	case "lm":
		fallthrough
	case "latest_mined":
		validator.samplingEpoch = types.EpochLatestMined
	case "ls":
		fallthrough
	case "latest_state":
		validator.samplingEpoch = types.EpochLatestState
	default:
		validator.samplingEpoch = types.EpochLatestConfirmed
	}

	defer logrus.WithFields(logrus.Fields{"config": conf, "samplingEpoch": validator.samplingEpoch}).Info("Test validation configurations loaded.")

	if conf.EpochScanFrom != math.MaxUint64 {
		return validator
	}

	conf.EpochScanFrom = 1 // default scan from epoch #1

	// Read last validated epoch from config file, on which the validation will continue
	dat, err := ioutil.ReadFile(validEpochFromNoFilePath)
	if err != nil {
		logrus.WithError(err).Debugf("Epoch validator failed to load last validated epoch from file %v", validEpochFromNoFilePath)
		return validator
	}

	datStr := strings.TrimSpace(string(dat))
	epochFrom, err := strconv.ParseUint(datStr, 10, 64)
	if err == nil {
		logrus.WithField("epochFrom", epochFrom).Infof("Epoch validator loaded last validated epoch from file %v", validEpochFromNoFilePath)
		conf.EpochScanFrom = epochFrom
	} else {
		logrus.WithError(err).Debugf("Epoch validator failed to load last validated epoch from file %v", validEpochFromNoFilePath)
	}

	return validator
}

func (validator *EpochValidator) Run(ctx context.Context, wg *sync.WaitGroup) {
	logrus.WithField("epochFrom", validator.conf.EpochScanFrom).Info("Epoch validator running to validate epoch data...")

	wg.Add(1)
	defer wg.Done()

	logger := logrus.WithFields(logrus.Fields{
		"fullNodeUrl": validator.cfx.GetNodeURL(),
		"infuraUrl":   validator.infura.GetNodeURL(),
	})

	// Randomly sampling nearhead epoch for validation. Nearhead epochs are very likely to be reverted
	// due to pivot switch, it's acceptable to just trigger a warnning once the validation failed.
	samplingTicker := time.NewTicker(validator.conf.SamplingInterval)
	defer samplingTicker.Stop()

	// Sequentially scaning epochs from the earliest epoch to latest confirmed epoch. Confirmed epochs are
	// rerely reverted, an error/panic will be triggered once validation failed.
	scanTicker := time.NewTicker(validator.conf.ScanInterval)
	defer scanTicker.Stop()

	scanValidFailures := 0    // mark how many failure times for scanning validation
	maxScanValidFailures := 5 // the maximum failure times for scanning validation, once exceeded, panic will be triggered

	for {
		select {
		case <-ctx.Done():
			logrus.Info("Epoch validator shutdown ok")
			return
		case <-samplingTicker.C:
			go func() { // randomly pick some nearhead epoch for validation test
				if err := validator.doSampling(); err != nil {
					logger.WithError(err).Error("Epoch validator failed to do samping for epoch validation")
				}
			}()
		case <-scanTicker.C:
			if err := validator.doScanning(scanTicker); err != nil {
				scanValidFailures++

				errFunc := logger.WithError(err).Error
				if scanValidFailures >= maxScanValidFailures {
					errFunc = logger.WithError(err).Fatal
					validator.saveScanCursor()
				}

				errFunc("Epoch validator Failed to do scanning for epoch validation")
				continue
			}

			scanValidFailures = 0 // clear scanning validation failure times
		}
	}
}

func (validator *EpochValidator) doSampling() error {
	logrus.Debug("Epoch validator ticking to sample nearhead epoch for validation...")

	// Fetch latest epoch number from blockchain
	epoch, err := validator.cfx.GetEpochNumber(validator.samplingEpoch)
	if err != nil {
		return errors.WithMessagef(err, "failed to query %v", validator.samplingEpoch)
	}

	// Shuffle epoch number by reduction of random number less than 100
	latestEpochNo := epoch.ToInt().Uint64()

	randomDiff := util.RandUint64(100)
	if latestEpochNo < randomDiff {
		randomDiff = 0
	}

	epochNo := latestEpochNo - randomDiff

	logrus.WithFields(logrus.Fields{
		"latestEpochNo": latestEpochNo, "shuffledEpochNo": epochNo,
	}).Debug("Epoch validator sampled random nearhead epoch for validation")

	// Validate the new shuffled epoch number
	epochVFuncs := []epochValidationFunc{
		validator.validateGetBlockSummaryByEpoch,
		validator.validateGetBlockByEpoch,
		validator.validateGetBlocksByEpoch,
		validator.validateEpochCombo,
		validator.validateGetLogs,
	}

	err = validator.doRun(epochNo, epochVFuncs...)
	// Since nearhead epoch re-orgs are of high possibility due to pivot switch,
	// we'd better do some retrying before determining the final validation result.
	for i := 0; i < samplingValidationRetries && errors.Is(err, errResultNotMatched); i++ {
		time.Sleep(samplingValidationSleepDuration)
		err = validator.doRun(epochNo, epochVFuncs...)
		logrus.WithField("epoch", epochNo).WithError(err).Infof("Epoch validator sampling validation retried %v time(s)", i+1)
	}

	return errors.WithMessagef(err, "failed to validate epoch #%v", epochNo)
}

func (validator *EpochValidator) doScanning(ticker *time.Ticker) error {
	logrus.WithField("epochFrom", validator.conf.EpochScanFrom).Debug("Epoch validation ticking to scan for validation...")

	_, err := validator.scanOnce()
	return err
}

func (validator *EpochValidator) scanOnce() (bool, error) {
	// Fetch latest confirmed epoch from blockchain
	epoch, err := validator.cfx.GetEpochNumber(types.EpochLatestConfirmed)
	if err != nil {
		return false, errors.WithMessage(err, "failed to query the latest confirmed epoch")
	}

	// Already catch up to the latest confirmed epoch
	maxEpochTo := epoch.ToInt().Uint64()
	if validator.conf.EpochScanFrom > maxEpochTo {
		logrus.WithField("epochRange", citypes.EpochRange{
			EpochFrom: validator.conf.EpochScanFrom,
			EpochTo:   maxEpochTo,
		}).Debug("Epoch validator scaning skipped due to catched up already")

		return true, nil
	}

	// Validate the scanned epoch
	epochVFuncs := []epochValidationFunc{
		validator.validateGetBlockSummaryByEpoch,
		validator.validateGetBlockByEpoch,
		validator.validateGetBlocksByEpoch,
		validator.validateEpochCombo,
		validator.validateGetLogs,
	}
	if err := validator.doRun(validator.conf.EpochScanFrom, epochVFuncs...); err != nil {
		return false, errors.WithMessagef(err, "failed to validate epoch #%v", validator.conf.EpochScanFrom)
	}

	validator.conf.EpochScanFrom++

	if validator.conf.EpochScanFrom%5000 == 0 { // periodly save the scaning progress per 5000 epochs in case of data lost
		validator.saveScanCursor()
	}

	return false, nil
}

func (validator *EpochValidator) doRun(epochNo uint64, vfuncs ...epochValidationFunc) error {
	logrus.WithField("epochNo", epochNo).Debug("Epoch validator does run validating epoch...")

	epoch := types.NewEpochNumberUint64(epochNo)
	for _, f := range vfuncs {
		if err := f(epoch); err != nil {
			return err
		}
	}

	return nil
}

// Validate epoch combo api suite eg., cfx_getTransactionByHash/cfx_getTransactionReceipt/cfx_getBlockByHash/cfx_getBlockByBlockNumber
func (validator *EpochValidator) validateEpochCombo(epoch *types.Epoch) error {
	epBlockHashes, err := validator.cfx.GetBlocksByEpoch(epoch)
	if err != nil {
		return errors.WithMessage(err, "failed to query epoch block hashes for validating epoch combo")
	}

	ri := util.RandUint64(uint64(len(epBlockHashes)))
	bh := epBlockHashes[ri]
	if _, err := validator.validateGetBlockSummaryByHash(bh); err != nil {
		return err
	}

	block, err := validator.validateGetBlockByHash(bh)
	if err != nil {
		return err
	}

	blockNum := hexutil.Uint64(block.BlockNumber.ToInt().Uint64())
	if _, err := validator.validateGetBlockSummaryByBlockNumber(blockNum); err != nil {
		return err
	}

	if _, err := validator.validateGetBlockByBlockNumber(blockNum); err != nil {
		return err
	}

	if len(block.Transactions) == 0 {
		return nil
	}

	ri = util.RandUint64(uint64(len(block.Transactions)))
	tx := block.Transactions[ri]

	if err := validator.validateGetTransactionByHash(tx.Hash); err != nil {
		return err
	}

	if err := validator.validateGetTransactionReceipt(tx.Hash); err != nil {
		return err
	}

	return nil
}

type matchInfo struct {
	matched     bool   // if validation results matched
	resJsonStr1 string // validation result from fullnode
	resJsonStr2 string // validation result from infura
}

func (validator *EpochValidator) doValidate(fnCall, infuraCall func() (interface{}, error)) (*matchInfo, error) {
	var wg sync.WaitGroup
	var res1, res2 interface{}
	var err1, err2 error
	var json1, json2 []byte
	var mi *matchInfo

	wg.Add(2)
	go func() {
		res1, err1 = fnCall()
		wg.Done()
	}()

	go func() {
		res2, err2 = infuraCall()
		wg.Done()
	}()

	wg.Wait()

	if err := multierr.Combine(err1, err2); err != nil {
		return nil, err
	}

	json1, err1 = json.Marshal(res1)
	json2, err2 = json.Marshal(res2)

	if err := multierr.Combine(err1, err2); err != nil {
		return nil, err
	}

	mi = &matchInfo{
		matched:     reflect.DeepEqual(json1, json2),
		resJsonStr1: string(json1),
		resJsonStr2: string(json2),
	}

	if !mi.matched {
		return mi, errResultNotMatched
	}

	return mi, nil
}

// Validate cfx_getTransactionReceipt
func (validator *EpochValidator) validateGetTransactionReceipt(txHash types.Hash) error {
	var rcpt1, rcpt2 *types.TransactionReceipt
	var err1, err2 error

	fnCall := func() (interface{}, error) {
		if rcpt1, err1 = validator.cfx.GetTransactionReceipt(txHash); err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query transaction receipts from fullnode")
		}
		return rcpt1, err1
	}

	infuraCall := func() (interface{}, error) {
		if rcpt2, err2 = validator.infura.GetTransactionReceipt(txHash); err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query transaction receipts from infura")
		}
		return rcpt2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "txHash": txHash,
		}).WithError(err).Info("Epoch validator failed to validate cfx_getTransactionReceipt")
		return errors.WithMessagef(err, "failed to validate cfx_getTransactionReceipt by hash %v", txHash)
	}

	return nil
}

// Validate cfx_getTransactionByHash
func (validator *EpochValidator) validateGetTransactionByHash(txHash types.Hash) error {
	var tx1, tx2 *types.Transaction
	var err1, err2 error

	fnCall := func() (interface{}, error) {
		if tx1, err1 = validator.cfx.GetTransactionByHash(txHash); err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query transaction from fullnode")
		}
		return tx1, err1
	}

	infuraCall := func() (interface{}, error) {
		if tx2, err2 = validator.infura.GetTransactionByHash(txHash); err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query transaction from infura")
		}
		return tx2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "txHash": txHash,
		}).WithError(err).Info("Epoch validator failed to validate cfx_getTransactionByHash")
		return errors.WithMessagef(err, "failed to validate cfx_getTransactionByHash by hash %v", txHash)
	}

	return nil
}

// Validate cfx_getBlockByHash (includeTxs = true)
func (validator *EpochValidator) validateGetBlockByHash(blockHash types.Hash) (*types.Block, error) {
	var b1, b2 *types.Block
	var err1, err2 error

	fnCall := func() (interface{}, error) {
		if b1, err1 = validator.cfx.GetBlockByHash(blockHash); err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query block by hash from fullnode")
		}
		return b1, err1
	}

	infuraCall := func() (interface{}, error) {
		if b2, err2 = validator.infura.GetBlockByHash(blockHash); err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query block by hash from infura")
		}
		return b2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "blockHash": blockHash,
		}).WithError(err).Info("Epoch validator failed to validate cfx_getBlockByHash (includeTxs = true)")
		return b1, errors.WithMessagef(err, "failed to validate cfx_getBlockByHash (includeTxs = true) by hash %v", blockHash)
	}

	return b1, nil
}

// Validate cfx_getBlockByHash (includeTxs = false)
func (validator *EpochValidator) validateGetBlockSummaryByHash(blockHash types.Hash) (*types.BlockSummary, error) {
	var bs1, bs2 *types.BlockSummary
	var err1, err2 error

	fnCall := func() (interface{}, error) {
		if bs1, err1 = validator.cfx.GetBlockSummaryByHash(blockHash); err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query block summary by hash from fullnode")
		}
		return bs1, err1
	}

	infuraCall := func() (interface{}, error) {
		if bs2, err2 = validator.infura.GetBlockSummaryByHash(blockHash); err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query block summary by hash from infura")
		}
		return bs2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "blockHash": blockHash,
		}).WithError(err).Info("Epoch validator failed to validate cfx_getBlockByHash (includeTxs = false)")
		return bs1, errors.WithMessagef(err, "failed to validate cfx_getBlockByHash (includeTxs = false) by hash %v", blockHash)
	}

	return bs1, nil
}

// Validate cfx_getBlockByBlockNumber (includeTxs = true)
func (validator *EpochValidator) validateGetBlockByBlockNumber(blockNumer hexutil.Uint64) (*types.Block, error) {
	var b1, b2 *types.Block
	var err1, err2 error

	fnCall := func() (interface{}, error) {
		if b1, err1 = validator.cfx.GetBlockByBlockNumber(blockNumer); err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query block from fullnode")
		}
		return b1, err1
	}

	infuraCall := func() (interface{}, error) {
		if b2, err2 = validator.infura.GetBlockByBlockNumber(blockNumer); err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query block from infura")
		}
		return b2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "blockNumer": blockNumer,
		}).WithError(err).Info("Epoch validator failed to validate cfx_getBlockByBlockNumber (includeTxs = true)")
		return b1, errors.WithMessagef(err, "failed to validate cfx_getBlockByBlockNumber (includeTxs = true) by number %v", blockNumer)
	}

	return b1, nil
}

// Validate cfx_getBlockByBlockNumber (includeTxs = false)
func (validator *EpochValidator) validateGetBlockSummaryByBlockNumber(blockNumer hexutil.Uint64) (*types.BlockSummary, error) {
	var b1, b2 *types.BlockSummary
	var err1, err2 error

	fnCall := func() (interface{}, error) {
		if b1, err1 = validator.cfx.GetBlockSummaryByBlockNumber(blockNumer); err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query block summary from fullnode")
		}
		return b1, err1
	}

	infuraCall := func() (interface{}, error) {
		if b2, err2 = validator.infura.GetBlockSummaryByBlockNumber(blockNumer); err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query block summary from infura")
		}
		return b2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "blockNumer": blockNumer,
		}).WithError(err).Info("Epoch validator failed to validate cfx_getBlockByBlockNumber (includeTxs = false)")
		return b1, errors.WithMessagef(err, "failed to validate cfx_getBlockByBlockNumber (includeTxs = false) by number %v", blockNumer)
	}

	return b1, nil
}

// Validate cfx_getBlocksByEpoch
func (validator *EpochValidator) validateGetBlocksByEpoch(epoch *types.Epoch) error {
	var bhs1, bhs2 []types.Hash
	var err1, err2 error

	fnCall := func() (interface{}, error) {
		if bhs1, err1 = validator.cfx.GetBlocksByEpoch(epoch); err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query epoch block hashes from fullnode")
		}
		return bhs1, err1
	}

	infuraCall := func() (interface{}, error) {
		if bhs2, err2 = validator.infura.GetBlocksByEpoch(epoch); err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query epoch block hashes from infura")
		}
		return bhs2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "epoch": epoch,
		}).WithError(err).Info("Epoch validator failed to validate cfx_getBlocksByEpoch")
		return errors.WithMessagef(err, "failed to validate cfx_getBlocksByEpoch by epoch %v", epoch)
	}

	return nil
}

// Validate cfx_getBlockByEpochNumber (includeTxs = true)
func (validator *EpochValidator) validateGetBlockByEpoch(epoch *types.Epoch) error {
	var b1, b2 *types.Block
	var err1, err2 error

	fnCall := func() (interface{}, error) {
		if b1, err1 = validator.cfx.GetBlockByEpoch(epoch); err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query epoch block from fullnode")
		}
		return b1, err1
	}

	infuraCall := func() (interface{}, error) {
		if b2, err2 = validator.infura.GetBlockByEpoch(epoch); err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query epoch block from infura")
		}
		return b2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "epoch": epoch,
		}).WithError(err).Info("Epoch validator failed to validate cfx_getBlockByEpochNumber (includeTxs = true) ")
		return errors.WithMessagef(err, "failed to validate cfx_getBlockByEpochNumber (includeTxs = true) by epoch %v", epoch)
	}

	return nil
}

// Validate cfx_getBlockByEpochNumber (includeTxs = false)
func (validator *EpochValidator) validateGetBlockSummaryByEpoch(epoch *types.Epoch) error {
	var bs1, bs2 *types.BlockSummary
	var err1, err2 error

	fnCall := func() (interface{}, error) {
		if bs1, err1 = validator.cfx.GetBlockSummaryByEpoch(epoch); err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query epoch block from fullnode")
		}
		return bs1, err1
	}

	infuraCall := func() (interface{}, error) {
		if bs2, err2 = validator.infura.GetBlockSummaryByEpoch(epoch); err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query epoch block from infura")
		}
		return bs2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "epoch": epoch,
		}).WithError(err).Info("Epoch validator failed to validate cfx_getBlockByEpochNumber (includeTxs = false) ")
		return errors.WithMessagef(err, "failed to validate cfx_getBlockByEpochNumber (includeTxs = false) by epoch %v", epoch)
	}

	return nil
}

// Validate cfx_getLogs
func (validator *EpochValidator) validateGetLogs(epoch *types.Epoch) error {
	epochBigInt, _ := epoch.ToInt()
	epochNo := epochBigInt.Uint64()

	randomDiff := util.RandUint64(200) // better keep this span small, otherwise fullnode maybe exhausted by low performance "cfx_getLogs" call.
	if epochNo < randomDiff {
		randomDiff = 0
	}

	logger := logrus.WithFields(logrus.Fields{"epochNo": epochNo, "randomDiff": randomDiff})

	// filter: epoch range
	filter := types.LogFilter{
		FromEpoch: types.NewEpochNumberUint64(epochNo - randomDiff),
		ToEpoch:   types.NewEpochNumberUint64(epochNo),
	}

	if err := validator.doValidateGetLogs(filter); err != nil {
		logger.WithField("filter", filter).WithError(err).Debug("Epoch validator failed to validate cfx_getLogs")
		return errors.WithMessagef(err, "failed to validate cfx_getLogs")
	}

	// TODO add more logFilter for testing

	return nil
}

func (validator *EpochValidator) doValidateGetLogs(filter types.LogFilter) error {
	var logs1, logs2 []types.Log
	var err1, err2 error

	fnCall := func() (interface{}, error) {
		if logs1, err1 = validator.cfx.GetLogs(filter); err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query logs from fullnode")
		}
		return logs1, err1
	}

	infuraCall := func() (interface{}, error) {
		if logs2, err2 = validator.infura.GetLogs(filter); err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query logs from infura")
		}
		return logs2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "filter": filter,
		}).WithError(err).Info("Epoch validator failed to validate cfx_getLogs")
		return err
	}

	return nil
}

func (validator *EpochValidator) saveScanCursor() error {
	// Write last validated epoch to config file
	epochStr := strconv.FormatUint(atomic.LoadUint64(&validator.conf.EpochScanFrom), 10)
	if err := ioutil.WriteFile(validEpochFromNoFilePath, []byte(epochStr), fs.ModePerm); err != nil {
		logrus.WithError(err).Infof("Epoch validator failed to write last validated epoch to file %v", validEpochFromNoFilePath)

		return err
	}

	return nil
}

func (validator *EpochValidator) Destroy() {
	// Close cfx client instance
	validator.cfx.Close()
	validator.infura.Close()

	// Save scaning cursor
	validator.saveScanCursor()
}