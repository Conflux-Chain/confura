package test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"
)

var (
	validEpochFromNoFilePath = ".evno" // file path to read/write epoch number from where the validation will start
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
	logrus.WithField("epochFrom", validator.conf.EpochScanFrom).Infof("Epoch validator running to validate epoch data...")

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
			if err := validator.doSampling(); err != nil {
				logger.WithError(err).Warn("Epoch validator failed to do samping for epoch validation")
			}
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
	if err := validator.validate(epochNo, epochVFuncs...); err != nil {
		return errors.WithMessagef(err, "failed to validate epoch #%v", epochNo)
	}

	return nil
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
	if err := validator.validate(validator.conf.EpochScanFrom, epochVFuncs...); err != nil {
		return false, errors.WithMessagef(err, "failed to validate epoch #%v", validator.conf.EpochScanFrom)
	}

	validator.conf.EpochScanFrom++

	if validator.conf.EpochScanFrom%5000 == 0 { // periodly save the scaning progress per 5000 epochs in case of data lost
		validator.saveScanCursor()
	}

	return false, nil
}

func (validator *EpochValidator) validate(epochNo uint64, vfuncs ...epochValidationFunc) error {
	logrus.WithField("epochNo", epochNo).Debug("Epoch validator validating epoch...")

	epoch := types.NewEpochNumberUint64(epochNo)
	for _, f := range vfuncs {
		if err := f(epoch); err != nil {
			return err
		}
	}

	return nil
}

// Validate epoch combo api suite eg., cfx_getTransactionByHash/cfx_getTransactionReceipt/cfx_getBlockByHash
func (validator *EpochValidator) validateEpochCombo(epoch *types.Epoch) error {
	epBlockHashes, err := validator.cfx.GetBlocksByEpoch(epoch)
	if err != nil {
		return errors.WithMessage(err, "failed to query epoch block hashes for validating epoch combo")
	}

	ri := util.RandUint64(uint64(len(epBlockHashes)))
	bh := epBlockHashes[ri]

	block, err := validator.validateGetBlockByHash(bh)
	if err != nil {
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

// Validate cfx_getTransactionReceipt
func (validator *EpochValidator) validateGetTransactionReceipt(txHash types.Hash) error {
	var wg sync.WaitGroup
	var rcpt1, rcpt2 *types.TransactionReceipt
	var err1, err2 error
	var json1, json2 []byte

	wg.Add(1)
	go func() {
		rcpt1, err1 = validator.cfx.GetTransactionReceipt(txHash)
		err1 = errors.WithMessagef(err1, "failed to query transaction receipts from fullnode by hash %v", txHash)

		wg.Done()
	}()

	wg.Add(1)
	go func() {
		rcpt2, err2 = validator.infura.GetTransactionReceipt(txHash)
		err2 = errors.WithMessagef(err2, "failed to query transaction receipts from infura by hash %v", txHash)

		wg.Done()
	}()

	wg.Wait()

	if err := multierr.Combine(err1, err2); err != nil {
		return err
	}

	json1, err1 = json.Marshal(rcpt1)
	err1 = errors.WithMessagef(err1, "failed to marshal json for result of cfx_getTransactionReceipt for fullnode")

	json2, err2 = json.Marshal(rcpt2)
	err2 = errors.WithMessagef(err2, "failed to marshal json for result of cfx_getTransactionReceipt for infura")

	if err := multierr.Combine(err1, err2); err != nil {
		return err
	}

	if string(json1) != string(json2) {
		logrus.WithFields(logrus.Fields{"json1": string(json1), "json2": string(json2)}).Debug("Epoch validator result not match for cfx_getTransactionReceipt")
		return errors.New("result not match for cfx_getTransactionReceipt")
	}

	return nil
}

// Validate cfx_getTransactionByHash
func (validator *EpochValidator) validateGetTransactionByHash(txHash types.Hash) error {
	var wg sync.WaitGroup
	var tx1, tx2 *types.Transaction
	var err1, err2 error
	var json1, json2 []byte

	wg.Add(1)
	go func() {
		tx1, err1 = validator.cfx.GetTransactionByHash(txHash)
		err1 = errors.WithMessagef(err1, "failed to query transaction from fullnode by hash %v ", txHash)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		tx2, err2 = validator.infura.GetTransactionByHash(txHash)
		err2 = errors.WithMessagef(err2, "failed to query transaction from infura by hash %v", txHash)
		wg.Done()
	}()

	wg.Wait()

	if err := multierr.Combine(err1, err2); err != nil {
		return err
	}

	json1, err1 = json.Marshal(tx1)
	err1 = errors.WithMessagef(err1, "failed to marshal json for result of cfx_getTransactionByHash for fullnode")

	json2, err2 = json.Marshal(tx2)
	err2 = errors.WithMessagef(err2, "failed to marshal json for result of cfx_getTransactionByHash for infura")

	if err := multierr.Combine(err1, err2); err != nil {
		return err
	}

	if string(json1) != string(json2) {
		logrus.WithFields(logrus.Fields{"json1": string(json1), "json2": string(json2)}).Debug("Epoch validator result not match for cfx_getTransactionByHash")
		return errors.New("result not match for cfx_getTransactionByHash")
	}

	return nil
}

// Validate cfx_getBlockByHash
func (validator *EpochValidator) validateGetBlockByHash(blockHash types.Hash) (*types.Block, error) {
	var wg sync.WaitGroup
	var b1, b2 *types.Block
	var err1, err2 error
	var json1, json2 []byte

	wg.Add(1)
	go func() {
		b1, err1 = validator.cfx.GetBlockByHash(blockHash)
		err1 = errors.WithMessagef(err1, "failed to query block by hash %v for fullnode", blockHash)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		b2, err2 = validator.infura.GetBlockByHash(blockHash)
		err2 = errors.WithMessagef(err2, "failed to query epoch block by hash %v for infura", blockHash)
		wg.Done()
	}()

	wg.Wait()

	if err := multierr.Combine(err1, err2); err != nil {
		return nil, err
	}

	json1, err1 = json.Marshal(b1)
	err1 = errors.WithMessagef(err1, "failed to marshal json for result of cfx_getBlockByHash for fullnode")

	json2, err2 = json.Marshal(b2)
	err2 = errors.WithMessagef(err2, "failed to marshal json for result of cfx_getBlockByHash for infura")

	if err := multierr.Combine(err1, err2); err != nil {
		return nil, err
	}

	if string(json1) != string(json2) {
		logrus.WithFields(logrus.Fields{"json1": string(json1), "json2": string(json2)}).Debug("Epoch validator result not match for cfx_getBlockByHash")
		return nil, errors.New("result not match for cfx_getBlockByHash")
	}

	return b1, nil
}

// Validate cfx_getBlocksByEpoch
func (validator *EpochValidator) validateGetBlocksByEpoch(epoch *types.Epoch) error {
	var wg sync.WaitGroup
	var bhs1, bhs2 []types.Hash
	var err1, err2 error
	var json1, json2 []byte

	wg.Add(1)
	go func() {
		bhs1, err1 = validator.cfx.GetBlocksByEpoch(epoch)
		err1 = errors.WithMessage(err1, "failed to query epoch block hashes for fullnode")
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		bhs2, err2 = validator.infura.GetBlocksByEpoch(epoch)
		err2 = errors.WithMessage(err2, "failed to query epoch block hashes for infura")
		wg.Done()
	}()

	wg.Wait()

	if err := multierr.Combine(err1, err2); err != nil {
		return err
	}

	json1, err1 = json.Marshal(bhs1)
	err1 = errors.WithMessagef(err1, "failed to marshal json for result of cfx_getBlocksByEpoch for fullnode")

	json2, err2 = json.Marshal(bhs2)
	err2 = errors.WithMessagef(err2, "failed to marshal json for result of cfx_getBlocksByEpoch for infura")

	if err := multierr.Combine(err1, err2); err != nil {
		return err
	}

	if string(json1) != string(json2) {
		logrus.WithFields(logrus.Fields{"epoch": epoch, "json1": string(json1), "json2": string(json2)}).Debug("Epoch validator result not match for cfx_getBlocksByEpoch")
		return errors.New("result not match for cfx_getBlocksByEpoch")
	}

	return nil
}

// Validate cfx_getBlockByEpochNumber (block)
func (validator *EpochValidator) validateGetBlockByEpoch(epoch *types.Epoch) error {
	var wg sync.WaitGroup
	var b1, b2 *types.Block
	var err1, err2 error
	var json1, json2 []byte

	wg.Add(1)
	go func() {
		b1, err1 = validator.cfx.GetBlockByEpoch(epoch)
		err1 = errors.WithMessage(err1, "failed to query epoch block for fullnode")
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		b2, err2 = validator.infura.GetBlockByEpoch(epoch)
		err2 = errors.WithMessage(err2, "failed to query epoch block for infura")
		wg.Done()
	}()

	wg.Wait()

	if err := multierr.Combine(err1, err2); err != nil {
		return err
	}

	json1, err1 = json.Marshal(b1)
	err1 = errors.WithMessagef(err1, "failed to marshal json for (block) result of cfx_getBlockByEpochNumber for fullnode")

	json2, err2 = json.Marshal(b2)
	err2 = errors.WithMessagef(err2, "failed to marshal json for (block) result of cfx_getBlockByEpochNumber for infura")

	if err := multierr.Combine(err1, err2); err != nil {
		return err
	}

	if string(json1) != string(json2) {
		logrus.WithFields(logrus.Fields{"json1": string(json1), "json2": string(json2)}).Debug("Epoch validator (block) result not match for cfx_getBlockByEpochNumber")
		return errors.New("(block) result not match for cfx_getBlockByEpochNumber")
	}

	return nil
}

// Validate cfx_getBlockByEpochNumber (block summary)
func (validator *EpochValidator) validateGetBlockSummaryByEpoch(epoch *types.Epoch) error {
	var wg sync.WaitGroup
	var bs1, bs2 *types.BlockSummary
	var err1, err2 error
	var json1, json2 []byte

	wg.Add(1)
	go func() {
		bs1, err1 = validator.cfx.GetBlockSummaryByEpoch(epoch)
		err1 = errors.WithMessage(err1, "failed to query epoch block summary for fullnode")
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		bs2, err2 = validator.infura.GetBlockSummaryByEpoch(epoch)
		err2 = errors.WithMessage(err2, "failed to query epoch block summary for infura")
		wg.Done()
	}()

	wg.Wait()

	if err := multierr.Combine(err1, err2); err != nil {
		return err
	}

	json1, err1 = json.Marshal(bs1)
	err1 = errors.WithMessagef(err1, "failed to marshal json for (block summary) result of cfx_getBlockByEpochNumber for fullnode")

	json2, err2 = json.Marshal(bs2)
	err2 = errors.WithMessagef(err2, "failed to marshal json for (block summary) result of cfx_getBlockByEpochNumber for infura")

	if err := multierr.Combine(err1, err2); err != nil {
		return err
	}

	if string(json1) != string(json2) {
		logrus.WithFields(logrus.Fields{"json1": string(json1), "json2": string(json2)}).Debug("Epoch validator (block summary) result not match for cfx_getBlockByEpochNumber")
		return errors.New("(block summary) result not match for cfx_getBlockByEpochNumber")
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
	var wg sync.WaitGroup
	var logs1, logs2 []types.Log
	var err1, err2 error
	var json1, json2 []byte

	wg.Add(1)
	go func() {
		logs1, err1 = validator.cfx.GetLogs(filter)
		err1 = errors.WithMessagef(err1, "failed to call cfx_getLogs for fullnode")
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		logs2, err2 = validator.cfx.GetLogs(filter)
		err2 = errors.WithMessagef(err2, "failed to call cfx_getLogs for infura")
		wg.Done()
	}()

	wg.Wait()

	if err := multierr.Combine(err1, err2); err != nil {
		return err
	}

	json1, err1 = json.Marshal(logs1)
	err1 = errors.WithMessagef(err1, "failed to marshal json for result of cfx_getLogs for fullnode")

	json2, err2 = json.Marshal(logs2)
	err2 = errors.WithMessagef(err2, "failed to marshal json for result of cfx_getLogs for infura")

	if err := multierr.Combine(err1, err2); err != nil {
		return err
	}

	if string(json1) != string(json2) {
		logrus.WithFields(logrus.Fields{"json1": string(json1), "json2": string(json2)}).Debug("Epoch validator result not match for cfx_getLogs")
		return errors.New("result not match for cfx_getLogs")
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
