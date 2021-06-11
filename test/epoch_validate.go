package test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"io/ioutil"
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
)

var (
	validEpochFromNoFilePath = ".evno" // file path to read/write epoch number from which the validation will start
)

type epochValidationFunc func(epoch *types.Epoch) error

// EVConfig validation config provided to EpochValidator
type EVConfig struct {
	FullnodeRpcEndpoint string // Fullnode rpc endpoint used as benchmark
	InfuraRpcEndpoint   string // Infura rpc endpoint used to be validated against
}

// EpochValidator pulls epoch data from fullnode and infura endpoints, and then compares the
// epoch data to validate if the infura epoch data complies to fullnode.
type EpochValidator struct {
	infura    sdk.ClientOperator // infura rpc service client instance
	cfx       sdk.ClientOperator // fullnode client instance
	epochFrom uint64             // epoch number to validate from

	samplingInterval    time.Duration // interval to sample epoch to validate
	scanIntervalNormal  time.Duration // interval to scan to validate epoch in normal mode
	scanIntervalCatchUp time.Duration // interval to scan to validate epoch in catching up mode
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
		cfx:       cfx,
		infura:    infura,
		epochFrom: 1,

		samplingInterval:    time.Second * 15,
		scanIntervalNormal:  time.Second * 1,
		scanIntervalCatchUp: time.Millisecond * 100,
	}

	// Read last validated epoch from config, from which the validation will continue
	dat, err := ioutil.ReadFile(validEpochFromNoFilePath)
	if err != nil {
		logrus.WithError(err).Debugf("Epoch validator failed to load last validated epoch from file %v", validEpochFromNoFilePath)
		return validator
	}

	datStr := strings.TrimSpace(string(dat))
	if validator.epochFrom, err = strconv.ParseUint(datStr, 10, 64); err == nil {
		logrus.WithField("epochFrom", validator.epochFrom).Infof("Epoch validator loaded last validated epoch from file %v", validEpochFromNoFilePath)
	} else {
		logrus.WithError(err).Debugf("Epoch validator failed to load last validated epoch from file %v", validEpochFromNoFilePath)
	}

	return validator
}

func (validator *EpochValidator) Run(ctx context.Context, wg *sync.WaitGroup) {
	logrus.WithField("epochFrom", validator.epochFrom).Infof("Epoch validator running to validate epoch data...")

	wg.Add(1)
	defer wg.Done()

	// Randomly sampling nearhead epoch for validation. Nearhead epochs are very likely to be reverted
	// due to pivot switch, it's acceptable to just trigger a warnning once the validation failed.
	samplingTicker := time.NewTicker(validator.samplingInterval)
	defer samplingTicker.Stop()

	// Sequentially scaning epochs from the earliest epoch to latest confirmed epoch. Confirmed epochs are
	// rerely reverted, an error/panic will be triggered once validation failed.
	scanTicker := time.NewTicker(validator.scanIntervalNormal)
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
				logrus.WithError(err).Warn("Epoch validator failed to do samping for epoch validation")
			}
		case <-scanTicker.C:
			if err := validator.doScanning(scanTicker); err != nil {
				scanValidFailures++

				errFunc := logrus.WithError(err).Error

				if scanValidFailures >= maxScanValidFailures {
					errFunc = logrus.WithError(err).Fatal

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
	logrus.Debug("Epoch validator ticking to sample nearhead for validation...")

	// Fetch latest state epoch from blockchain
	epoch, err := validator.cfx.GetEpochNumber(types.EpochLatestState)
	if err != nil {
		return errors.WithMessage(err, "failed to query the latest state epoch number")
	}

	// Shuffle epoch number by reduction of random number less than 100
	stateEpochNo := epoch.ToInt().Uint64()

	randomDiff := util.RandUint64(100)
	if stateEpochNo < randomDiff {
		randomDiff = 0
	}

	epochNo := stateEpochNo - randomDiff

	logrus.WithFields(logrus.Fields{
		"stateEpochNo": stateEpochNo, "shuffledEpochNo": epochNo,
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
	logrus.WithField("epochFrom", validator.epochFrom).Debug("Epoch validation ticking to scan for validation...")

	if complete, err := validator.scanOnce(); err != nil {
		ticker.Reset(validator.scanIntervalNormal)
		return err
	} else if complete {
		ticker.Reset(validator.scanIntervalNormal)
	} else {
		ticker.Reset(validator.scanIntervalCatchUp)
	}

	return nil
}

func (validator *EpochValidator) scanOnce() (bool, error) {
	// Fetch latest confirmed epoch from blockchain
	epoch, err := validator.cfx.GetEpochNumber(types.EpochLatestConfirmed)
	if err != nil {
		return false, errors.WithMessage(err, "failed to query the latest confirmed epoch")
	}

	// Already catch up to the latest confirmed epoch
	maxEpochTo := epoch.ToInt().Uint64()
	if validator.epochFrom > maxEpochTo {
		logrus.WithField("epochRange", citypes.EpochRange{
			EpochFrom: validator.epochFrom,
			EpochTo:   maxEpochTo,
		},
		).Debug("Epoch validator scaning skipped due to catched up already")

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
	if err := validator.validate(validator.epochFrom, epochVFuncs...); err != nil {
		return false, errors.WithMessagef(err, "failed to validate epoch #%v", validator.epochFrom)
	}

	validator.epochFrom++

	if validator.epochFrom%5000 == 0 { // periodly save the scaning cursor per 5000 epochs in case of data lost
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
	var encoded []byte

	wg.Add(1)
	go func() {
		rcpt1, err1 = validator.cfx.GetTransactionReceipt(txHash)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		rcpt2, err2 = validator.infura.GetTransactionReceipt(txHash)
		wg.Done()
	}()

	wg.Wait()

	if err1 != nil {
		return errors.WithMessagef(err1, "failed to query transaction receipts from fullnode by hash %v", txHash)
	}

	if err2 != nil {
		return errors.WithMessagef(err2, "failed to query transaction receipts from infura by hash %v", txHash)
	}

	encoded, err1 = json.Marshal(rcpt1)
	if err1 != nil {
		return errors.WithMessagef(err1, "failed to marshal json for result of cfx_getTransactionReceipt for fullnode")
	}

	json1 := string(encoded)

	encoded, err2 = json.Marshal(rcpt2)
	if err2 != nil {
		return errors.WithMessagef(err2, "failed to marshal json for result of cfx_getTransactionReceipt for infura")
	}

	json2 := string(encoded)

	if json1 != json2 {
		logrus.WithFields(logrus.Fields{"json1": json1, "json2": json2}).Debug("Epoch validator result not match for cfx_getTransactionReceipt")
		return errors.New("result not match for cfx_getTransactionReceipt")
	}

	return nil
}

// Validate cfx_getTransactionByHash
func (validator *EpochValidator) validateGetTransactionByHash(txHash types.Hash) error {
	var wg sync.WaitGroup
	var tx1, tx2 *types.Transaction
	var err1, err2 error
	var encoded []byte

	wg.Add(1)
	go func() {
		tx1, err1 = validator.cfx.GetTransactionByHash(txHash)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		tx2, err2 = validator.infura.GetTransactionByHash(txHash)
		wg.Done()
	}()

	wg.Wait()

	if err1 != nil {
		return errors.WithMessagef(err1, "failed to query transaction from fullnode by hash %v ", txHash)
	}

	if err2 != nil {
		return errors.WithMessagef(err2, "failed to query transaction from infura by hash %v", txHash)
	}

	encoded, err1 = json.Marshal(tx1)
	if err1 != nil {
		return errors.WithMessagef(err1, "failed to marshal json for result of cfx_getTransactionByHash for fullnode")
	}

	json1 := string(encoded)

	encoded, err2 = json.Marshal(tx2)
	if err2 != nil {
		return errors.WithMessagef(err2, "failed to marshal json for result of cfx_getTransactionByHash for infura")
	}

	json2 := string(encoded)

	if json1 != json2 {
		logrus.WithFields(logrus.Fields{"json1": json1, "json2": json2}).Debug("Epoch validator result not match for cfx_getTransactionByHash")
		return errors.New("result not match for cfx_getTransactionByHash")
	}

	return nil
}

// Validate cfx_getBlockByHash
func (validator *EpochValidator) validateGetBlockByHash(blockHash types.Hash) (*types.Block, error) {
	var wg sync.WaitGroup
	var b1, b2 *types.Block
	var err1, err2 error
	var encoded []byte

	wg.Add(1)
	go func() {
		b1, err1 = validator.cfx.GetBlockByHash(blockHash)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		b2, err2 = validator.infura.GetBlockByHash(blockHash)
		wg.Done()
	}()

	wg.Wait()

	if err1 != nil {
		return nil, errors.WithMessagef(err1, "failed to query block by hash %v for fullnode", blockHash)
	}

	if err2 != nil {
		return nil, errors.WithMessagef(err2, "failed to query epoch block by hash %v for infura", blockHash)
	}

	encoded, err1 = json.Marshal(b1)
	if err1 != nil {
		return nil, errors.WithMessagef(err1, "failed to marshal json for result of cfx_getBlockByHash for fullnode")
	}

	json1 := string(encoded)

	encoded, err2 = json.Marshal(b2)
	if err2 != nil {
		return nil, errors.WithMessagef(err2, "failed to marshal json for result of cfx_getBlockByHash for infura")
	}

	json2 := string(encoded)

	if json1 != json2 {
		logrus.WithFields(logrus.Fields{"json1": json1, "json2": json2}).Debug("Epoch validator result not match for cfx_getBlockByHash")
		return nil, errors.New("result not match for cfx_getBlockByHash")
	}

	return b1, nil
}

// Validate cfx_getBlocksByEpoch
func (validator *EpochValidator) validateGetBlocksByEpoch(epoch *types.Epoch) error {
	var wg sync.WaitGroup
	var bhs1, bhs2 []types.Hash
	var err1, err2 error
	var encoded []byte

	wg.Add(1)
	go func() {
		bhs1, err1 = validator.cfx.GetBlocksByEpoch(epoch)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		bhs2, err2 = validator.infura.GetBlocksByEpoch(epoch)
		wg.Done()
	}()

	wg.Wait()

	if err1 != nil {
		return errors.WithMessage(err1, "failed to query epoch block hashes for fullnode")
	}

	if err2 != nil {
		return errors.WithMessage(err2, "failed to query epoch block hashes for infura")
	}

	encoded, err1 = json.Marshal(bhs1)
	if err1 != nil {
		return errors.WithMessagef(err1, "failed to marshal json for result of cfx_getBlocksByEpoch for fullnode")
	}

	json1 := string(encoded)

	encoded, err2 = json.Marshal(bhs2)
	if err2 != nil {
		return errors.WithMessagef(err2, "failed to marshal json for result of cfx_getBlocksByEpoch for infura")
	}

	json2 := string(encoded)

	if json1 != json2 {
		logrus.WithFields(logrus.Fields{"epoch": epoch, "json1": json1, "json2": json2}).Debug("Epoch validator result not match for cfx_getBlocksByEpoch")
		return errors.New("result not match for cfx_getBlocksByEpoch")
	}

	return nil
}

// Validate cfx_getBlockByEpochNumber (block)
func (validator *EpochValidator) validateGetBlockByEpoch(epoch *types.Epoch) error {
	var wg sync.WaitGroup
	var b1, b2 *types.Block
	var err1, err2 error
	var encoded []byte

	wg.Add(1)
	go func() {
		b1, err1 = validator.cfx.GetBlockByEpoch(epoch)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		b2, err2 = validator.infura.GetBlockByEpoch(epoch)
		wg.Done()
	}()

	wg.Wait()

	if err1 != nil {
		return errors.WithMessage(err1, "failed to query epoch block for fullnode")
	}

	if err2 != nil {
		return errors.WithMessage(err2, "failed to query epoch block for infura")
	}

	encoded, err1 = json.Marshal(b1)
	if err1 != nil {
		return errors.WithMessagef(err1, "failed to marshal json for (block) result of cfx_getBlockByEpochNumber for fullnode")
	}

	json1 := string(encoded)

	encoded, err2 = json.Marshal(b2)
	if err2 != nil {
		return errors.WithMessagef(err2, "failed to marshal json for (block) result of cfx_getBlockByEpochNumber for infura")
	}

	json2 := string(encoded)

	if json1 != json2 {
		logrus.WithFields(logrus.Fields{"json1": json1, "json2": json2}).Debug("Epoch validator (block) result not match for cfx_getBlockByEpochNumber")
		return errors.New("(block) result not match for cfx_getBlockByEpochNumber")
	}

	return nil
}

// Validate cfx_getBlockByEpochNumber (block summary)
func (validator *EpochValidator) validateGetBlockSummaryByEpoch(epoch *types.Epoch) error {
	var wg sync.WaitGroup
	var bs1, bs2 *types.BlockSummary
	var err1, err2 error
	var encoded []byte

	wg.Add(1)
	go func() {
		bs1, err1 = validator.cfx.GetBlockSummaryByEpoch(epoch)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		bs2, err2 = validator.infura.GetBlockSummaryByEpoch(epoch)
		wg.Done()
	}()

	wg.Wait()

	if err1 != nil {
		return errors.WithMessage(err1, "failed to query epoch block summary for fullnode")
	}

	if err2 != nil {
		return errors.WithMessage(err2, "failed to query epoch block summary for infura")
	}

	encoded, err1 = json.Marshal(bs1)
	if err1 != nil {
		return errors.WithMessagef(err1, "failed to marshal json for (block summary) result of cfx_getBlockByEpochNumber for fullnode")
	}

	json1 := string(encoded)

	encoded, err2 = json.Marshal(bs2)
	if err2 != nil {
		return errors.WithMessagef(err2, "failed to marshal json for (block summary) result of cfx_getBlockByEpochNumber for infura")
	}

	json2 := string(encoded)

	if json1 != json2 {
		logrus.WithFields(logrus.Fields{"json1": json1, "json2": json2}).Debug("Epoch validator (block summary) result not match for cfx_getBlockByEpochNumber")
		return errors.New("(block summary) result not match for cfx_getBlockByEpochNumber")
	}

	return nil
}

// Validate cfx_getLogs
func (validator *EpochValidator) validateGetLogs(epoch *types.Epoch) error {
	epochBigInt, _ := epoch.ToInt()
	epochNo := epochBigInt.Uint64()

	randomDiff := util.RandUint64(1000)
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
	var encoded []byte

	wg.Add(1)
	go func() {
		logs1, err1 = validator.cfx.GetLogs(filter)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		logs2, err2 = validator.cfx.GetLogs(filter)
		wg.Done()
	}()

	wg.Wait()

	if err1 != nil {
		return errors.WithMessagef(err1, "failed to call cfx_getLogs for fullnode")
	}

	if err2 != nil {
		return errors.WithMessagef(err2, "failed to call cfx_getLogs for infura")
	}

	encoded, err1 = json.Marshal(logs1)
	if err1 != nil {
		return errors.WithMessagef(err1, "failed to marshal json for result of cfx_getLogs for fullnode")
	}

	json1 := string(encoded)

	encoded, err2 = json.Marshal(logs2)
	if err2 != nil {
		return errors.WithMessagef(err2, "failed to marshal json for result of cfx_getLogs for infura")
	}

	json2 := string(encoded)

	if json1 != json2 {
		logrus.WithFields(logrus.Fields{"json1": json1, "json2": json2}).Debug("Epoch validator result not match for cfx_getLogs")
		return errors.New("result not match for cfx_getLogs")
	}

	return nil
}

func (validator *EpochValidator) saveScanCursor() error {
	// Write last validated epoch to config file
	epochStr := strconv.FormatUint(atomic.LoadUint64(&validator.epochFrom), 10)
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
