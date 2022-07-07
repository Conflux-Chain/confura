package test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"io/ioutil"
	"math"
	"math/big"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/conflux-chain/conflux-infura/util/blacklist"
	"github.com/conflux-chain/conflux-infura/util/rpc"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"
)

type epochValidationCtxKey string

const (
	// number of retries on sampling validation failed
	samplingValidationRetries = 2
	// sleeping duration before each retry
	samplingValidationSleepDuration = time.Second * 1
	// random epoch diff to latest epoch for sampling validation
	samplingRandomEpochDiff = 100

	// context keys to store epoch context values
	ctxKeyCfxPivotBlock             epochValidationCtxKey = "pivotBlock"             // *types.Block
	ctxKeyCfxPivotBlockSummary      epochValidationCtxKey = "pivotBlockSummary"      // *types.BlockSummary
	ctxKeyCfxBlockHashes            epochValidationCtxKey = "blockHashes"            // []types.Hash
	ctxKeyCfxFirstBlock             epochValidationCtxKey = "firstBlock"             // *types.Block
	ctxKeyCfxFirstBlockSummary      epochValidationCtxKey = "firstBlockSummary"      // *types.BlockSummary
	ctxKeyCfxSomeTransactionReceipt epochValidationCtxKey = "someTransactionReceipt" // *types.TransactionReceipt
)

var (
	// file path to read/write epoch number from where the validation will start
	validEpochFromNoFilePath = ".evno"

	errResultNotMatched  = errors.New("results not matched")
	errValidationSkipped = errors.New("validation skipped")
)

type epochValidationFunc func(ctx context.Context, epoch *types.Epoch) (context.Context, error)

// EVConfig validation config provided to EpochValidator
type EVConfig struct {
	FullnodeRpcEndpoint string        // Fullnode rpc endpoint used as benchmark
	InfuraRpcEndpoint   string        // Infura rpc endpoint used to be validated against
	EpochScanFrom       uint64        // the epoch scan from
	ScanInterval        time.Duration // scan interval
	SamplingInterval    time.Duration // sampling interval
	// sampling epoch type:
	// lm(latest_mined), ls(latest_state), lc(latest_confirmed), lf(latest_finalized), lcp(latest_checkpoint)
	SamplingEpochType string
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
	cfx := rpc.MustNewCfxClient(conf.FullnodeRpcEndpoint)
	// Prepare infura rpc service client instance
	infura := rpc.MustNewCfxClient(conf.InfuraRpcEndpoint)

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
	case "lcp":
		fallthrough
	case "latest_checkpoint":
		validator.samplingEpoch = types.EpochLatestCheckpoint
	case "lc":
		fallthrough
	case "latest_confirmed":
		validator.samplingEpoch = types.EpochLatestConfirmed
	default:
		validator.samplingEpoch = types.EpochLatestFinalized
	}

	defer logrus.
		WithFields(logrus.Fields{"config": conf, "samplingEpoch": validator.samplingEpoch}).
		Info("Test validation configurations loaded.")

	if conf.EpochScanFrom != math.MaxUint64 {
		return validator
	}

	conf.EpochScanFrom = 1 // default scan from epoch #1

	// Read last validated epoch from config file, on which the validation will continue
	dat, err := ioutil.ReadFile(validEpochFromNoFilePath)
	if err != nil {
		logrus.WithError(err).Debugf(
			"Epoch validator failed to load last validated epoch from file %v", validEpochFromNoFilePath,
		)
		return validator
	}

	datStr := strings.TrimSpace(string(dat))
	epochFrom, err := strconv.ParseUint(datStr, 10, 64)
	if err == nil {
		logrus.WithField("epochFrom", epochFrom).Infof(
			"Epoch validator loaded last validated epoch from file %v", validEpochFromNoFilePath,
		)
		conf.EpochScanFrom = epochFrom
	} else {
		logrus.WithError(err).Debugf(
			"Epoch validator failed to load last validated epoch from file %v", validEpochFromNoFilePath,
		)
	}

	return validator
}

func (validator *EpochValidator) Run(ctx context.Context, wg *sync.WaitGroup) {
	logrus.
		WithField("epochFrom", validator.conf.EpochScanFrom).
		Info("Epoch validator running to validate epoch data...")

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

	// mark how many failure times for scanning validation
	scanValidFailures := 0
	// the maximum failure times for scanning validation, once exceeded, panic will be triggered
	maxScanValidFailures := 5

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

	randomDiff := util.RandUint64(samplingRandomEpochDiff)
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
		validator.validateGetBlockSummaryByHash,
		validator.validateGetBlockByHash,
		validator.validateGetBlockSummaryByBlockNumber,
		validator.validateGetBlockByBlockNumber,
		validator.validateGetTransactionByHash,
		validator.validateGetTransactionReceipt,
		validator.validateGetLogs,
	}

	err = validator.doRun(epochNo, epochVFuncs...)
	// Since nearhead epoch re-orgs are of high possibility due to pivot switch,
	// we'd better do some retrying before determining the final validation result.
	for i := 1; i <= samplingValidationRetries && errors.Is(err, errResultNotMatched); i++ {
		time.Sleep(samplingValidationSleepDuration)
		err = validator.doRun(epochNo, epochVFuncs...)
		logrus.WithField("epoch", epochNo).
			WithError(err).Infof("Epoch validator sampling validation retried %v time(s)", i)
	}

	return errors.WithMessagef(err, "failed to validate epoch #%v", epochNo)
}

func (validator *EpochValidator) doScanning(ticker *time.Ticker) error {
	logrus.WithField("epochFrom", validator.conf.EpochScanFrom).
		Debug("Epoch validation ticking to scan for validation...")

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
		epochRange := citypes.RangeUint64{
			From: validator.conf.EpochScanFrom,
			To:   maxEpochTo,
		}
		logrus.WithField("epochRange", epochRange).Debug(
			"Epoch validator scaning skipped due to catched up already",
		)

		return true, nil
	}

	// Validate the scanned epoch
	epochVFuncs := []epochValidationFunc{
		validator.validateGetBlockSummaryByEpoch,
		validator.validateGetBlockByEpoch,
		validator.validateGetBlocksByEpoch,
		validator.validateGetBlockSummaryByHash,
		validator.validateGetBlockByHash,
		validator.validateGetBlockSummaryByBlockNumber,
		validator.validateGetBlockByBlockNumber,
		validator.validateGetTransactionByHash,
		validator.validateGetTransactionReceipt,
		validator.validateGetLogs,
	}
	if err := validator.doRun(validator.conf.EpochScanFrom, epochVFuncs...); err != nil {
		return false, errors.WithMessagef(
			err, "failed to validate epoch #%v", validator.conf.EpochScanFrom,
		)
	}

	validator.conf.EpochScanFrom++

	// periodly save the scaning progress per 1000 epochs in case of data lost
	if validator.conf.EpochScanFrom%1000 == 0 {
		validator.saveScanCursor()
	}

	return false, nil
}

func (validator *EpochValidator) doRun(epochNo uint64, vfuncs ...epochValidationFunc) error {
	logrus.WithField("epochNo", epochNo).Debug("Epoch validator does run validating epoch...")

	epoch := types.NewEpochNumberUint64(epochNo)
	ctx := context.Background()

	var err error
	for _, f := range vfuncs {
		if ctx, err = f(ctx, epoch); err != nil {
			return err
		}
	}

	return nil
}

type matchInfo struct {
	matched     bool   // if validation results matched
	resJsonStr1 string // validation result from fullnode
	resJsonStr2 string // validation result from infura
}

func (validator *EpochValidator) doValidate(
	fnCall, infuraCall func() (interface{}, error),
) (*matchInfo, error) {
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

// Validate `cfx_getTransactionReceipt`
func (validator *EpochValidator) validateGetTransactionReceipt(
	ctx context.Context, epoch *types.Epoch,
) (context.Context, error) {
	txn, err := validator.getSomeTransaction(ctx, epoch, true)
	if err != nil || txn == nil {
		return ctx, errors.WithMessage(
			err, "failed to get some exec transaction to validate cfx_getTransactionReceipt",
		)
	}

	var rcpt1, rcpt2 *types.TransactionReceipt
	var err1, err2 error

	fnCall := func() (interface{}, error) {
		if rcpt1, err1 = validator.cfx.GetTransactionReceipt(txn.Hash); err1 != nil {
			return nil, errors.WithMessage(err1, "failed to query transaction receipts from fullnode")
		}

		for i := range rcpt1.Logs { // receipt log has no `epochNumber` data
			rcpt1.Logs[i].EpochNumber = (*hexutil.Big)(big.NewInt(int64(*rcpt1.EpochNumber)))
		}

		rcpt1.Logs = validator.filterLogs(rcpt1.Logs)
		return rcpt1, err1
	}

	infuraCall := func() (interface{}, error) {
		if rcpt2, err2 = validator.infura.GetTransactionReceipt(txn.Hash); err2 != nil {
			return nil, errors.WithMessage(err2, "failed to query transaction receipts from infura")
		}

		for i := range rcpt2.Logs { // receipt log has no `epochNumber` data
			rcpt2.Logs[i].EpochNumber = (*hexutil.Big)(big.NewInt(int64(*rcpt2.EpochNumber)))
		}

		rcpt2.Logs = validator.filterLogs(rcpt2.Logs)
		return rcpt2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi,
			"txHash":    txn.Hash,
		}).WithError(err).Info("Epoch validator failed to validate cfx_getTransactionReceipt")
		return ctx, errors.WithMessage(err, "failed to validate cfx_getTransactionReceipt by hash")
	}

	return context.WithValue(ctx, ctxKeyCfxSomeTransactionReceipt, rcpt1), nil
}

// Validate `cfx_getTransactionByHash`
func (validator *EpochValidator) validateGetTransactionByHash(
	ctx context.Context, epoch *types.Epoch,
) (context.Context, error) {
	txn, err := validator.getSomeTransaction(ctx, epoch, false)
	if err != nil || txn == nil {
		return ctx, errors.WithMessage(
			err, "failed to get some transaction to validate cfx_getTransactionByHash",
		)
	}

	var tx1, tx2 *types.Transaction
	var err1, err2 error

	fnCall := func() (interface{}, error) {
		if tx1, err1 = validator.cfx.GetTransactionByHash(txn.Hash); err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query transaction from fullnode")
		}
		return tx1, err1
	}

	infuraCall := func() (interface{}, error) {
		if tx2, err2 = validator.infura.GetTransactionByHash(txn.Hash); err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query transaction from infura")
		}
		return tx2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "txHash": txn.Hash,
		}).WithError(err).Info("Epoch validator failed to validate cfx_getTransactionByHash")
		return ctx, errors.WithMessage(err, "failed to validate cfx_getTransactionByHash by hash")
	}

	return ctx, nil
}

// Validate `cfx_getBlockByHash` (includeTxs = true)
func (validator *EpochValidator) validateGetBlockByHash(
	ctx context.Context, epoch *types.Epoch,
) (context.Context, error) {
	blockHashes, err := validator.getBlockHashes(ctx, epoch)
	if err != nil || len(blockHashes) == 0 {
		return ctx, errors.WithMessage(
			err, "failed to get block hashes to validate cfx_getBlockByHash (includeTxs = true)",
		)
	}

	var b1, b2 *types.Block
	var err1, err2 error

	fnCall := func() (interface{}, error) {
		if b1, err1 = validator.cfx.GetBlockByHash(blockHashes[0]); err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query block by hash from fullnode")
		}
		return b1, err1
	}

	infuraCall := func() (interface{}, error) {
		if b2, err2 = validator.infura.GetBlockByHash(blockHashes[0]); err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query block by hash from infura")
		}
		return b2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "blockHash": blockHashes[0],
		}).WithError(err).Info("Epoch validator failed to validate cfx_getBlockByHash (includeTxs = true)")
		return ctx, errors.WithMessage(
			err, "failed to validate cfx_getBlockByHash (includeTxs = true) by hash",
		)
	}

	return context.WithValue(ctx, ctxKeyCfxFirstBlock, b1), nil
}

// Validate `cfx_getBlockByHash` (includeTxs = false)
func (validator *EpochValidator) validateGetBlockSummaryByHash(
	ctx context.Context, epoch *types.Epoch,
) (context.Context, error) {
	blockHashes, err := validator.getBlockHashes(ctx, epoch)
	if err != nil || len(blockHashes) == 0 {
		return ctx, errors.WithMessage(
			err, "failed to get block hashes to validate cfx_getBlockByHash (includeTxs = false)",
		)
	}

	var bs1, bs2 *types.BlockSummary
	var err1, err2 error

	fnCall := func() (interface{}, error) {
		if bs1, err1 = validator.cfx.GetBlockSummaryByHash(blockHashes[0]); err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query block summary by hash from fullnode")
		}
		return bs1, err1
	}

	infuraCall := func() (interface{}, error) {
		if bs2, err2 = validator.infura.GetBlockSummaryByHash(blockHashes[0]); err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query block summary by hash from infura")
		}
		return bs2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi,
			"blockHash": blockHashes[0],
		}).WithError(err).Info("Epoch validator failed to validate cfx_getBlockByHash (includeTxs = false)")

		return ctx, errors.WithMessage(
			err, "failed to validate cfx_getBlockByHash (includeTxs = false) by hash",
		)
	}

	return context.WithValue(ctx, ctxKeyCfxFirstBlockSummary, bs1), nil
}

// Validate `cfx_getBlockByBlockNumber` (includeTxs = true)
func (validator *EpochValidator) validateGetBlockByBlockNumber(
	ctx context.Context, epoch *types.Epoch,
) (context.Context, error) {
	block, err := validator.getFirstBlock(ctx, epoch)
	if err != nil || block == nil {
		return ctx, errors.WithMessage(
			err, "failed to get first block to validate cfx_getBlockByHash (includeTxs = true)",
		)
	}

	blockNumber := hexutil.Uint64(block.BlockNumber.ToInt().Uint64())

	var b1, b2 *types.Block
	var err1, err2 error

	fnCall := func() (interface{}, error) {
		if b1, err1 = validator.cfx.GetBlockByBlockNumber(blockNumber); err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query block from fullnode")
		}
		return b1, err1
	}

	infuraCall := func() (interface{}, error) {
		if b2, err2 = validator.infura.GetBlockByBlockNumber(blockNumber); err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query block from infura")
		}
		return b2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo":  mi,
			"blockNumer": blockNumber,
		}).WithError(err).Info("Epoch validator failed to validate cfx_getBlockByBlockNumber (includeTxs = true)")

		return ctx, errors.WithMessagef(
			err, "failed to validate cfx_getBlockByBlockNumber (includeTxs = true) by number %v", blockNumber,
		)
	}

	return ctx, nil
}

// Validate `cfx_getBlockByBlockNumber` (includeTxs = false)
func (validator *EpochValidator) validateGetBlockSummaryByBlockNumber(
	ctx context.Context, epoch *types.Epoch,
) (context.Context, error) {
	block, err := validator.getFirstBlock(ctx, epoch)
	if err != nil || block == nil {
		return ctx, errors.WithMessage(
			err, "failed to get first block to validate cfx_getBlockByHash (includeTxs = false)",
		)
	}

	blockNumber := hexutil.Uint64(block.BlockNumber.ToInt().Uint64())

	var b1, b2 *types.BlockSummary
	var err1, err2 error

	fnCall := func() (interface{}, error) {
		if b1, err1 = validator.cfx.GetBlockSummaryByBlockNumber(blockNumber); err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query block summary from fullnode")
		}
		return b1, err1
	}

	infuraCall := func() (interface{}, error) {
		if b2, err2 = validator.infura.GetBlockSummaryByBlockNumber(blockNumber); err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query block summary from infura")
		}
		return b2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "blockNumer": blockNumber,
		}).WithError(err).Info("Epoch validator failed to validate cfx_getBlockByBlockNumber (includeTxs = false)")

		return ctx, errors.WithMessagef(
			err, "failed to validate cfx_getBlockByBlockNumber (includeTxs = false) by number %v", blockNumber,
		)
	}

	return ctx, nil
}

// Validate `cfx_getBlocksByEpoch`
func (validator *EpochValidator) validateGetBlocksByEpoch(
	ctx context.Context, epoch *types.Epoch,
) (context.Context, error) {
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

		return ctx, errors.WithMessagef(
			err, "failed to validate cfx_getBlocksByEpoch by epoch %v", epoch,
		)
	}

	return context.WithValue(ctx, ctxKeyCfxBlockHashes, bhs1), nil
}

// Validate `cfx_getBlockByEpochNumber` (includeTxs = true)
func (validator *EpochValidator) validateGetBlockByEpoch(
	ctx context.Context, epoch *types.Epoch,
) (context.Context, error) {
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
		return ctx, errors.WithMessagef(
			err, "failed to validate cfx_getBlockByEpochNumber (includeTxs = true) by epoch %v", epoch,
		)
	}

	return context.WithValue(ctx, ctxKeyCfxPivotBlock, b1), nil
}

// Validate `cfx_getBlockByEpochNumber` (includeTxs = false)
func (validator *EpochValidator) validateGetBlockSummaryByEpoch(
	ctx context.Context, epoch *types.Epoch,
) (context.Context, error) {
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

		return ctx, errors.WithMessagef(
			err, "failed to validate cfx_getBlockByEpochNumber (includeTxs = false) by epoch %v", epoch,
		)
	}

	return context.WithValue(ctx, ctxKeyCfxPivotBlockSummary, bs1), nil
}

// Validate `cfx_getLogs`
func (validator *EpochValidator) validateGetLogs(
	ctx context.Context, epoch *types.Epoch,
) (context.Context, error) {
	epochBigInt, _ := epoch.ToInt()
	epochNo := epochBigInt.Uint64()

	var someContractAddrs []cfxaddress.Address
	var someTopics [][]types.Hash

	if someReceipt, _ := validator.getSomeTransactionReceipt(ctx, epoch); someReceipt != nil {
		for _, log := range someReceipt.Logs {
			if len(someContractAddrs) >= 3 {
				break
			}

			someContractAddrs = append(someContractAddrs, log.Address)

			for i, topic := range log.Topics {
				if len(someTopics) <= i {
					someTopics = append(someTopics, []types.Hash{})
				}

				if len(topic) > 0 {
					someTopics[i] = append(someTopics[i], topic)
				}
			}
		}
	}

	logger := logrus.WithFields(logrus.Fields{
		"epochNo":           epochNo,
		"withContractAddrs": len(someContractAddrs),
		"withTopics":        len(someTopics),
	})

	// filter: epoch range
	filterByEpoch := types.LogFilter{
		FromEpoch: epoch, ToEpoch: epoch,
	}

	if err := validator.doValidateGetLogs(filterByEpoch); err != nil {
		logger.WithField("filterByEpoch", filterByEpoch).
			WithError(err).
			Info("Epoch validator failed to validate cfx_getLogs")
		return ctx, errors.WithMessagef(err, "failed to validate cfx_getLogs")
	}

	if len(someContractAddrs) > 0 { // add address and topics filter
		filterByEpoch.Address = someContractAddrs
		filterByEpoch.Topics = someTopics

		if err := validator.doValidateGetLogs(filterByEpoch); err != nil {
			logger.WithField("filterByEpochWithAddr", filterByEpoch).
				WithError(err).
				Info("Epoch validator failed to validate cfx_getLogs")

			return ctx, errors.WithMessagef(err, "failed to validate cfx_getLogs")
		}
	}

	// filter: blockhashes
	blockHashes, err := validator.getBlockHashes(ctx, epoch)
	if err != nil {
		logger.WithError(err).
			Info("Epoch validator failed to get block hashes to validate cfx_getLogs")
		return ctx, errors.WithMessage(err, "failed to get block hashes to validate cfx_getLogs")
	}

	filterByBlockHashes := types.LogFilter{
		BlockHashes: []types.Hash{
			blockHashes[len(blockHashes)-1], blockHashes[0],
		},
	}

	if err := validator.doValidateGetLogs(filterByBlockHashes); err != nil {
		logger.WithField("filterByBlockHashes", filterByBlockHashes).
			WithError(err).
			Info("Epoch validator failed to validate cfx_getLogs")
		return ctx, errors.WithMessagef(err, "failed to validate cfx_getLogs")
	}

	if len(someContractAddrs) > 0 { // add address and topics filter
		filterByBlockHashes.Address = someContractAddrs
		filterByBlockHashes.Topics = someTopics

		if err := validator.doValidateGetLogs(filterByBlockHashes); err != nil {
			logger.WithField("filterByBlockHashesWithAddr", filterByBlockHashes).
				WithError(err).
				Info("Epoch validator failed to validate cfx_getLogs")
			return ctx, errors.WithMessagef(err, "failed to validate cfx_getLogs")
		}
	}

	// filter: blocknumbers
	firstBlock, err := validator.getFirstBlock(ctx, epoch)
	if err != nil {
		logger.WithError(err).Info("Epoch validator failed to get first block for validating cfx_getLogs")
		return ctx, errors.WithMessage(err, "failed to get first block for validating cfx_getLogs")
	}

	pivotBlock, err := validator.getPivotBlock(ctx, epoch)
	if err != nil {
		logger.WithError(err).Info("Epoch validator failed to get pivot block for validating cfx_getLogs")
		return ctx, errors.WithMessage(err, "failed to get pivot block for validating cfx_getLogs")
	}

	// ensure test block number range within bound
	filterByBlockNumbers := types.LogFilter{
		FromBlock: firstBlock.BlockNumber,
		ToBlock:   pivotBlock.BlockNumber,
	}

	if err := validator.doValidateGetLogs(filterByBlockNumbers); err != nil {
		logger.WithField("filterByBlockNumbers", filterByBlockNumbers).
			WithError(err).
			Info("Epoch validator failed to validate cfx_getLogs")

		return ctx, errors.WithMessagef(err, "failed to validate cfx_getLogs")
	}

	if len(someContractAddrs) > 0 { // add address and topics filter
		filterByBlockNumbers.Address = someContractAddrs
		filterByBlockNumbers.Topics = someTopics

		if err := validator.doValidateGetLogs(filterByBlockNumbers); err != nil {
			logger.WithField("filterByBlockNumbersWithAddr", filterByBlockNumbers).
				WithError(err).
				Info("Epoch validator failed to validate cfx_getLogs")
			return ctx, errors.WithMessagef(err, "failed to validate cfx_getLogs")
		}
	}

	return ctx, nil
}

func (validator *EpochValidator) doValidateGetLogs(filter types.LogFilter) error {
	genCall := func(src string, client sdk.ClientOperator) func() (interface{}, error) {
		return func() (interface{}, error) {
			logs, err := client.GetLogs(filter)
			if err != nil {
				return logs, errors.WithMessagef(err, "failed to query logs from %v", src)
			}

			return validator.filterLogs(logs), nil
		}
	}

	fnCall := genCall("fullnode", validator.cfx)
	infuraCall := genCall("infura", validator.infura)

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil && !isValidationSkippedErr(err) {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi,
			"filter":    filter,
		}).WithError(err).Info("Epoch validator failed to validate cfx_getLogs")
		return err
	}

	return nil
}

func (validator *EpochValidator) filterLogs(logs []types.Log) []types.Log {
	filteredLogs := make([]types.Log, 0, len(logs))
	for _, log := range logs {
		// skip `log.TransactionIndex` field validation due to fullnode bug
		// TODO: remove if fullnode bug fixed
		log.TransactionIndex = nil

		epochNo := log.EpochNumber.ToInt().Uint64()
		if !blacklist.IsAddressBlacklisted(&log.Address, epochNo) {
			filteredLogs = append(filteredLogs, log)
		}
	}

	return filteredLogs
}

func (validator *EpochValidator) saveScanCursor() error {
	// Write last validated epoch to config file
	epochStr := strconv.FormatUint(atomic.LoadUint64(&validator.conf.EpochScanFrom), 10)
	if err := ioutil.WriteFile(validEpochFromNoFilePath, []byte(epochStr), fs.ModePerm); err != nil {
		logrus.WithError(err).Infof(
			"Epoch validator failed to write last validated epoch to file %v", validEpochFromNoFilePath,
		)

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

func isValidationSkippedErr(err error) bool {
	errs := multierr.Errors(err)
	for _, e := range errs {
		if errors.Is(e, errValidationSkipped) {
			return true
		}
	}

	return false
}

func (validator *EpochValidator) getBlockHashes(
	ctx context.Context, epoch *types.Epoch,
) ([]types.Hash, error) {
	if blockhashes, ok := ctx.Value(ctxKeyCfxBlockHashes).([]types.Hash); ok {
		return blockhashes, nil
	}

	return validator.cfx.GetBlocksByEpoch(epoch)
}

func (validator *EpochValidator) getPivotBlock(
	ctx context.Context, epoch *types.Epoch,
) (*types.Block, error) {
	if b, ok := ctx.Value(ctxKeyCfxPivotBlock).(*types.Block); ok {
		return b, nil
	}

	return validator.cfx.GetBlockByEpoch(epoch)
}

func (validator *EpochValidator) getFirstBlock(
	ctx context.Context, epoch *types.Epoch,
) (*types.Block, error) {
	if b, ok := ctx.Value(ctxKeyCfxFirstBlock).(*types.Block); ok {
		return b, nil
	}

	blockHashes, err := validator.getBlockHashes(ctx, epoch)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get block hashes")
	}

	return validator.cfx.GetBlockByHash(blockHashes[0])
}

func (validator *EpochValidator) getSomeTransaction(
	ctx context.Context, epoch *types.Epoch, executed bool,
) (*types.Transaction, error) {
	blockGetters := map[string]func(context.Context, *types.Epoch) (*types.Block, error){
		"first": validator.getFirstBlock, "pivot": validator.getPivotBlock,
	}

	for name, getter := range blockGetters {
		block, err := getter(ctx, epoch)
		if err != nil {
			return nil, errors.WithMessagef(err, "failed to get %v block", name)
		}

		if len(block.Transactions) > 0 {
			if !executed {
				return &block.Transactions[0], nil
			}

			for _, txn := range block.Transactions {
				if util.IsTxExecutedInBlock(&txn) {
					return &txn, nil
				}
			}
		}
	}

	return nil, nil
}

func (validator *EpochValidator) getSomeTransactionReceipt(
	ctx context.Context, epoch *types.Epoch,
) (*types.TransactionReceipt, error) {
	if rcpt, ok := ctx.Value(ctxKeyCfxSomeTransactionReceipt).(*types.TransactionReceipt); ok {
		return rcpt, nil
	}

	txn, err := validator.getSomeTransaction(ctx, epoch, true)
	if err != nil || txn == nil {
		return nil, errors.WithMessage(err, "failted to get some transaction")
	}

	return validator.cfx.GetTransactionReceipt(txn.Hash)
}
