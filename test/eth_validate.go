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

	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/types"

	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"
)

const (
	// random block diff to latest block for sampling validation
	samplingRandomBlockDiff = 100
)

var (
	validBlockFromNoFilePath = ".ethvno" // file path to read/write block number from where the validation will start
)

// EthValidConfig validation config provided to EthValidator
type EthValidConfig struct {
	FullnodeRpcEndpoint string        // Fullnode rpc endpoint used as benchmark
	InfuraRpcEndpoint   string        // Infura rpc endpoint used to be validated against
	ScanFromBlock       uint64        // the block to scan from
	ScanInterval        time.Duration // scan interval
	SamplingInterval    time.Duration // sampling interval
}

// EthValidator pulls eth block data from fullnode and infura endpoints, and then compares the
// eth block data to validate if the infura eth block data complies to fullnode.
type EthValidator struct {
	infura *web3go.Client  // infura rpc service client instance
	fn     *web3go.Client  // fullnode client instance
	conf   *EthValidConfig // validation configuration
}

func init() {
	if len(os.Getenv("HOME")) > 0 {
		validBlockFromNoFilePath = fmt.Sprintf("%v/.ethvno", os.Getenv("HOME"))
	}
}

func MustNewEthValidator(conf *EthValidConfig) *EthValidator {
	// Prepare fullnode && infura client instance
	endpoints := []string{conf.FullnodeRpcEndpoint, conf.InfuraRpcEndpoint}
	clientInsts := [2]*web3go.Client{}

	for i, endpoint := range endpoints {
		client, err := web3go.NewClient(endpoint)
		if err != nil {
			logrus.WithField("endpoint", endpoint).WithError(err).Fatal("Failed to new web3 client")
		}
		clientInsts[i] = client
	}

	validator := &EthValidator{
		fn:     clientInsts[0],
		infura: clientInsts[1],
		conf:   conf,
	}

	defer logrus.WithField("config", conf).Info("Test validation configurations loaded")

	if conf.ScanFromBlock != math.MaxUint64 {
		return validator
	}

	conf.ScanFromBlock = 1 // default scan from block #1

	// Read last validated block from config file, on which the validation will continue
	dat, err := ioutil.ReadFile(validBlockFromNoFilePath)
	if err != nil {
		logrus.WithError(err).Debugf(
			"Eth validator failed to load last validated block from file %v", validBlockFromNoFilePath,
		)
		return validator
	}

	datStr := strings.TrimSpace(string(dat))
	blockFrom, err := strconv.ParseUint(datStr, 10, 64)
	if err == nil {
		logrus.WithField("blockFrom", blockFrom).Infof(
			"ETH validator loaded last validated block from file %v", validBlockFromNoFilePath,
		)
		conf.ScanFromBlock = blockFrom
	} else {
		logrus.WithError(err).Debugf(
			"ETH validator failed to load last validated block from file %v", validBlockFromNoFilePath,
		)
	}

	return validator
}

func (validator *EthValidator) Run(ctx context.Context, wg *sync.WaitGroup) {
	logrus.WithField("blockFrom", validator.conf.ScanFromBlock).Info("ETH validator running to validate block data...")

	wg.Add(1)
	defer wg.Done()

	logger := logrus.WithFields(logrus.Fields{
		"fullNodeUrl": validator.conf.FullnodeRpcEndpoint,
		"infuraUrl":   validator.conf.InfuraRpcEndpoint,
	})

	// Randomly sampling nearhead blocks for validation.
	samplingTicker := time.NewTicker(validator.conf.SamplingInterval)
	defer samplingTicker.Stop()

	// Sequentially scaning blocks from specified blocks to latest block.
	scanTicker := time.NewTicker(validator.conf.ScanInterval)
	defer scanTicker.Stop()

	scanValidFailures := 0    // mark how many failure times for scanning validation
	maxScanValidFailures := 5 // the maximum failure times for scanning validation, once exceeded, panic will be triggered

	for {
		select {
		case <-ctx.Done():
			logrus.Info("ETH validator shutdown ok")
			return
		case <-samplingTicker.C:
			go func() { // randomly pick some nearhead block for validation test
				if err := validator.doSampling(); err != nil {
					logger.WithError(err).Error("ETH validator failed to do samping for block validation")
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

				errFunc("ETH validator Failed to do scanning for block validation")
				continue
			}

			scanValidFailures = 0 // clear scanning validation failure times
		}
	}
}

func (validator *EthValidator) doSampling() error {
	logrus.Debug("ETH validator ticking to sample nearhead block for validation...")

	// Fetch latest block number from fullnode
	block, err := validator.fn.Eth.BlockNumber()
	if err != nil {
		return errors.WithMessage(err, "failed to query latest block number")
	}

	// Shuffle block number by reduction of random number from latest block
	latestBlockNo := block.Uint64()

	randomDiff := util.RandUint64(samplingRandomBlockDiff)
	if latestBlockNo < randomDiff {
		randomDiff = 0
	}

	blockNo := latestBlockNo - randomDiff

	logrus.WithFields(logrus.Fields{
		"latestBlockNo": latestBlockNo, "shuffledBlockNo": blockNo,
	}).Debug("ETH validator sampled random nearhead block for validation")

	err = validator.validateEthBlock(blockNo)
	// Since nearhead block revert are of high possibility due to chain reorg,
	// we'd better do some retrying before determining the final validation result.
	for i := 1; i <= samplingValidationRetries && errors.Is(err, errResultNotMatched); i++ {
		time.Sleep(samplingValidationSleepDuration)
		err = validator.validateEthBlock(blockNo)
		logrus.WithField("block", blockNo).WithError(err).Infof("ETH validator sampling validation retried %v time(s)", i)
	}

	return errors.WithMessagef(err, "failed to validate block #%v", blockNo)
}

func (validator *EthValidator) doScanning(ticker *time.Ticker) error {
	logrus.WithField("blockFrom", validator.conf.ScanFromBlock).Debug("ETH validation ticking to scan for validation...")

	// Fetch latest block from fullnode
	block, err := validator.fn.Eth.BlockNumber()
	if err != nil {
		return errors.WithMessage(err, "failed to query the latest block")
	}

	// Already catch up to the latest block?
	maxBlockTo := block.Uint64()
	if validator.conf.ScanFromBlock > maxBlockTo {
		logrus.WithField("blockRange", citypes.EpochRange{
			EpochFrom: validator.conf.ScanFromBlock,
			EpochTo:   maxBlockTo,
		}).Debug("ETH validator scaning skipped due to catched up already")

		return nil
	}

	if err := validator.validateEthBlock(validator.conf.ScanFromBlock); err != nil {
		return errors.WithMessagef(err, "failed to validate block #%v", validator.conf.ScanFromBlock)
	}

	validator.conf.ScanFromBlock++

	if validator.conf.ScanFromBlock%5000 == 0 { // periodly save the scaning progress per 5000 blocks in case of data lost
		validator.saveScanCursor()
	}

	return nil
}

func (validator *EthValidator) validateEthBlock(blockNo uint64) error {
	logrus.WithField("blockNo", blockNo).Debug("ETH validator runs validating ETH block...")

	block, err := validator.validateGetBlockSummaryByNumber(blockNo)
	if err != nil {
		return err
	}

	if err = validator.validateGetBlockSummaryByHash(block.Hash); err != nil {
		return err
	}

	if err = validator.validateGetBlockByNumber(blockNo); err != nil {
		return err
	}

	if err = validator.validateGetBlockByHash(block.Hash); err != nil {
		return err
	}

	blockTxHashes := block.Transactions.Hashes()
	if len(blockTxHashes) == 0 {
		return nil
	}

	// randomly pick a block transaction for validation
	ri := util.RandUint64(uint64(len(blockTxHashes)))
	txh := blockTxHashes[ri]
	if err = validator.validateGetTransactionByHash(txh); err != nil {
		return err
	}

	if err = validator.validateGetTransactionReceipt(txh); err != nil {
		return err
	}

	if err = validator.validateGetLogs(blockNo, block.Hash); err != nil {
		return err
	}

	return nil
}

func (validator *EthValidator) doValidate(fnCall, infuraCall func() (interface{}, error)) (*matchInfo, error) {
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

// Validate eth_getBlockByNumber (includeTxs = false)
func (validator *EthValidator) validateGetBlockSummaryByNumber(blockNumer uint64) (*types.Block, error) {
	var b1, b2 *types.Block
	var err1, err2 error

	fnCall := func() (interface{}, error) {
		b1, err1 = validator.fn.Eth.BlockByNumber(rpc.BlockNumber(blockNumer), false)
		if err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query block summary from fullnode")
		}

		return b1, err1
	}

	infuraCall := func() (interface{}, error) {
		b2, err2 = validator.infura.Eth.BlockByNumber(rpc.BlockNumber(blockNumer), false)
		if err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query block summary from infura")
		}

		return b2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "blockNumer": blockNumer,
		}).WithError(err).Info("ETH validator failed to validate eth_getBlockByNumber (includeTxs = false)")
		return b1, errors.WithMessagef(err, "failed to validate eth_getBlockByNumber (includeTxs = false) by number %v", blockNumer)
	}

	return b1, nil
}

// Validate eth_getBlockByNumber (includeTxs = true)
func (validator *EthValidator) validateGetBlockByNumber(blockNumer uint64) error {
	fnCall := func() (interface{}, error) {
		b1, err1 := validator.fn.Eth.BlockByNumber(rpc.BlockNumber(blockNumer), true)
		if err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query block from fullnode")
		}
		return b1, err1
	}

	infuraCall := func() (interface{}, error) {
		b2, err2 := validator.infura.Eth.BlockByNumber(rpc.BlockNumber(blockNumer), true)
		if err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query block from infura")
		}
		return b2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "blockNumer": blockNumer,
		}).WithError(err).Info("ETH validator failed to validate eth_getBlockByNumber (includeTxs = true)")
		return errors.WithMessagef(err, "failed to validate eth_getBlockByNumber (includeTxs = true) by number %v", blockNumer)
	}

	return nil
}

// Validate eth_getTransactionReceipt
func (validator *EthValidator) validateGetTransactionReceipt(txHash common.Hash) error {
	genCall := func(src string, w3c *web3go.Client) func() (interface{}, error) {
		return func() (interface{}, error) {
			rcpt, err := w3c.Eth.TransactionReceipt(txHash)
			if err != nil {
				return rcpt, errors.WithMessagef(
					err, "failed to query transaction receipts from %v", src,
				)
			}

			return rcpt, nil
		}
	}

	fnCall := genCall("fullnode", validator.fn)
	infuraCall := genCall("infura", validator.infura)

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "txHash": txHash.Hex(),
		}).WithError(err).Info("ETH validator failed to validate eth_getTransactionReceipt")
		return errors.WithMessagef(err, "failed to validate eth_getTransactionReceipt by hash %v", txHash.Hex())
	}

	return nil
}

// Validate eth_getTransactionByHash
func (validator *EthValidator) validateGetTransactionByHash(txHash common.Hash) error {
	fnCall := func() (interface{}, error) {
		tx1, err1 := validator.fn.Eth.TransactionByHash(txHash)
		if err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query transaction from fullnode")
		}
		return tx1, err1
	}

	infuraCall := func() (interface{}, error) {
		tx2, err2 := validator.infura.Eth.TransactionByHash(txHash)
		if err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query transaction from infura")
		}
		return tx2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "txHash": txHash.Hex(),
		}).WithError(err).Info("ETH validator failed to validate eth_getTransactionByHash")
		return errors.WithMessagef(err, "failed to validate eth_getTransactionByHash by hash %v", txHash.Hex())
	}

	return nil
}

// Validate eth_getBlockByHash (includeTxs = true)
func (validator *EthValidator) validateGetBlockByHash(blockHash common.Hash) error {
	fnCall := func() (interface{}, error) {
		b1, err1 := validator.fn.Eth.BlockByHash(blockHash, true)
		if err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query block by hash from fullnode")
		}
		return b1, err1
	}

	infuraCall := func() (interface{}, error) {
		b2, err2 := validator.infura.Eth.BlockByHash(blockHash, true)
		if err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query block by hash from infura")
		}
		return b2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "blockHash": blockHash.Hex(),
		}).WithError(err).Info("ETH validator failed to validate eth_getBlockByHash (includeTxs = true)")
		return errors.WithMessagef(err, "failed to validate eth_getBlockByHash (includeTxs = true) by hash %v", blockHash)
	}

	return nil
}

// Validate eth_getBlockByHash (includeTxs = false)
func (validator *EthValidator) validateGetBlockSummaryByHash(blockHash common.Hash) error {
	fnCall := func() (interface{}, error) {
		bs1, err1 := validator.fn.Eth.BlockByHash(blockHash, false)
		if err1 != nil {
			err1 = errors.WithMessage(err1, "failed to query block summary by hash from fullnode")
		}
		return bs1, err1
	}

	infuraCall := func() (interface{}, error) {
		bs2, err2 := validator.infura.Eth.BlockByHash(blockHash, false)
		if err2 != nil {
			err2 = errors.WithMessage(err2, "failed to query block summary by hash from infura")
		}
		return bs2, err2
	}

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "blockHash": blockHash.Hex(),
		}).WithError(err).Info("ETH validator failed to validate eth_getBlockByHash (includeTxs = false)")
		return errors.WithMessagef(err, "failed to validate eth_getBlockByHash (includeTxs = false) by hash %v", blockHash.Hex())
	}

	return nil
}

// Validate eth_getLogs
func (validator *EthValidator) validateGetLogs(blockNo uint64, blockHash common.Hash) error {
	logger := logrus.WithFields(logrus.Fields{
		"blockNo": blockNo, "blockHash": blockHash.Hex(),
	})

	// filter: blocknumbers
	fromBn, toBn, limitSize := int64(blockNo), int64(blockNo), uint(maxLogsLimit)
	filterByBlockNums := types.FilterQuery{
		FromBlock: (*rpc.BlockNumber)(&fromBn),
		ToBlock:   (*rpc.BlockNumber)(&toBn),
		Limit:     &limitSize,
	}

	if err := validator.doValidateGetLogs(&filterByBlockNums); err != nil {
		logger.WithField(
			"filterByBlockNums", filterByBlockNums,
		).WithError(err).Info("ETH validator failed to validate eth_getLogs")
		return errors.WithMessagef(err, "failed to validate eth_getLogs")
	}

	// filter: blockhash
	filterByBlockHash := types.FilterQuery{
		BlockHash: &blockHash,
		Limit:     &limitSize,
	}

	if err := validator.doValidateGetLogs(&filterByBlockHash); err != nil {
		logger.WithField(
			"filterByBlockHash", filterByBlockHash,
		).WithError(err).Info("ETH validator failed to validate eth_getLogs")
		return errors.WithMessagef(err, "failed to validate eth_getLogs")
	}

	// TODO add more logFilter for testing

	return nil
}

func (validator *EthValidator) doValidateGetLogs(filter *types.FilterQuery) error {
	genCall := func(src string, w3c *web3go.Client) func() (interface{}, error) {
		return func() (interface{}, error) {
			logs, err := w3c.Eth.Logs(*filter)
			if err != nil {
				return logs, errors.WithMessagef(err, "failed to query logs from %v", src)
			}

			return logs, nil
		}
	}

	fnCall := genCall("fullnode", validator.fn)
	infuraCall := genCall("infura", validator.infura)

	mi, err := validator.doValidate(fnCall, infuraCall)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"matchInfo": mi, "filter": filter,
		}).WithError(err).Info("Epoch validator failed to validate eth_getLogs")
		return err
	}

	return nil
}

func (validator *EthValidator) saveScanCursor() error {
	// Write last validated epoch to config file
	epochStr := strconv.FormatUint(atomic.LoadUint64(&validator.conf.ScanFromBlock), 10)
	if err := ioutil.WriteFile(validBlockFromNoFilePath, []byte(epochStr), fs.ModePerm); err != nil {
		logrus.WithError(err).Infof(
			"ETH validator failed to write last validated block to file %v", validBlockFromNoFilePath,
		)

		return err
	}

	return nil
}

func (validator *EthValidator) Destroy() {
	// Save scaning cursor
	validator.saveScanCursor()
}
