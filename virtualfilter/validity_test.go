package virtualfilter

import (
	"encoding/json"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/util"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	ethClient   *web3go.Client
	ethHttpNode = "http://evmtestnet.confluxrpc.com"

	filterCrit = &types.FilterQuery{Addresses: []common.Address{
		common.HexToAddress("0xfee2359f47617058ce4138cde54bf55fbca0be4b"),
		common.HexToAddress("0x73d95c9b1b3d4acc22847aa2985521f2adde5988"),
		common.HexToAddress("0xfee2359f47617058ce4138cde54bf55fbca0be4b"),
		common.HexToAddress("0x2ed3dddae5b2f321af0806181fbfa6d049be47d8"),
	}}

	vfchain = NewFilterChain(500)
)

func setup() error {
	var err error

	ethClient, err = rpcutil.NewEthClient(ethHttpNode, rpcutil.WithClientRequestTimeout(15*time.Second))
	if err != nil {
		return errors.WithMessage(err, "failed to new web3go client")
	}

	return nil
}

func teardown() (err error) {
	if ethClient != nil {
		ethClient.Provider().Close()
	}

	return nil
}

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		panic(errors.WithMessage(err, "failed to setup"))
	}

	code := m.Run()

	if err := teardown(); err != nil {
		panic(errors.WithMessage(err, "failed to tear down"))
	}

	os.Exit(code)
}

// Instantly polls changed logs from filter API to accumulate event logs from a `finalized` perspective,
// then fetches corresponding event logs  with `eth_getLogs` to validate the correctness of polled and
// fetched result data.
// Please run this test case with timeout disabled if long running validation is tended:
// `go test -test.timeout 0 -v -run ^TestFilterDataValidity$ github.com/Conflux-Chain/confura/virtualfilter`
func TestFilterDataValidity(t *testing.T) {
	fid, err := ethClient.Filter.NewLogFilter(filterCrit)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to new log filter")
	}

	vheadNode := &vfchain.root

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		fc, err := ethClient.Filter.GetFilterChanges(*fid)
		if err != nil {
			logrus.WithError(err).Error("Failed to call `eth_getFilterChanges`")
			continue
		}

		blockInfos := make(map[common.Hash]uint64)
		for i := range fc.Logs {
			blockInfos[fc.Logs[i].BlockHash] = fc.Logs[i].BlockNumber
		}

		logrus.WithFields(logrus.Fields{
			"blockInfos": blockInfos,
			"numLogs":    len(fc.Logs),
			"numHashes":  len(fc.Hashes),
		}).Info("Filter changes polled")

		if len(fc.Logs) == 0 {
			continue
		}

		for i := range fc.Logs {
			if !util.IncludeEthLogAddrs(&fc.Logs[i], filterCrit.Addresses) {
				fcJsonStr, _ := json.Marshal(fc)
				logrus.WithField("filterChanges", string(fcJsonStr)).
					WithError(err).
					Fatal("Filter changed logs not met filter criterion")
			}
		}

		// merge the filter changes
		if err = vfchain.Merge(fc); err != nil {
			// logging the invalid polled filter changes for debug
			fcJsonStr, _ := json.Marshal(fc)
			logrus.WithField("filterChanges", string(fcJsonStr)).
				WithError(err).
				Fatal("Failed to merge filter changes")
		}

		finblock, err := ethClient.Eth.BlockByNumber(types.FinalizedBlockNumber, false)
		if err != nil {
			logrus.WithError(err).Error("Failed to get finalized block")
			continue
		}

		finBlockNum := finblock.Number.Uint64()
		fromBlockNum, toBlockNum := uint64(math.MaxUint64), uint64(0)

		vheadBlockNum := int64(-1)
		if vheadNode.FilterBlock != nil {
			vheadBlockNum = int64(vheadNode.blockNum)
		}

		for cur := vheadNode.Next(); cur != nil && cur.blockNum <= finBlockNum; {
			if len(cur.logs) == 0 { // pruned?
				fromBlockNum, toBlockNum = uint64(math.MaxUint64), uint64(0)
			} else {
				fromBlockNum, toBlockNum = util.MinUint64(fromBlockNum, cur.blockNum), cur.blockNum
			}

			vheadNode = cur
			cur = cur.Next()
		}

		logger := logrus.WithFields(logrus.Fields{
			"vHeadBlockNum":      vheadBlockNum,
			"fromBlockNum":       fromBlockNum,
			"toBlockNum":         toBlockNum,
			"finializedBlockNum": finBlockNum,
		})

		if fromBlockNum > toBlockNum {
			logger.Info("Skip validation")
			continue
		}

		from, to := rpc.BlockNumber(fromBlockNum), rpc.BlockNumber(toBlockNum)
		logs, err := ethClient.Eth.Logs(types.FilterQuery{
			FromBlock: &from, ToBlock: &to, Addresses: filterCrit.Addresses,
		})
		if err != nil {
			logger.WithError(err).Error("Failed to get logs")
			continue
		}

		hash2Logs := make(map[common.Hash][]types.Log)
		for i := range logs {
			bh := logs[i].BlockHash
			hash2Logs[bh] = append(hash2Logs[bh], logs[i])
		}

		for bh, vlogs := range hash2Logs {
			node, ok := vfchain.hashToNodes[bh]
			if !ok {
				logger.WithField("blockHash", bh).Fatal("Validation failed due to block not found")
			}

			if !reflect.DeepEqual(node.logs, vlogs) {
				changeLogs, _ := json.Marshal(node.logs)
				getLogs, _ := json.Marshal(vlogs)

				logger.WithFields(logrus.Fields{
					"blockHash":  bh,
					"changeLogs": string(changeLogs),
					"getLogs":    string(getLogs),
				}).Fatal("Validation failed due to not matched")
			}
		}

		logger.Info("Validation passed")
	}
}
