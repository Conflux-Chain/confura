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
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	// EVM space
	ethClient     *web3go.Client
	ethFilterCrit = &types.FilterQuery{Addresses: []common.Address{
		common.HexToAddress("0xfee2359f47617058ce4138cde54bf55fbca0be4b"),
		common.HexToAddress("0x73d95c9b1b3d4acc22847aa2985521f2adde5988"),
		common.HexToAddress("0xfee2359f47617058ce4138cde54bf55fbca0be4b"),
		common.HexToAddress("0x2ed3dddae5b2f321af0806181fbfa6d049be47d8"),
	}}

	// Core space
	cfxClient     *sdk.Client
	cfxFilterCrit = cfxtypes.LogFilter{
		Address: []cfxtypes.Address{
			cfxaddress.MustNewFromBase32("cfxtest:achs3nehae0j6ksvy1bhrffsh1rtfrw1f6w1kzv46t"),
			cfxaddress.MustNewFromBase32("cfxtest:acejjfa80vj06j2jgtz9pngkv423fhkuxj786kjr61"),
			cfxaddress.MustNewFromBase32("cfxtest:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaajh3dw3ctn"),
			cfxaddress.MustNewFromBase32("cfxtest:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaa2eaeg85p5"),
		},
	}
)

// Please set the following environments before running the validity test:
// `TEST_CFX_CLIENT_ENDPOINT`: Core space JSON-RPC endpoint to construct sdk client.
// `TEST_ETH_CLIENT_ENDPOINT`: EVM space JSON-RPC endpoint to construct sdk client.
//
// Also run the test case with timeout disabled as follows if long running validation is tended:
// go test -test.timeout 0 -v -run ^Test[Cfx|Eth]FilterDataValidity$ github.com/Conflux-Chain/confura/virtualfilter

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

func setup() error {
	endpoint := os.Getenv("TEST_ETH_CLIENT_ENDPOINT")
	if len(endpoint) > 0 {
		ethClient = rpcutil.MustNewEthClient(
			endpoint, rpcutil.WithClientRequestTimeout(15*time.Second),
		)
	}

	endpoint = os.Getenv("TEST_CFX_CLIENT_ENDPOINT")
	if len(endpoint) > 0 {
		cfxClient = rpcutil.MustNewCfxClient(
			endpoint, rpcutil.WithClientRequestTimeout(15*time.Second),
		)
	}

	return nil
}

func teardown() (err error) {
	if ethClient != nil {
		ethClient.Provider().Close()
	}

	if cfxClient != nil {
		cfxClient.Provider().Close()
	}

	return nil
}

// Consistantly polls changed logs from filter API to accumulate event logs from a `finalized` perspective,
// then fetches corresponding event logs  with `cfx_getLogs` to validate the correctness of polled and
// fetched result data.
func TestCfxFilterDataValidity(t *testing.T) {
	if cfxClient == nil {
		t.SkipNow()
	}

	cfxVfChain := newCfxFilterChain(500)

	fid, err := cfxClient.Filter().NewFilter(cfxFilterCrit)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to new cfx log filter")
	}

	vheadNode := &cfxVfChain.root
	vheadEpochNum := int64(-1)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		fc, err := cfxClient.Filter().GetFilterChanges(*fid)
		if err != nil {
			logrus.WithError(err).Error("Failed to call `cfx_getFilterChanges`")
			continue
		}

		blockInfos := make(map[cfxtypes.Hash]uint64)
		for i := range fc.Logs {
			if fc.Logs[i].IsRevertLog() {
				continue
			}

			if !util.IncludeCfxLogAddrs(fc.Logs[i].Log, cfxFilterCrit.Address) {
				fcJsonStr, _ := json.Marshal(fc)
				logrus.WithField("filterChanges", string(fcJsonStr)).
					WithError(err).
					Fatal("Filter changed logs not met filter criterion")
			}

			bn := fc.Logs[i].Log.EpochNumber.ToInt().Uint64()
			blockInfos[*fc.Logs[i].Log.BlockHash] = bn
		}

		logrus.WithFields(logrus.Fields{
			"blockInfos": blockInfos,
			"numLogs":    len(fc.Logs),
			"numHashes":  len(fc.Hashes),
		}).Info("Filter changes polled")

		if len(fc.Logs) == 0 {
			continue
		}

		// merge the filter changes
		if err = cfxVfChain.merge(fc); err != nil {
			// logging the invalid polled filter changes for debug
			fcJsonStr, _ := json.Marshal(fc)
			logrus.WithField("filterChanges", string(fcJsonStr)).
				WithError(err).
				Fatal("Failed to merge filter changes")
		}

		finEpochNumber, err := cfxClient.GetEpochNumber(cfxtypes.EpochLatestFinalized)
		if err != nil {
			logrus.WithError(err).Error("Failed to get finalized epoch number")
			continue
		}

		finEpochNum := finEpochNumber.ToInt().Uint64()
		valStartEpochNum, valEndEpochNum := uint64(math.MaxUint64), uint64(0)

		for curN := vheadNode.getNext(); curN != nil; {
			fepoch := curN.filterNodable.(*cfxFilterEpoch)
			if fepoch.epochNum > finEpochNum { // caught up?
				break
			}

			vheadEpochNum = int64(fepoch.epochNum)

			if len(fepoch.logs) == 0 { // pruned?
				valStartEpochNum, valEndEpochNum = uint64(math.MaxUint64), uint64(0)
			} else {
				valStartEpochNum = util.MinUint64(valStartEpochNum, fepoch.epochNum)
				valEndEpochNum = fepoch.epochNum
			}

			vheadNode = curN
			curN = curN.getNext()
		}

		logger := logrus.WithFields(logrus.Fields{
			"vheadEpochNum":    vheadEpochNum,
			"valStartEpochNum": valStartEpochNum,
			"valEndEpochNum":   valEndEpochNum,
			"finEpochNum":      finEpochNum,
		})

		if valStartEpochNum > valEndEpochNum {
			logger.Info("Skip validation")
			continue
		}

		logs, err := cfxClient.GetLogs(cfxtypes.LogFilter{
			FromEpoch: cfxtypes.NewEpochNumberUint64(valStartEpochNum),
			ToEpoch:   cfxtypes.NewEpochNumberUint64(uint64(valEndEpochNum)),
			Address:   cfxFilterCrit.Address,
		})

		if err != nil {
			logger.WithError(err).Error("Failed to get logs")
			continue
		}

		hash2Logs := make(map[cfxtypes.Hash][]cfxtypes.Log)
		for i := range logs {
			bh := logs[i].BlockHash
			hash2Logs[*bh] = append(hash2Logs[*bh], logs[i])
		}

		for bh, vlogs := range hash2Logs {
			node, ok := cfxVfChain.hashToNodes[bh.String()]
			if !ok {
				logger.WithField("blockHash", bh).Fatal("Validation failed due to epoch not found")
			}

			fepoch := node.filterNodable.(*cfxFilterEpoch)

			if !reflect.DeepEqual(fepoch.logs, vlogs) {
				changeLogs, _ := json.Marshal(fepoch.logs)
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

// Consistantly polls changed logs from filter API to accumulate event logs from a `finalized` perspective,
// then fetches corresponding event logs  with `eth_getLogs` to validate the correctness of polled and
// fetched result data.
func TestEthFilterDataValidity(t *testing.T) {
	if ethClient == nil {
		t.SkipNow()
	}

	ethVfChain := newEthFilterChain(500)

	fid, err := ethClient.Filter.NewLogFilter(ethFilterCrit)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to new eth log filter")
	}

	vheadNode := &ethVfChain.root
	vheadBlockNum := int64(-1)

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
			if !util.IncludeEthLogAddrs(&fc.Logs[i], ethFilterCrit.Addresses) {
				fcJsonStr, _ := json.Marshal(fc)
				logrus.WithField("filterChanges", string(fcJsonStr)).
					WithError(err).
					Fatal("Filter changed logs not met filter criterion")
			}
		}

		// merge the filter changes
		if err = ethVfChain.merge(fc); err != nil {
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
		valStartBlockNum, valEndBlockNum := uint64(math.MaxUint64), uint64(0)

		for curN := vheadNode.getNext(); curN != nil; {
			fblock := curN.filterNodable.(*ethFilterBlock)
			if fblock.blockNum > finBlockNum { // caught up?
				break
			}

			vheadBlockNum = int64(fblock.blockNum)

			if len(fblock.logs) == 0 { // pruned?
				valStartBlockNum, valEndBlockNum = uint64(math.MaxUint64), uint64(0)
			} else {
				valStartBlockNum = util.MinUint64(valStartBlockNum, fblock.blockNum)
				valEndBlockNum = fblock.blockNum
			}

			vheadNode = curN
			curN = curN.getNext()
		}

		logger := logrus.WithFields(logrus.Fields{
			"vHeadBlockNum":      vheadBlockNum,
			"valStartBlockNum":   valStartBlockNum,
			"valEndBlockNum":     valEndBlockNum,
			"finializedBlockNum": finBlockNum,
		})

		if valStartBlockNum > valEndBlockNum {
			logger.Info("Skip validation")
			continue
		}

		from, to := rpc.BlockNumber(valStartBlockNum), rpc.BlockNumber(valEndBlockNum)
		logs, err := ethClient.Eth.Logs(types.FilterQuery{
			FromBlock: &from, ToBlock: &to, Addresses: ethFilterCrit.Addresses,
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
			node, ok := ethVfChain.hashToNodes[bh.String()]
			if !ok {
				logger.WithField("blockHash", bh).Fatal("Validation failed due to block not found")
			}

			fblock := node.filterNodable.(*ethFilterBlock)

			if !reflect.DeepEqual(fblock.logs, vlogs) {
				changeLogs, _ := json.Marshal(fblock.logs)
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
