package ethbridge

import (
	"os"
	"testing"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/conflux-chain/conflux-infura/rpc/cfxbridge"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/openweb3/web3go"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

var (
	ethHttpNode = "http://evmtestnet.confluxrpc.com"
	cfxHttpNode = "http://test.confluxrpc.com"

	ethClient *web3go.Client
	cfxClient sdk.ClientOperator

	ethNetworkId *uint64
)

func setup() error {
	var err error

	if ethClient, err = web3go.NewClient(ethHttpNode); err != nil {
		return errors.WithMessage(err, "failed to new web3go client")
	}

	if ethNetworkId, err = ethClient.Eth.ChainId(); err != nil {
		return errors.WithMessage(err, "failed to get eth chainid")
	}

	if cfxClient, err = sdk.NewClient(cfxHttpNode); err != nil {
		return errors.WithMessage(err, "failed to new cfx client")
	}

	return nil
}

func teardown() (err error) {
	if ethClient != nil {
		ethClient.Provider().Close()
	}

	if cfxClient != nil {
		cfxClient.Close()
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

func TestConvertBlockHeader(t *testing.T) {
	blockNum := rpc.BlockNumber(64630500)

	ethBlock, err := ethClient.Eth.BlockByNumber(blockNum, false)
	assert.NoError(t, err)

	t.Log("logsBloom for original eth block: ", ethBlock.LogsBloom)

	convertedCfxBlock := cfxbridge.ConvertBlockSummary(ethBlock, uint32(*ethNetworkId))
	t.Log("logsBloom for converted cfx block: ", convertedCfxBlock.DeferredLogsBloomHash)

	convertedEthBlock := ConvertBlockHeader(&convertedCfxBlock.BlockHeader, nil)
	t.Log("logsBloom for converted eth block: ", convertedEthBlock.LogsBloom)

	assert.Equal(t, ethBlock.LogsBloom, convertedEthBlock.LogsBloom)
}

func TestConvertReceipt(t *testing.T) {
	txHash := "0xff2438365f72360a0eb60faf217b4d2ea2cc3599d59f5141113b68a58802452c"
	ethTxHash := common.HexToHash(txHash)

	ethReceipt, err := ethClient.Eth.TransactionReceipt(ethTxHash)
	assert.NoError(t, err)

	convertedCfxReceipt := cfxbridge.ConvertReceipt(ethReceipt, uint32(*ethNetworkId))
	t.Log("logsBloom for converted cfx block: ", convertedCfxReceipt.LogsBloom)

	convertedEthReceipt := ConvertReceipt(convertedCfxReceipt, nil)
	t.Log("logsBloom for converted eth block: ", convertedEthReceipt.LogsBloom)

	assert.Equal(t, ethReceipt.LogsBloom, convertedEthReceipt.LogsBloom)
}
