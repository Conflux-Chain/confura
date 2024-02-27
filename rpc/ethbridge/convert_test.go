package ethbridge

import (
	"os"
	"testing"

	"github.com/Conflux-Chain/confura/rpc/cfxbridge"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

var (
	ethClient    *web3go.Client
	ethNetworkId *uint64
)

// Please set the following enviroment before start:
// `TEST_ETH_CLIENT_ENDPOINT`: EVM space JSON-RPC endpoint to construct sdk client.

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		panic(errors.WithMessage(err, "failed to setup"))
	}

	code := 0
	if ethClient != nil {
		code = m.Run()
	}

	if err := teardown(); err != nil {
		panic(errors.WithMessage(err, "failed to tear down"))
	}

	os.Exit(code)
}

func setup() error {
	ethHttpNode := os.Getenv("TEST_ETH_CLIENT_ENDPOINT")
	if len(ethHttpNode) == 0 {
		return nil
	}

	ethClient = web3go.MustNewClient(ethHttpNode)
	chainId, err := ethClient.Eth.ChainId()
	if err != nil {
		return errors.WithMessage(err, "failed to get eth chainid")
	}

	ethNetworkId = chainId
	return nil
}

func teardown() (err error) {
	if ethClient != nil {
		ethClient.Provider().Close()
	}

	return nil
}

func TestConvertBlockHeader(t *testing.T) {
	blockNum := ethTypes.BlockNumber(64630500)

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
	txHash := "0x4a55b1def27caf6c262f2359d78a33eee4e05e390d55f8845486cd5d1e7d3a20"
	ethTxHash := common.HexToHash(txHash)

	ethReceipt, err := ethClient.Eth.TransactionReceipt(ethTxHash)
	assert.NoError(t, err)

	convertedCfxReceipt := cfxbridge.ConvertReceipt(ethReceipt, uint32(*ethNetworkId))
	t.Log("logsBloom for converted cfx block: ", convertedCfxReceipt.LogsBloom)

	convertedEthReceipt := ConvertReceipt(convertedCfxReceipt, nil)
	t.Log("logsBloom for converted eth block: ", convertedEthReceipt.LogsBloom)

	assert.Equal(t, ethReceipt.LogsBloom, convertedEthReceipt.LogsBloom)
}
