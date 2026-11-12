package catchup

import (
	"context"
	"fmt"
	"github.com/openweb3/go-rpc-provider/interfaces"
	"github.com/openweb3/web3go"
	et "github.com/openweb3/web3go/types"
	"math/big"
	"testing"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	ct "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/stretchr/testify/require"
)

type rangeNode struct {
	sdk.ClientOperator
	asked []uint64
}

func (n *rangeNode) GetBlocksByEpoch(e *ct.Epoch) ([]ct.Hash, error) {
	v, _ := e.ToInt()
	n.asked = append(n.asked, v.Uint64())
	return []ct.Hash{ct.Hash(fmt.Sprintf("0x%064x", v.Uint64()))}, nil
}
func (n *rangeNode) GetBlockByHashWithPivotAssumption(h, p ct.Hash, e hexutil.Uint64) (ct.Block, error) {
	return ct.Block{BlockHeader: ct.BlockHeader{Hash: h, EpochNumber: ct.NewBigInt(uint64(e)), GasUsed: ct.NewBigInt(0)}}, nil
}
func TestCoreQueryRange(t *testing.T) {
	n := &rangeNode{}
	result, err := NewCoreRpcClient(n).QueryChainData(context.Background(), 1, 3)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3}, n.asked)
	for i, d := range result {
		require.EqualValues(t, i+1, d.Number())
	}
}

type rpcProvider struct {
	interfaces.Provider
	call func(context.Context, interface{}, string, ...interface{}) error
}

func (p *rpcProvider) CallContext(ctx context.Context, out interface{}, method string, args ...interface{}) error {
	return p.call(ctx, out, method, args...)
}
func (*rpcProvider) Close() {}

func TestEvmFinalizationResponse(t *testing.T) {
	for _, block := range []*et.Block{nil, {}, {Number: big.NewInt(0)}, {Number: big.NewInt(42)}} {
		c := NewEvmRpcClient(web3go.NewClientWithProvider(&rpcProvider{call: func(_ context.Context, out interface{}, method string, _ ...interface{}) error {
			require.Equal(t, "eth_getBlockByNumber", method)
			*out.(**et.Block) = block
			return nil
		}}))
		status, err := c.GetFinalizationStatus(context.Background())
		if block == nil || block.Number == nil {
			require.ErrorContains(t, err, "invalid finalized block")
			require.Nil(t, status)
		} else {
			require.NoError(t, err)
			require.Equal(t, block.Number.Uint64(), status.LatestFinalized)
		}
	}
}

func evmLogFixture() (*et.Block, []et.Log) {
	bh, th := common.HexToHash("0xabc"), common.HexToHash("0x123")
	block := &et.Block{Number: big.NewInt(1), Hash: bh, Transactions: *et.NewTxOrHashListByTxs([]et.TransactionDetail{{Hash: th, Value: big.NewInt(0)}})}
	return block, []et.Log{{BlockNumber: 1, BlockHash: bh, TxHash: th, Index: 0, Topics: []common.Hash{common.HexToHash("0x456")}, Address: common.HexToAddress("0x1")}}
}

func fixtureEvmClient(block *et.Block, logs []et.Log) *EvmRpcClient {
	return NewEvmRpcClient(web3go.NewClientWithProvider(&rpcProvider{call: func(_ context.Context, out interface{}, method string, _ ...interface{}) error {
		switch method {
		case "eth_getBlockByNumber":
			*out.(**et.Block) = block
		case "eth_getLogs":
			*out.(*[]et.Log) = logs
		default:
			return fmt.Errorf("unexpected method %s", method)
		}
		return nil
	}}))
}

func TestEvmBoostLogIdentity(t *testing.T) {
	for _, kind := range []string{"valid", "genesis", "empty", "fork", "height", "nil-number", "null-block", "missing-tx", "tx-index", "failed-tx", "removed", "duplicate", "range"} {
		t.Run(kind, func(t *testing.T) {
			block, logs := evmLogFixture()
			from := uint64(1)
			switch kind {
			case "genesis":
				from, block.Number, logs[0].BlockNumber = 0, big.NewInt(0), 0
			case "null-block":
				block = nil
			case "failed-tx":
				status := uint64(0)
				block.Transactions.Transactions()[0].Status = &status
			case "empty":
				logs = nil
			case "fork":
				logs[0].BlockHash = common.HexToHash("0xbad")
			case "height":
				block.Number = big.NewInt(2)
			case "nil-number":
				block.Number = nil
			case "missing-tx":
				logs[0].TxHash = common.HexToHash("0xbad")
			case "tx-index":
				logs[0].TxIndex = 1
			case "removed":
				logs[0].Removed = true
			case "duplicate":
				logs = append(logs, logs[0])
			case "range":
				logs[0].BlockNumber = 2
			}
			data, err := fixtureEvmClient(block, logs).BoostQueryChainData(context.Background(), from, from)
			if kind != "valid" && kind != "genesis" && kind != "empty" {
				require.Error(t, err)
				require.Nil(t, data)
				return
			}
			require.NoError(t, err)
			if kind == "empty" {
				require.Empty(t, data[0].Receipts)
				return
			}
			require.Len(t, data[0].ExtractLogs(), 1)
		})
	}
}

type coreLogNode struct {
	sdk.ClientOperator
	block ct.Block
	logs  []ct.Log
}

func (n *coreLogNode) GetLogs(ct.LogFilter) ([]ct.Log, error) { return n.logs, nil }
func (n *coreLogNode) GetBlocksByEpoch(*ct.Epoch) ([]ct.Hash, error) {
	return []ct.Hash{n.block.Hash}, nil
}
func (n *coreLogNode) GetBlockByHash(ct.Hash) (*ct.Block, error) {
	return &n.block, nil
}
func (n *coreLogNode) GetBlockByEpoch(*ct.Epoch) (*ct.Block, error) {
	return &n.block, nil
}
func TestCoreBoostLogIdentity(t *testing.T) {
	for _, kind := range []string{"valid", "genesis", "deferred-index", "nil-epoch", "nil-hash", "fork", "wrong-block", "missing-tx", "failed-tx", "duplicate", "range"} {
		t.Run(kind, func(t *testing.T) {
			bh, th := ct.Hash("0xabc"), ct.Hash("0x123")
			success := hexutil.Uint64(0)
			n := &coreLogNode{block: ct.Block{BlockHeader: ct.BlockHeader{Hash: bh, EpochNumber: ct.NewBigInt(1), BlockNumber: ct.NewBigInt(1)}, Transactions: []ct.Transaction{{Hash: th, BlockHash: &bh, Status: &success}}}, logs: []ct.Log{{EpochNumber: ct.NewBigInt(1), BlockHash: &bh, TransactionHash: &th, LogIndex: ct.NewBigInt(0), TransactionIndex: ct.NewBigInt(0)}}}
			from := uint64(1)
			switch kind {
			case "genesis":
				from, n.block.EpochNumber, n.block.BlockNumber, n.logs[0].EpochNumber = 0, ct.NewBigInt(0), ct.NewBigInt(0), ct.NewBigInt(0)
			case "deferred-index":
				n.logs[0].TransactionIndex = ct.NewBigInt(100)
			case "nil-epoch":
				n.logs[0].EpochNumber = nil
			case "nil-hash":
				n.logs[0].BlockHash = nil
			case "fork":
				h := ct.Hash("0xbad")
				n.logs[0].BlockHash = &h
			case "wrong-block":
				n.block.EpochNumber = ct.NewBigInt(2)
			case "missing-tx":
				n.block.Transactions = nil
			case "failed-tx":
				success = 1
			case "duplicate":
				n.logs = append(n.logs, n.logs[0])
			case "range":
				n.logs[0].EpochNumber = ct.NewBigInt(2)
			}
			data, err := NewCoreRpcClient(n).BoostQueryChainData(context.Background(), from, from)
			if kind != "valid" && kind != "genesis" && kind != "deferred-index" {
				require.Error(t, err)
				require.Nil(t, data)
				return
			}
			require.NoError(t, err)
			require.Len(t, data[0].ExtractLogs(), 1)
		})
	}
}

func TestEvmBoostReceiptExecutionStatus(t *testing.T) {
	block, logs := evmLogFixture()
	data, err := fixtureEvmClient(block, logs).BoostQueryChainData(context.Background(), 1, 1)
	require.NoError(t, err)
	require.EqualValues(t, 1, *data[0].Receipts[logs[0].TxHash].Status)
	require.True(t, data[0].ExtractBlocks()[0].Transactions()[0].Executed(), "synthesized receipt must preserve events through the store adapter")
}
