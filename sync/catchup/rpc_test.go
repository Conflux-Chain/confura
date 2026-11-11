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
