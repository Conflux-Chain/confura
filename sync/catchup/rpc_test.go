package catchup

import (
	"context"
	"fmt"
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
