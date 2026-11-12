package sync

import (
	"context"
	"math/big"
	"testing"

	"github.com/Conflux-Chain/confura/sync/election"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/go-rpc-provider/interfaces"
	"github.com/openweb3/web3go"
	et "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

type syncHeadProvider struct {
	interfaces.Provider
	block *et.Block
	calls int
}

func (p *syncHeadProvider) CallContext(_ context.Context, out interface{}, method string, _ ...interface{}) error {
	p.calls++
	*out.(**et.Block) = p.block
	return nil
}
func (*syncHeadProvider) Close() {}

func TestEthSyncRejectsMalformedSafeHead(t *testing.T) {
	for _, tc := range []struct {
		name  string
		block *et.Block
	}{
		{"null", nil},
		{"missing-number", &et.Block{Hash: common.HexToHash("0x1")}},
		{"negative-number", &et.Block{Hash: common.HexToHash("0x1"), Number: big.NewInt(-1)}},
		{"overflow-number", &et.Block{Hash: common.HexToHash("0x1"), Number: new(big.Int).Lsh(big.NewInt(1), 64)}},
		{"missing-hash", &et.Block{Number: big.NewInt(1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := &syncHeadProvider{block: tc.block}
			s := &EthSyncer{w3cs: []*web3go.Client{web3go.NewClientWithProvider(provider)}, elm: election.NewNoopLeaderManager()}
			done, err := s.syncOnce(context.Background())
			require.ErrorContains(t, err, "invalid empty block")
			require.False(t, done)
			require.Equal(t, 1, provider.calls, "reject the response before any database access")
		})
	}
}
