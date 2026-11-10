package store

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/go-rpc-provider/interfaces"
	"github.com/openweb3/web3go"
	ethtypes "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

type ethContextProvider struct {
	interfaces.Provider
	call func(context.Context, interface{}, string, ...interface{}) error
}

func (p *ethContextProvider) CallContext(ctx context.Context, out interface{}, method string, args ...interface{}) error {
	return p.call(ctx, out, method, args...)
}

type logsOnlyFilter struct{}

func (logsOnlyFilter) IsBlockDisabled() bool   { return true }
func (logsOnlyFilter) IsTxnDisabled() bool     { return true }
func (logsOnlyFilter) IsReceiptDisabled() bool { return true }
func (logsOnlyFilter) IsLogDisabled() bool     { return false }

func TestEthDataRequestContext(t *testing.T) {
	for _, stage := range []string{"block", "serial receipt", "logs optimization"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if stage != "logs optimization" {
				cancel()
			}
			reached := false
			provider := &ethContextProvider{call: func(got context.Context, out interface{}, method string, args ...interface{}) error {
				if stage == "logs optimization" && method == "eth_getBlockByNumber" {
					require.NoError(t, got.Err())
					txs := []ethtypes.TransactionDetail{{Hash: common.HexToHash("0x01")}}
					*out.(**ethtypes.Block) = &ethtypes.Block{Number: big.NewInt(1), Transactions: *ethtypes.NewTxOrHashListByTxs(txs)}
					cancel()
					return nil
				}
				reached = true
				require.ErrorIs(t, got.Err(), context.Canceled)
				return got.Err()
			}}
			client := web3go.NewClientWithProvider(provider)
			var err error
			if stage == "serial receipt" {
				block := &ethtypes.Block{Number: big.NewInt(1), Transactions: *ethtypes.NewTxOrHashListByHashes([]common.Hash{common.HexToHash("0x01")})}
				_, err = QueryEthReceipt(ctx, client, ethtypes.BlockNumberOrHashWithNumber(1), EthReceiptOption{Method: EthReceiptMethodEthTxnReceipt, Concurrency: 1, Prefetched: block})
			} else {
				_, err = QueryEthData(ctx, client, 1, QueryOption{Filter: logsOnlyFilter{}, EnableLogsOptimization: stage == "logs optimization"})
			}
			require.ErrorIs(t, err, context.Canceled)
			require.True(t, reached)
			provider.call = func(got context.Context, out interface{}, method string, args ...interface{}) error {
				require.NoError(t, got.Err())
				return nil
			}
			// The pooled SDK client is still usable by an unrelated request.
			_, err = client.Eth.BlockByNumber(1, false)
			require.NoError(t, err)
		})
	}
}
