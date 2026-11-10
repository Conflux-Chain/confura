package handler

import (
	"context"
	"math/big"
	"testing"

	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/stretchr/testify/require"
)

type fixedEpochResolver struct {
	value *big.Int
	calls int
}

func (r *fixedEpochResolver) GetEpochNumber(...*cfxtypes.Epoch) (*hexutil.Big, error) {
	r.calls++
	return (*hexutil.Big)(r.value), nil
}

func TestCoreScanEpochRangeDoesNotWrap(t *testing.T) {
	for _, text := range []string{"0", "18446744073709551615", "18446744073709551616", "18446744073709551617", "-1"} {
		t.Run(text, func(t *testing.T) {
			number, ok := new(big.Int).SetString(text, 10)
			require.True(t, ok)
			epoch := cfxtypes.NewEpochNumber((*hexutil.Big)(number))
			resolver := &fixedEpochResolver{value: new(big.Int).SetUint64(^uint64(0))}
			params, err := NormalizeCfxScanLogRequest(resolver, CfxScanLogRequest{Filter: CfxScanLogFilter{FromEpoch: epoch, ToEpoch: epoch}}, false)
			if number.IsUint64() {
				require.NoError(t, err)
				require.Equal(t, number.Uint64(), params.EpochRange.From)
				require.Equal(t, number.Uint64(), params.EpochRange.To)
			} else {
				require.ErrorIs(t, err, ErrScanLogsInvalidParams)
				require.Zero(t, resolver.calls)
			}
			// The store must reject before touching its nil DB, including both block shapes.
			if !number.IsUint64() {
				h := NewCfxStoreHandler("test", nil)
				for _, include := range []bool{false, true} {
					_, err = h.GetBlockByEpochNumber(context.Background(), epoch, include)
					require.Error(t, err)
				}
				_, err = h.GetBlocksByEpoch(context.Background(), epoch)
				require.Error(t, err)
			}
		})
	}
	resolver := &fixedEpochResolver{value: new(big.Int).Lsh(big.NewInt(1), 64)}
	_, err := NormalizeCfxScanLogRequest(resolver, CfxScanLogRequest{}, false)
	require.ErrorIs(t, err, ErrScanLogsInvalidParams)
}
