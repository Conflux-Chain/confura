package rpc

import (
	"testing"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	web3types "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

type logHeadResolver struct{ sdk.ClientOperator }

func (logHeadResolver) Resolve(web3types.BlockNumber) (uint64, error) { return 12, nil }
func (logHeadResolver) GetEpochNumber(...*cfxtypes.Epoch) (*hexutil.Big, error) {
	return cfxtypes.NewBigInt(12), nil
}

func TestLogNormalizationResolvesRange(t *testing.T) {
	for _, mode := range []string{"default", "head", "numeric", "other_tag", "hash"} {
		t.Run(mode, func(t *testing.T) {
			from, to := web3types.BlockNumber(10), web3types.LatestBlockNumber
			eth := &web3types.FilterQuery{FromBlock: &from, ToBlock: &to}
			core := &cfxtypes.LogFilter{FromEpoch: cfxtypes.NewEpochNumberUint64(10), ToEpoch: cfxtypes.EpochLatestState}
			ethFlag, coreFlag := LogFilterTypeBlockRange, LogFilterTypeEpochRange
			switch mode {
			case "default":
				eth.ToBlock = nil
				core.ToEpoch = nil
			case "numeric":
				to = 12
				core.ToEpoch = cfxtypes.NewEpochNumberUint64(12)
			case "other_tag":
				to = web3types.PendingBlockNumber
				core.ToEpoch = cfxtypes.EpochLatestMined
			case "hash":
				ethFlag, coreFlag = LogFilterTypeBlockHash, LogFilterTypeBlockHash
			}
			require.NoError(t, NormalizeEthLogFilter(logHeadResolver{}, ethFlag, eth, 0))
			require.NoError(t, NormalizeLogFilter(logHeadResolver{}, coreFlag, core))
			if mode != "hash" {
				require.EqualValues(t, 12, *eth.ToBlock)
				upper, numeric := core.ToEpoch.ToInt()
				require.True(t, numeric)
				require.EqualValues(t, 12, upper.Uint64())
			} else {
				require.Equal(t, web3types.LatestBlockNumber, *eth.ToBlock)
				require.True(t, cfxtypes.EpochLatestState.Equals(core.ToEpoch))
			}
		})
	}
}
