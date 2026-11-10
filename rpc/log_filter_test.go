package rpc

import (
	"context"
	"encoding/json"
	"fmt"
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

func TestCoreLogFilterRejectsNumberOverflow(t *testing.T) {
	for _, text := range []string{"0x0", "0xffffffffffffffff", "0x10000000000000000", "0x10000000000000001"} {
		for _, selector := range []string{"epoch", "block"} {
			t.Run(selector+text, func(t *testing.T) {
				var filter cfxtypes.LogFilter
				fieldFrom, fieldTo := "fromEpoch", "toEpoch"
				if selector == "block" {
					fieldFrom, fieldTo = "fromBlock", "toBlock"
				}
				require.NoError(t, json.Unmarshal([]byte(fmt.Sprintf(`{"%s":%q,"%s":%q}`, fieldFrom, text, fieldTo, text)), &filter))
				flag, ok := ParseLogFilterType(&filter)
				require.True(t, ok)
				err := NormalizeLogFilter(logHeadResolver{}, flag, &filter)
				if err == nil {
					err = ValidateLogFilter(flag, &filter)
				}
				if len(text) > 18 {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestCoreBlockAPIsRejectEpochOverflowBeforeRouting(t *testing.T) {
	var epoch cfxtypes.Epoch
	require.NoError(t, json.Unmarshal([]byte(`"0x10000000000000001"`), &epoch))
	api := &cfxAPI{}
	for _, include := range []bool{false, true} {
		result, err := api.GetBlockByEpochNumber(context.Background(), epoch, include)
		require.Error(t, err)
		require.Nil(t, result)
	}
	result, err := api.GetBlocksByEpoch(context.Background(), epoch)
	require.Error(t, err)
	require.Nil(t, result)
}
