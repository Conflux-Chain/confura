package handler

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/sync/tracelog"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/stretchr/testify/require"
)

type traceViewReader struct {
	mapping        mysql.CfxTraceSyncEpochBlockMap
	mappingErr     error
	version, reads int
	reorg          bool
	logs           []mysql.InternalContractLog
}

func (s *traceViewReader) GetReorgVersionWithContext(ctx context.Context) (int, error) {
	return s.version, ctx.Err()
}
func (s *traceViewReader) BlockMapping(ctx context.Context, epoch uint64) (mysql.CfxTraceSyncEpochBlockMap, error) {
	if err := ctx.Err(); err != nil {
		return s.mapping, err
	}
	return s.mapping, s.mappingErr
}
func (s *traceViewReader) GetLogs(ctx context.Context, f mysql.CfxInternalContractLogFilter) ([]mysql.InternalContractLog, error) {
	s.reads++
	if s.reorg {
		s.version++
		s.mapping.PivotHash = "new-pivot"
		s.reorg = false
	}
	return s.logs, ctx.Err()
}

type tracePivotClient struct {
	sdk.ClientOperator
	block     *types.BlockSummary
	checked   types.Block
	err       error
	pivots    []types.Hash
	summaries int
}

func (c *tracePivotClient) GetBlockSummaryByHash(types.Hash) (*types.BlockSummary, error) {
	c.summaries++
	return c.block, nil
}
func (c *tracePivotClient) GetBlockByHashWithPivotAssumption(hash, pivot types.Hash, epoch hexutil.Uint64) (types.Block, error) {
	c.pivots = append(c.pivots, pivot)
	return c.checked, c.err
}
func TestInternalLogsHashPivotValidation(t *testing.T) {
	for _, scenario := range []string{"empty", "duplicate", "missing", "unexecuted", "mapping-missing", "wrong-pivot", "wrong-hash", "wrong-epoch", "out-of-range", "reorg", "wrong-log"} {
		t.Run(scenario, func(t *testing.T) {
			header := types.BlockHeader{Hash: "requested", EpochNumber: hexBig(7), BlockNumber: hexBig(11)}
			c := &tracePivotClient{block: &types.BlockSummary{BlockHeader: header}, checked: types.Block{BlockHeader: header}}
			s := &traceViewReader{mapping: mysql.CfxTraceSyncEpochBlockMap{Epoch: 7, BnMin: 10, BnMax: 12, PivotHash: "saved-pivot"}}
			h := &CfxInternalContractLogsApiHandler{registry: &tracelog.Registry{}, logStore: s}
			filter := &types.LogFilter{BlockHashes: []types.Hash{"requested"}}
			wantError := true
			switch scenario {
			case "empty":
				wantError = false
			case "duplicate":
				filter.BlockHashes = append(filter.BlockHashes, "requested")
				wantError = false
			case "missing":
				c.block = nil
			case "unexecuted":
				c.block.BlockNumber = nil
			case "mapping-missing":
				s.mappingErr = errors.New("mapping missing")
			case "wrong-pivot":
				c.err = errors.New("pivot chain assumption failed")
			case "wrong-hash":
				c.checked.Hash = "other"
			case "wrong-epoch":
				c.checked.EpochNumber = hexBig(8)
			case "out-of-range":
				c.checked.BlockNumber = hexBig(13)
			case "reorg":
				s.reorg = true
				wantError = false
			case "wrong-log":
				s.logs = []mysql.InternalContractLog{{BlockHash: "other", BlockNumber: 11}}
			}
			logs, err := h.GetLogs(context.Background(), c, filter)
			require.Empty(t, logs)
			if wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if scenario == "reorg" {
					require.Equal(t, 2, s.reads)
				} else {
					require.Equal(t, 1, s.reads)
				}
			}
			if scenario == "duplicate" {
				require.Equal(t, 1, c.summaries)
				require.Len(t, c.pivots, 1)
			}
			if scenario == "reorg" {
				require.Equal(t, []types.Hash{"saved-pivot", "new-pivot"}, c.pivots)
			}
		})
	}
}

func TestInternalLogsHashPreservesSynthesizedEvents(t *testing.T) {
	hash := types.Hash("0x" + strings.Repeat("ab", 32))
	header := types.BlockHeader{Hash: hash, EpochNumber: hexBig(7), BlockNumber: hexBig(11)}
	c := &tracePivotClient{block: &types.BlockSummary{BlockHeader: header}, checked: types.Block{BlockHeader: header}}
	client, err := sdk.NewClientWithProvider(&logsContextTestProvider{})
	require.NoError(t, err)
	client.SetNetworkId(1029)
	client.SetChainId(1029)
	c.ClientOperator = client
	registry, err := tracelog.NewRegistry(c)
	require.NoError(t, err)
	addr := cfxaddress.MustNewFromHex("0x0888000000000000000000000000000000000002", 1029)
	event, ok := registry.EventHashByIndex(tracelog.EventStakingDeposit)
	require.True(t, ok)
	s := &traceViewReader{mapping: mysql.CfxTraceSyncEpochBlockMap{Epoch: 7, BnMin: 10, BnMax: 12, PivotHash: "saved-pivot"}, logs: []mysql.InternalContractLog{{
		Epoch: 7, BlockNumber: 11, BlockHash: hash.String(), AddressIndex: tracelog.ContractStaking, Topic0Index: tracelog.EventStakingDeposit, Data: []byte{1, 2},
	}}}
	h := &CfxInternalContractLogsApiHandler{registry: registry, logStore: s}
	logs, err := h.GetLogs(context.Background(), c, &types.LogFilter{BlockHashes: []types.Hash{types.Hash("0x" + strings.Repeat("AB", 32))}, Address: []types.Address{addr}})
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, hash, *logs[0].BlockHash)
	require.Equal(t, addr, logs[0].Address)
	require.Equal(t, event, logs[0].Topics[0])
	require.Equal(t, []byte{1, 2}, []byte(logs[0].Data))
}
