package handler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/sync/tracelog"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common/hexutil"
	rpcprovider "github.com/openweb3/go-rpc-provider"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type traceViewReader struct {
	mapping         mysql.CfxTraceSyncEpochBlockMap
	mappingErr      error
	readErr         error
	logs            []mysql.InternalContractLog
	versionReads    int
	mappingReads    int
	reads           int
	versionResponse func(int) (int, error)
	mappingResponse func(int) (mysql.CfxTraceSyncEpochBlockMap, error)
	logResponse     func(int) ([]mysql.InternalContractLog, error)
}

func (s *traceViewReader) GetReorgVersionWithContext(ctx context.Context) (int, error) {
	s.versionReads++
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if s.versionResponse != nil {
		return s.versionResponse(s.versionReads)
	}
	return 0, nil
}
func (s *traceViewReader) BlockMapping(ctx context.Context, epoch uint64) (mysql.CfxTraceSyncEpochBlockMap, error) {
	s.mappingReads++
	if err := ctx.Err(); err != nil {
		return s.mapping, err
	}
	if s.mappingResponse != nil {
		return s.mappingResponse(s.mappingReads)
	}
	return s.mapping, s.mappingErr
}
func (s *traceViewReader) GetLogs(ctx context.Context, f mysql.CfxInternalContractLogFilter) ([]mysql.InternalContractLog, error) {
	s.reads++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.logResponse != nil {
		return s.logResponse(s.reads)
	}
	return s.logs, s.readErr
}

type tracePivotClient struct {
	sdk.ClientOperator
	block         *types.BlockSummary
	checked       types.Block
	err           error
	pivots        []types.Hash
	summaries     int
	pivotResponse func(int) (types.Block, error)
}

func (c *tracePivotClient) GetBlockSummaryByHash(types.Hash) (*types.BlockSummary, error) {
	c.summaries++
	return c.block, nil
}
func (c *tracePivotClient) GetBlockByHashWithPivotAssumption(hash, pivot types.Hash, epoch hexutil.Uint64) (types.Block, error) {
	c.pivots = append(c.pivots, pivot)
	if c.pivotResponse != nil {
		return c.pivotResponse(len(c.pivots))
	}
	return c.checked, c.err
}

func newTraceViewTestHandler() (*CfxInternalContractLogsApiHandler, *traceViewReader, *tracePivotClient) {
	header := types.BlockHeader{Hash: "requested", EpochNumber: hexBig(7), BlockNumber: hexBig(11)}
	client := &tracePivotClient{block: &types.BlockSummary{BlockHeader: header}, checked: types.Block{BlockHeader: header}}
	reader := &traceViewReader{mapping: mysql.CfxTraceSyncEpochBlockMap{Epoch: 7, BnMin: 10, BnMax: 12, PivotHash: "saved-pivot"}}
	handler := &CfxInternalContractLogsApiHandler{registry: &tracelog.Registry{}, logStore: reader}
	return handler, reader, client
}

// The first attempt uses the saved pivot; subsequent attempts see the new view.
func setTraceReorgResponses(s *traceViewReader) {
	s.versionResponse = func(call int) (int, error) {
		if call == 1 {
			return 0, nil
		}
		return 1, nil
	}
	s.mappingResponse = func(call int) (mysql.CfxTraceSyncEpochBlockMap, error) {
		mapping := s.mapping
		if call > 1 {
			mapping.PivotHash = "new-pivot"
		}
		return mapping, nil
	}
}

func TestInternalLogsRetriesChangedCanonicalView(t *testing.T) {
	for _, scenario := range []string{"pivot", "pivot-provider", "log-identity", "block-identity", "block-range", "missing-mapping"} {
		t.Run(scenario, func(t *testing.T) {
			h, s, c := newTraceViewTestHandler()
			setTraceReorgResponses(s)
			switch scenario {
			case "pivot", "pivot-provider":
				message := "Invalid parameters: pivot chain assumption failed"
				if scenario == "pivot-provider" {
					message = "Invalid parameters: Error: pivot chain assumption failed"
				}
				c.pivotResponse = func(call int) (types.Block, error) {
					if call == 1 {
						return types.Block{}, &rpcprovider.JsonError{Code: -32602, Message: message}
					}
					return c.checked, nil
				}
			case "log-identity":
				s.logResponse = func(call int) ([]mysql.InternalContractLog, error) {
					if call == 1 {
						return []mysql.InternalContractLog{{BlockHash: "old-block", BlockNumber: 11}}, nil
					}
					return nil, nil
				}
			case "block-identity", "block-range":
				c.pivotResponse = func(call int) (types.Block, error) {
					checked := c.checked
					if call == 1 {
						if scenario == "block-identity" {
							checked.Hash = "old-block"
						} else {
							checked.BlockNumber = hexBig(13)
						}
					}
					return checked, nil
				}
			case "missing-mapping":
				s.mappingResponse = func(call int) (mysql.CfxTraceSyncEpochBlockMap, error) {
					if call == 1 {
						return mysql.CfxTraceSyncEpochBlockMap{}, gorm.ErrRecordNotFound
					}
					mapping := s.mapping
					mapping.PivotHash = "new-pivot"
					return mapping, nil
				}
			}
			logs, err := h.GetLogs(context.Background(), c, &types.LogFilter{BlockHashes: []types.Hash{"requested"}})
			require.NoError(t, err)
			require.Empty(t, logs)
			require.Equal(t, 2, c.summaries)
			require.Equal(t, 3, s.versionReads)
			require.Equal(t, types.Hash("new-pivot"), c.pivots[len(c.pivots)-1])
		})
	}
}

func TestInternalLogsDoesNotRetryOrdinaryErrors(t *testing.T) {
	for _, queryErr := range []error{
		errors.New("connection reset"),
		errors.New("pivot chain assumption failed"),
		&rpcprovider.JsonError{Code: -32602, Message: "Invalid parameters: Block not found"},
		&rpcprovider.JsonError{Code: -32000, Message: "Invalid parameters: pivot chain assumption failed"},
	} {
		t.Run(queryErr.Error(), func(t *testing.T) {
			h, s, c := newTraceViewTestHandler()
			setTraceReorgResponses(s)
			c.err = queryErr
			_, err := h.GetLogs(context.Background(), c, &types.LogFilter{BlockHashes: []types.Hash{"requested"}})
			require.ErrorIs(t, err, queryErr)
			require.Equal(t, 1, c.summaries)
			require.Equal(t, 1, s.versionReads)
		})
	}
}

func TestInternalLogsCanonicalErrorBoundaries(t *testing.T) {
	for _, scenario := range []string{"stable", "version-failure", "database-error"} {
		t.Run(scenario, func(t *testing.T) {
			h, s, c := newTraceViewTestHandler()
			pivotErr := &rpcprovider.JsonError{Code: -32602, Message: "Invalid parameters: pivot chain assumption failed"}
			c.err = pivotErr
			wantErr := error(pivotErr)
			wantVersionReads := 2
			switch scenario {
			case "version-failure":
				wantErr = errors.New("version read failed")
				s.versionResponse = func(call int) (int, error) {
					if call == 1 {
						return 0, nil
					}
					return 0, wantErr
				}
			case "database-error":
				wantErr = errors.New("database unavailable")
				setTraceReorgResponses(s)
				c.err, s.readErr = nil, wantErr
				wantVersionReads = 1
			}
			_, err := h.GetLogs(context.Background(), c, &types.LogFilter{BlockHashes: []types.Hash{"requested"}})
			require.ErrorIs(t, err, wantErr)
			require.Equal(t, wantVersionReads, s.versionReads)
			require.Equal(t, 1, c.summaries)
		})
	}
}

func TestInternalLogsCanonicalErrorCancellation(t *testing.T) {
	h, s, c := newTraceViewTestHandler()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	c.pivotResponse = func(int) (types.Block, error) {
		cancel()
		return types.Block{}, &rpcprovider.JsonError{Code: -32602, Message: "Invalid parameters: pivot chain assumption failed"}
	}
	_, err := h.GetLogs(ctx, c, &types.LogFilter{BlockHashes: []types.Hash{"requested"}})
	require.ErrorIs(t, err, store.ErrGetLogsTimeout)
	require.Equal(t, 1, c.summaries)
	require.Equal(t, 1, s.versionReads)
}

func TestInternalLogsContinuousReorgTimeout(t *testing.T) {
	h, s, c := newTraceViewTestHandler()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	s.versionResponse = func(call int) (int, error) { return call - 1, nil }
	c.err = &rpcprovider.JsonError{Code: -32602, Message: "Invalid parameters: pivot chain assumption failed"}
	_, err := h.GetLogs(ctx, c, &types.LogFilter{BlockHashes: []types.Hash{"requested"}})
	require.ErrorIs(t, err, store.ErrGetLogsTimeout)
	require.Greater(t, c.summaries, 1)
}

func TestInternalLogsHashPivotValidation(t *testing.T) {
	for _, scenario := range []string{"empty", "duplicate", "missing", "unexecuted", "mapping-missing", "wrong-pivot", "wrong-hash", "wrong-epoch", "out-of-range", "reorg", "wrong-log"} {
		t.Run(scenario, func(t *testing.T) {
			h, s, c := newTraceViewTestHandler()
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
				setTraceReorgResponses(s)
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
