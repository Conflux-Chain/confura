package rpc

import (
	"context"
	"testing"

	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/stretchr/testify/require"
)

func TestCfxScanLogsEntryErrorCategories(t *testing.T) {
	api := &cfxAPI{}
	_, err := api.scanLogs(context.Background(), handler.CfxScanLogRequest{}, nil, false)
	require.ErrorIs(t, err, handler.ErrScanLogsUnavailable)
	require.EqualError(t, err, "scan logs rpc unavailable: api handler not configured")

	api.LogApiHandler = &handler.CfxLogsApiHandler{}
	request := handler.CfxScanLogRequest{Cursor: &handler.ScanLogCursor{}}
	_, err = api.scanLogs(context.Background(), request, nil, true)
	require.ErrorIs(t, err, handler.ErrScanLogsInvalidParams)
	require.EqualError(t, err, "invalid scan logs params: missing pivot assumption")
}

func TestEthScanLogsEntryErrorCategories(t *testing.T) {
	api := &ethAPI{}
	_, err := api.scanLogs(context.Background(), handler.EthScanLogRequest{}, nil, false)
	require.ErrorIs(t, err, handler.ErrScanLogsUnavailable)
	require.EqualError(t, err, "scan logs rpc unavailable: api handler not configured")

	api.LogApiHandler = &handler.EthLogsApiHandler{}
	request := handler.EthScanLogRequest{Cursor: &handler.ScanLogCursor{}}
	_, err = api.scanLogs(context.Background(), request, nil, true)
	require.ErrorIs(t, err, handler.ErrScanLogsInvalidParams)
	require.EqualError(t, err, "invalid scan logs params: missing pivot assumption")
}

func TestValidateScanLogsRequestAllowsOtherShapes(t *testing.T) {
	cursor := &handler.ScanLogCursor{}
	tests := []struct {
		name                string
		withPivotAssumption bool
		cursor              *handler.ScanLogCursor
		assumptionProvided  bool
	}{
		{name: "with assumption", withPivotAssumption: true, cursor: cursor, assumptionProvided: true},
		{name: "first page", withPivotAssumption: true},
		{name: "plain scan logs", cursor: cursor},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, validateScanLogsRequest(
				test.withPivotAssumption, test.cursor, test.assumptionProvided,
			))
		})
	}
}
