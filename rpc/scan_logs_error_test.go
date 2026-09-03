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

}

func TestEthScanLogsEntryErrorCategories(t *testing.T) {
	api := &ethAPI{}
	_, err := api.scanLogs(context.Background(), handler.EthScanLogRequest{}, nil, false)
	require.ErrorIs(t, err, handler.ErrScanLogsUnavailable)
	require.EqualError(t, err, "scan logs rpc unavailable: api handler not configured")

}
