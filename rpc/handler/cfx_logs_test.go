package handler

import (
	"context"
	"errors"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/stretchr/testify/require"
)

type hashLogsClient struct {
	sdk.ClientOperator
	filter types.LogFilter
	logs   []types.Log
	err    error
	calls  int
}

func (c *hashLogsClient) GetLogs(filter types.LogFilter) ([]types.Log, error) {
	c.filter = filter
	c.calls++
	return c.logs, c.err
}

func TestCoreHashLogsBypassDatabase(t *testing.T) {
	oldLimit := maxGetLogsResponseBytes
	maxGetLogsResponseBytes = 1 << 20
	t.Cleanup(func() { maxGetLogsResponseBytes = oldLimit })
	client := &hashLogsClient{}
	handler := NewCfxLogsApiHandler(nil, nil, 0)
	filter := &types.LogFilter{BlockHashes: []types.Hash{"block-a", "block-b"}, Address: []types.Address{cfxaddress.MustNewFromHex("0x1000000000000000000000000000000000000001", 1029)}, Topics: [][]types.Hash{{"topic"}}}
	_, hit, err := handler.GetLogs(context.Background(), client, filter, "")
	require.NoError(t, err)
	require.False(t, hit)
	require.Equal(t, *filter, client.filter)
	nodeErr := errors.New("block not found")
	client.err = nodeErr
	_, _, err = handler.GetLogs(context.Background(), client, filter, "")
	require.ErrorIs(t, err, nodeErr)
	client.err = nil
	client.logs = make([]types.Log, int(store.MaxLogLimit)+2)
	_, _, err = handler.GetLogs(context.Background(), client, filter, "")
	require.ErrorIs(t, err, store.ErrFilterResultSetTooLarge)
	filter.BlockHashes = filter.BlockHashes[:1]
	logs, hit, err := handler.GetLogs(context.Background(), client, filter, "")
	require.NoError(t, err)
	require.False(t, hit)
	require.Len(t, logs, int(store.MaxLogLimit)+2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := client.calls
	_, _, err = handler.GetLogs(ctx, client, filter, "")
	require.ErrorIs(t, err, store.ErrGetLogsTimeout)
	require.Equal(t, before, client.calls)
}
