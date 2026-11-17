package handler

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/stretchr/testify/require"
)

type archiveErrorRouter struct{ err error }

func (r archiveErrorRouter) Route(context.Context, node.Group, []byte) (string, error) {
	return "", r.err
}

func TestPrunedLogsUnavailableErrorCompatibility(t *testing.T) {
	rejected := errors.New("fixture permission denied")
	transport := &net.OpError{Op: "read", Net: "tcp", Err: errors.New("fixture connection reset")}
	for _, test := range []struct {
		name   string
		router node.Router
		want   error
	}{
		{"empty chain", node.NewChainedRouter(nil), errQuotaNotEnough},
		{"direct", archiveErrorRouter{node.ErrClientUnavailable}, errQuotaNotEnough},
		{"wrapped", archiveErrorRouter{fmt.Errorf("fixture: %w", node.ErrClientUnavailable)}, errQuotaNotEnough},
		{"joined", archiveErrorRouter{errors.Join(node.ErrClientUnavailable, transport)}, errQuotaNotEnough},
		{"outage without fallback", node.NewChainedRouter(nil, archiveErrorRouter{transport}), errQuotaNotEnough},
		{"rejected", archiveErrorRouter{rejected}, rejected},
		{"canceled", archiveErrorRouter{context.Canceled}, context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := &CfxPrunedLogsHandler{pool: node.NewCfxClientProvider(nil, test.router)}
			// A context without an HTTP request takes the shared archive path.
			logs, err := h.GetLogs(context.Background(), types.LogFilter{})
			require.Nil(t, logs)
			require.ErrorIs(t, err, test.want)
		})
	}
}
