package rpc

import (
	"context"
	"encoding/json"

	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	rpcprovider "github.com/openweb3/go-rpc-provider"
	"github.com/sirupsen/logrus"
)

var errTraceRPCUnavailable = &rpcprovider.JsonError{Code: -32000, Message: "trace RPC method temporarily unavailable"}

func logTraceRPCUnavailable(method string, params any) {
	encoded, err := json.Marshal(params)
	if err != nil {
		logrus.WithFields(logrus.Fields{"method": method, "marshalError": err}).Info("trace RPC method temporarily unavailable")
		return
	}
	logrus.WithFields(logrus.Fields{"method": method, "params": string(encoded)}).Info("trace RPC method temporarily unavailable")
}

// traceAPI provides core space trace RPC proxy API.
type traceAPI struct {
	stateHandler *handler.CfxStateHandler
}

func (api *traceAPI) Block(ctx context.Context, blockHash types.Hash) (*types.LocalizedBlockTrace, error) {
	logTraceRPCUnavailable("trace_block", map[string]any{"blockHash": blockHash})
	return nil, errTraceRPCUnavailable
}

func (api *traceAPI) Filter(ctx context.Context, filter types.TraceFilter) ([]types.LocalizedTrace, error) {
	logTraceRPCUnavailable("trace_filter", map[string]any{"filter": filter})
	return nil, errTraceRPCUnavailable
}

func (api *traceAPI) Transaction(ctx context.Context, txHash types.Hash) ([]types.LocalizedTrace, error) {
	logTraceRPCUnavailable("trace_transaction", map[string]any{"txHash": txHash})
	return nil, errTraceRPCUnavailable
}

func (api *traceAPI) Epoch(ctx context.Context, epoch types.Epoch) (types.EpochTrace, error) {
	logTraceRPCUnavailable("trace_epoch", map[string]any{"epoch": epoch})
	return types.EpochTrace{}, errTraceRPCUnavailable
}
