package rpc

import (
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

type traceAPI struct {
	cfx sdk.ClientOperator
}

func newTraceAPI(cfx sdk.ClientOperator) *traceAPI {
	return &traceAPI{cfx}
}

func (api *traceAPI) Block(blockHash types.Hash) (*types.LocalizedBlockTrace, error) {
	return api.cfx.GetBlockTrace(blockHash)
}
