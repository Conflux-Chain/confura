package rpc

import (
	"context"

	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/types"
)

// cfxGasStationAPI provides core space gasstation API.
type cfxGasStationAPI struct {
	handler *handler.CfxGasStationHandler
}

// newCfxGasStationAPI creates a new instance of `cfxGasStationAPI`.
func newCfxGasStationAPI(handler *handler.CfxGasStationHandler) *cfxGasStationAPI {
	return &cfxGasStationAPI{handler: handler}
}

// SuggestedGasFees returns the suggested gas fees.
func (api *cfxGasStationAPI) SuggestedGasFees(ctx context.Context) (*types.SuggestedGasFees, error) {
	cfx := GetCfxClientFromContext(ctx)
	return api.handler.SuggestGasFee(cfx)
}

// TODO: Deprecate it if not used by the community any more.
func (api *cfxGasStationAPI) Price(ctx context.Context) (*types.GasStationPrice, error) {
	cfx := GetCfxClientFromContext(ctx)
	return api.handler.SuggestGasPrice(cfx)
}
