package rpc

import (
	"context"

	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/types"
)

// gasStationAPI provides core space gasstation API.
type gasStationAPI struct {
	handler *handler.GasStationHandler
}

func newGasStationAPI(handler *handler.GasStationHandler) *gasStationAPI {
	return &gasStationAPI{handler: handler}
}

func (api *gasStationAPI) Price(ctx context.Context) (*types.GasStationPrice, error) {
	return api.handler.GetPrice()
}
