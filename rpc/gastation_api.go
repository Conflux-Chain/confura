package rpc

import (
	"context"

	"github.com/conflux-chain/conflux-infura/rpc/handler"
	"github.com/conflux-chain/conflux-infura/types"
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
