package rpc

import (
	"context"

	"github.com/conflux-chain/conflux-infura/types"
)

type gasStationAPI struct {
	handler *GasStationHandler
}

func newGasStationAPI(handler *GasStationHandler) *gasStationAPI {
	return &gasStationAPI{handler: handler}
}

func (api *gasStationAPI) Price(ctx context.Context) (*types.GasStationPrice, error) {
	return api.handler.GetPrice()
}
