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
	//return api.handler.GetPrice()

	// Use oracle gas price from the blockchain.
	cfx := GetCfxClientFromContext(ctx)
	price, err := cfx.GetGasPrice()
	if err != nil {
		return nil, err
	}

	return &types.GasStationPrice{
		Fast:    price,
		Fastest: price,
		SafeLow: price,
		Average: price,
	}, nil
}
