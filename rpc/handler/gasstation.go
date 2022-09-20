package handler

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/scroll-tech/rpc-gateway/store"
	itypes "github.com/scroll-tech/rpc-gateway/types"
	"github.com/scroll-tech/rpc-gateway/util"
	"github.com/sirupsen/logrus"
)

const ( // gas station price configs
	ConfGasStationPriceFast    = "gasstation_price_fast"
	ConfGasStationPriceFastest = "gasstation_price_fastest"
	ConfGasStationPriceSafeLow = "gasstation_price_safe_low"
	ConfGasStationPriceAverage = "gasstation_price_average"
)

var (
	defaultGasStationPriceFastest = big.NewInt(1_000_000_000) // (1G)
	defaultGasStationPriceFast    = big.NewInt(1_000_000_000) // (1G)
	defaultGasStationPriceAverage = big.NewInt(1_000_000_000) // (1G)
	defaultGasStationPriceSafeLow = big.NewInt(1_000_000_000) // (1G)

	maxGasStationPriceFastest = big.NewInt(10_000_000_000) // (10G)
	maxGasStationPriceFast    = big.NewInt(10_000_000_000) // (10G)
	maxGasStationPriceAverage = big.NewInt(10_000_000_000) // (10G)
	maxGasStationPriceSafeLow = big.NewInt(10_000_000_000) // (10G)
)

// GasStationHandler RPC handler to serve gas price estimation etc.,
type GasStationHandler struct {
	db, cache store.Configurable
}

func NewGasStationHandler(db, cache store.Configurable) *GasStationHandler {
	return &GasStationHandler{db: db, cache: cache}
}

func (handler *GasStationHandler) GetPrice() (*itypes.GasStationPrice, error) {
	gasStationPriceConfs := []string{ // order is important !!!
		ConfGasStationPriceFast,
		ConfGasStationPriceFastest,
		ConfGasStationPriceSafeLow,
		ConfGasStationPriceAverage,
	}

	maxGasStationPrices := []*big.Int{ // order is important !!!
		maxGasStationPriceFast,
		maxGasStationPriceFastest,
		maxGasStationPriceSafeLow,
		maxGasStationPriceAverage,
	}

	var gasPriceConf map[string]interface{}
	var err error

	useCache := false
	if !util.IsInterfaceValNil(handler.cache) { // load from cache first
		useCache = true

		gasPriceConf, err = handler.cache.LoadConfig(gasStationPriceConfs...)
		if err != nil {
			logrus.WithError(err).Error("Failed to get gasstation price config from cache")
			useCache = false
		} else {
			logrus.WithField("gasPriceConf", gasPriceConf).Debug("Loaded gasstation price config from cache")
		}
	}

	if len(gasPriceConf) != len(gasStationPriceConfs) && !util.IsInterfaceValNil(handler.db) { // load from db
		gasPriceConf, err = handler.db.LoadConfig(gasStationPriceConfs...)
		if err != nil {
			logrus.WithError(err).Error("Failed to get gasstation price config from db")

			goto defaultR
		}

		logrus.WithField("gasPriceConf", gasPriceConf).Debug("Gasstation price loaded from db")

		if useCache { // update cache
			for confName, confVal := range gasPriceConf {
				if err := handler.cache.StoreConfig(confName, confVal); err != nil {
					logrus.WithError(err).Error("Failed to update gas station price config in cache")
				} else {
					logrus.WithFields(logrus.Fields{
						"confName": confName, "confVal": confVal,
					}).Debug("Update gas station price config in cache")
				}
			}
		}
	}

	if len(gasPriceConf) == len(gasStationPriceConfs) {
		var gsp itypes.GasStationPrice

		setPtrs := []**hexutil.Big{ // order is important !!!
			&gsp.Fast, &gsp.Fastest, &gsp.SafeLow, &gsp.Average,
		}

		for i, gpc := range gasStationPriceConfs {
			var bigV hexutil.Big

			gasPriceHex, ok := gasPriceConf[gpc].(string)
			if !ok {
				logrus.WithFields(logrus.Fields{
					"gasPriceConfig": gpc, "gasPriceConf": gasPriceConf,
				}).Error("Invalid gas statation gas price config")

				goto defaultR
			}

			if err := bigV.UnmarshalText([]byte(gasPriceHex)); err != nil {
				logrus.WithFields(logrus.Fields{
					"gasPriceConfig": gpc, "gasPriceHex": gasPriceHex,
				}).Error("Failed to unmarshal gas price from hex string")

				goto defaultR
			}

			if maxGasStationPrices[i].Cmp((*big.Int)(&bigV)) < 0 {
				logrus.WithFields(logrus.Fields{
					"gasPriceConfig": gpc, "bigV": bigV.ToInt(),
				}).Warn("Configured gas statation price overflows max limit, pls double check")

				*setPtrs[i] = (*hexutil.Big)(maxGasStationPrices[i])
			} else {
				*setPtrs[i] = &bigV
			}
		}

		return &gsp, nil
	}

defaultR:
	logrus.Debug("Gas station uses default as final gas price")

	// use default gas price
	return &itypes.GasStationPrice{
		Fast:    (*hexutil.Big)(defaultGasStationPriceFast),
		Fastest: (*hexutil.Big)(defaultGasStationPriceFastest),
		SafeLow: (*hexutil.Big)(defaultGasStationPriceSafeLow),
		Average: (*hexutil.Big)(defaultGasStationPriceAverage),
	}, nil
}
