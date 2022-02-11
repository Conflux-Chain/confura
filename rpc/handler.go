package rpc

import (
	"context"
	"errors"
	"math/big"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/store"
	itypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/sirupsen/logrus"
)

// cfxHandler interface delegated to handle cfx rpc request
type cfxHandler interface {
	GetBlockByHash(ctx context.Context, blockHash types.Hash, includeTxs bool) (interface{}, error)
	GetBlockByEpochNumber(ctx context.Context, epoch *types.Epoch, includeTxs bool) (interface{}, error)
	GetLogs(ctx context.Context, filter store.LogFilter) ([]types.Log, error)
	GetTransactionByHash(ctx context.Context, txHash types.Hash) (*types.Transaction, error)
	GetBlocksByEpoch(ctx context.Context, epoch *types.Epoch) ([]types.Hash, error)
	GetTransactionReceipt(ctx context.Context, txHash types.Hash) (*types.TransactionReceipt, error)
	GetBlockByBlockNumber(ctx context.Context, blockNumer hexutil.Uint64, includeTxs bool) (block interface{}, err error)
}

// CfxStoreHandler implements cfxHandler interface to accelerate rpc request handling by loading epoch data from store
type CfxStoreHandler struct {
	store store.Store
	next  cfxHandler
}

func NewCfxStoreHandler(store store.Store, next cfxHandler) *CfxStoreHandler {
	return &CfxStoreHandler{store: store, next: next}
}

func (h *CfxStoreHandler) GetBlockByHash(ctx context.Context, blockHash types.Hash, includeTxs bool) (block interface{}, err error) {
	var sblock *store.Block
	var sblocksum *store.BlockSummary

	if includeTxs {
		sblock, err = h.store.GetBlockByHash(blockHash)
	} else {
		sblocksum, err = h.store.GetBlockSummaryByHash(blockHash)
	}

	if err != nil && !util.IsInterfaceValNil(h.next) {
		return h.next.GetBlockByHash(ctx, blockHash, includeTxs)
	}

	if sblock != nil {
		return sblock.CfxBlock, nil
	}

	if sblocksum != nil {
		return sblocksum.CfxBlockSummary, nil
	}

	return
}

func (h *CfxStoreHandler) GetBlockByEpochNumber(ctx context.Context, epoch *types.Epoch, includeTxs bool) (block interface{}, err error) {
	epBigInt, ok := epoch.ToInt()
	if !ok {
		err = store.ErrUnsupported
		return
	}

	var sblock *store.Block
	var sblocksum *store.BlockSummary

	epochNo := epBigInt.Uint64()

	if includeTxs {
		sblock, err = h.store.GetBlockByEpoch(epochNo)
	} else {
		sblocksum, err = h.store.GetBlockSummaryByEpoch(epochNo)
	}

	if err != nil && !util.IsInterfaceValNil(h.next) {
		return h.next.GetBlockByEpochNumber(ctx, epoch, includeTxs)
	}

	if sblock != nil {
		return sblock.CfxBlock, nil
	}

	if sblocksum != nil {
		return sblocksum.CfxBlockSummary, nil
	}

	return
}

func (h *CfxStoreHandler) GetLogs(ctx context.Context, filter store.LogFilter) (logs []types.Log, err error) {
	slogs, err := h.store.GetLogs(filter)
	if err == nil {
		logs = make([]types.Log, len(slogs))
		for i := 0; i < len(slogs); i++ {
			logs[i] = *slogs[i].CfxLog
		}

		return logs, nil
	}

	switch {
	case h.store.IsRecordNotFound(err):
	case errors.Is(err, store.ErrUnsupported):
	case errors.Is(err, store.ErrAlreadyPruned):
	default: // must be something wrong with the store
		logrus.WithError(err).Error("cfxStoreHandler failed to get logs from store")
	}

	if !util.IsInterfaceValNil(h.next) {
		return h.next.GetLogs(ctx, filter)
	}

	return
}

func (h *CfxStoreHandler) GetTransactionByHash(ctx context.Context, txHash types.Hash) (*types.Transaction, error) {
	stx, err := h.store.GetTransaction(txHash)
	if err == nil {
		return stx.CfxTransaction, nil
	}

	if !util.IsInterfaceValNil(h.next) {
		return h.next.GetTransactionByHash(ctx, txHash)
	}

	return nil, err
}

func (h *CfxStoreHandler) GetBlocksByEpoch(ctx context.Context, epoch *types.Epoch) (blockHashes []types.Hash, err error) {
	epBigInt, ok := epoch.ToInt()
	if !ok {
		err = store.ErrUnsupported
		return
	}

	epochNo := epBigInt.Uint64()
	if blockHashes, err = h.store.GetBlocksByEpoch(epochNo); err == nil {
		return
	}

	if !util.IsInterfaceValNil(h.next) {
		return h.next.GetBlocksByEpoch(ctx, epoch)
	}

	return
}

func (h *CfxStoreHandler) GetBlockByBlockNumber(ctx context.Context, blockNumer hexutil.Uint64, includeTxs bool) (
	block interface{}, err error,
) {
	var sblock *store.Block
	var sblocksum *store.BlockSummary

	if includeTxs {
		sblock, err = h.store.GetBlockByBlockNumber(uint64(blockNumer))
	} else {
		sblocksum, err = h.store.GetBlockSummaryByBlockNumber(uint64(blockNumer))
	}

	if err != nil && !util.IsInterfaceValNil(h.next) {
		return h.next.GetBlockByBlockNumber(ctx, blockNumer, includeTxs)
	}

	if sblock != nil {
		return sblock.CfxBlock, nil
	}

	if sblocksum != nil {
		return sblocksum.CfxBlockSummary, nil
	}

	return
}

func (h *CfxStoreHandler) GetTransactionReceipt(ctx context.Context, txHash types.Hash) (*types.TransactionReceipt, error) {
	stxRcpt, err := h.store.GetReceipt(txHash)
	if err == nil {
		return stxRcpt.CfxReceipt, nil
	}

	if !util.IsInterfaceValNil(h.next) {
		return h.next.GetTransactionReceipt(ctx, txHash)
	}

	return nil, err
}

const ( // gas station price configs
	ConfGasStationPriceFast    = "gasstation_price_fast"
	ConfGasStationPriceFastest = "gasstation_price_fastest"
	ConfGasStationPriceSafeLow = "gasstation_price_safe_low"
	ConfGasStationPriceAverage = "gasstation_price_average"
)

var (
	defaultGasStationPriceFastest = big.NewInt(1_000_000_000) // (1G)
	defaultGasStationPriceFast    = big.NewInt(100_000_000)   // (100M)
	defaultGasStationPriceAverage = big.NewInt(10_000_000)    // (10M)
	defaultGasStationPriceSafeLow = big.NewInt(1_000_000)     // (1M)

	maxGasStationPriceFastest = big.NewInt(100_000_000_000) // (100G)
	maxGasStationPriceFast    = big.NewInt(10_000_000_000)  // (10G)
	maxGasStationPriceAverage = big.NewInt(1_000_000_000)   // (1G)
	maxGasStationPriceSafeLow = big.NewInt(100_000_000)     // (100M)
)

// GasStationHandler gas station handler for gas price estimation etc.,
type GasStationHandler struct {
	db, cache store.Store
}

func NewGasStationHandler(db, cache store.Store) *GasStationHandler {
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
