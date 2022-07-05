package rpc

import (
	"context"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	postypes "github.com/Conflux-Chain/go-conflux-sdk/types/pos"
	"github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/rpc/cache"
	"github.com/conflux-chain/conflux-infura/rpc/handler"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/conflux-chain/conflux-infura/util/metrics"
	"github.com/conflux-chain/conflux-infura/util/relay"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/openweb3/go-rpc-provider"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	// Flyweight objects
	emptyEpochs = []*types.Epoch{}
	emptyLogs   = []types.Log{}
)

type CfxAPIOption struct {
	StoreHandler  *handler.CfxStoreHandler
	LogApiHandler *handler.CfxLogsApiHandler
	Relayer       *relay.TxnRelayer
}

type cfxAPI struct {
	CfxAPIOption
	provider         *node.CfxClientProvider
	inputEpochMetric metrics.InputEpochMetric
}

func newCfxAPI(provider *node.CfxClientProvider, option ...CfxAPIOption) *cfxAPI {
	var opt CfxAPIOption
	if len(option) > 0 {
		opt = option[0]
	}

	return &cfxAPI{
		CfxAPIOption: opt,
		provider:     provider,
	}
}

func toSlice(epoch *types.Epoch) []*types.Epoch {
	if epoch == nil {
		return emptyEpochs
	}

	return []*types.Epoch{epoch}
}

func (api *cfxAPI) GasPrice(ctx context.Context) (*hexutil.Big, error) {
	cfx := GetCfxClientFromContext(ctx)
	return cache.CfxDefault.GetGasPrice(cfx)
}

func (api *cfxAPI) EpochNumber(ctx context.Context, epoch *types.Epoch) (*hexutil.Big, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_epochNumber", cfx)
	return cache.CfxDefault.GetEpochNumber(cfx, epoch)
}

func (api *cfxAPI) GetBalance(ctx context.Context, address types.Address, epoch *types.Epoch) (*hexutil.Big, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getBalance", cfx)
	return cfx.GetBalance(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetAdmin(ctx context.Context, contract types.Address, epoch *types.Epoch) (*types.Address, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getAdmin", cfx)
	return cfx.GetAdmin(contract, toSlice(epoch)...)
}

func (api *cfxAPI) GetSponsorInfo(ctx context.Context, contract types.Address, epoch *types.Epoch) (types.SponsorInfo, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getSponsorInfo", cfx)
	return cfx.GetSponsorInfo(contract, toSlice(epoch)...)
}

func (api *cfxAPI) GetStakingBalance(ctx context.Context, address types.Address, epoch *types.Epoch) (*hexutil.Big, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getStakingBalance", cfx)
	return cfx.GetStakingBalance(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetDepositList(ctx context.Context, address types.Address, epoch *types.Epoch) ([]types.DepositInfo, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getDepositList", cfx)
	return cfx.GetDepositList(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetVoteList(ctx context.Context, address types.Address, epoch *types.Epoch) ([]types.VoteStakeInfo, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getVoteList", cfx)
	return cfx.GetVoteList(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetCollateralForStorage(ctx context.Context, address types.Address, epoch *types.Epoch) (*hexutil.Big, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getCollateralForStorage", cfx)
	return cfx.GetCollateralForStorage(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetCode(ctx context.Context, contract types.Address, epoch *types.Epoch) (hexutil.Bytes, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getCode", cfx)
	return cfx.GetCode(contract, toSlice(epoch)...)
}

func (api *cfxAPI) GetStorageAt(ctx context.Context, address types.Address, position *hexutil.Big, epoch *types.Epoch) (hexutil.Bytes, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getStorageAt", cfx)
	return cfx.GetStorageAt(address, position, toSlice(epoch)...)
}

func (api *cfxAPI) GetStorageRoot(ctx context.Context, address types.Address, epoch *types.Epoch) (*types.StorageRoot, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getStorageRoot", cfx)
	return cfx.GetStorageRoot(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetBlockByHash(ctx context.Context, blockHash types.Hash, includeTxs bool) (interface{}, error) {
	metrics.Registry.RPC.Percentage("cfx_getBlockByHash", "includeTxs").Mark(includeTxs)

	logger := logrus.WithFields(logrus.Fields{"blockHash": blockHash, "includeTxs": includeTxs})

	if !util.IsInterfaceValNil(api.StoreHandler) {
		block, err := api.StoreHandler.GetBlockByHash(ctx, blockHash, includeTxs)

		logger.WithError(err).Debug("Delegated `cfx_getBlockByHash` to store handler")
		api.collectHitStats("cfx_getBlockByHash", err == nil)

		if err == nil {
			return block, nil
		}
	}

	cfx := GetCfxClientFromContext(ctx)

	logger.WithField("nodeUrl", cfx.GetNodeURL()).Debug("Delegating `cfx_getBlockByHash` to fullnode")

	if includeTxs {
		return cfx.GetBlockByHash(blockHash)
	}

	return cfx.GetBlockSummaryByHash(blockHash)
}

func (api *cfxAPI) GetBlockByHashWithPivotAssumption(
	ctx context.Context, blockHash, pivotHash types.Hash, epoch hexutil.Uint64,
) (types.Block, error) {
	cfx := GetCfxClientFromContext(ctx)
	return cfx.GetBlockByHashWithPivotAssumption(blockHash, pivotHash, epoch)
}

func (api *cfxAPI) GetBlockByEpochNumber(ctx context.Context, epoch types.Epoch, includeTxs bool) (interface{}, error) {
	metrics.Registry.RPC.Percentage("cfx_getBlockByEpochNumber", "includeTxs").Mark(includeTxs)

	logger := logrus.WithFields(logrus.Fields{"epoch": epoch, "includeTxs": includeTxs})

	cfx := GetCfxClientFromContext(ctx)

	api.inputEpochMetric.Update(&epoch, "cfx_getBlockByEpochNumber", cfx)

	if !util.IsInterfaceValNil(api.StoreHandler) {
		block, err := api.StoreHandler.GetBlockByEpochNumber(ctx, &epoch, includeTxs)

		logger.WithError(err).Debug("Delegated `cfx_getBlockByEpochNumber` to store handler")
		api.collectHitStats("cfx_getBlockByEpochNumber", err == nil)

		if err == nil {
			return block, nil
		}
	}

	logger.WithField("nodeUrl", cfx.GetNodeURL()).Debug("Delegating `cfx_getBlockByEpochNumber` to fullnode")

	if includeTxs {
		return cfx.GetBlockByEpoch(&epoch)
	}

	return cfx.GetBlockSummaryByEpoch(&epoch)
}

func (api *cfxAPI) GetBlockByBlockNumber(
	ctx context.Context, blockNumer hexutil.Uint64, includeTxs bool) (interface{}, error) {
	metrics.Registry.RPC.Percentage("cfx_getBlockByBlockNumber", "details").Mark(includeTxs)

	logger := logrus.WithFields(logrus.Fields{"blockNumber": blockNumer, "includeTxs": includeTxs})

	if !util.IsInterfaceValNil(api.StoreHandler) {
		block, err := api.StoreHandler.GetBlockByBlockNumber(ctx, blockNumer, includeTxs)

		logger.WithError(err).Debug("Delegated `cfx_getBlockByBlockNumber` to store handler")
		api.collectHitStats("cfx_getBlockByBlockNumber", err == nil)

		if err == nil {
			return block, nil
		}
	}

	cfx := GetCfxClientFromContext(ctx)

	logger.WithField("nodeUrl", cfx.GetNodeURL()).Debug("Delegating `cfx_getBlockByBlockNumber` to fullnode")

	if includeTxs {
		return cfx.GetBlockByBlockNumber(blockNumer)
	}

	return cfx.GetBlockSummaryByBlockNumber(blockNumer)
}

func (api *cfxAPI) GetBestBlockHash(ctx context.Context) (types.Hash, error) {
	cfx := GetCfxClientFromContext(ctx)
	return cache.CfxDefault.GetBestBlockHash(cfx)
}

func (api *cfxAPI) GetNextNonce(ctx context.Context, address types.Address, epoch *types.Epoch) (*hexutil.Big, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getNextNonce", cfx)
	return cfx.GetNextNonce(address, toSlice(epoch)...)
}

func (api *cfxAPI) SendRawTransaction(ctx context.Context, signedTx hexutil.Bytes) (types.Hash, error) {
	cfx := GetCfxClientFromContext(ctx)
	txHash, err := cfx.SendRawTransaction(signedTx)
	if err == nil && api.Relayer != nil {
		// relay transaction broadcasting asynchronously
		if !api.Relayer.AsyncRelay(signedTx) {
			logrus.Info("Transaction relay pool is full, dropping transaction relay")
		}
	}

	return txHash, err
}

func (api *cfxAPI) Call(ctx context.Context, request types.CallRequest, epoch *types.Epoch) (hexutil.Bytes, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_call", cfx)
	return cfx.Call(request, epoch)
}

func (api *cfxAPI) GetLogs(ctx context.Context, filter types.LogFilter) ([]types.Log, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.metricLogFilter(cfx, &filter)

	flag, ok := store.ParseLogFilterType(&filter)
	if !ok {
		logrus.WithField("filter", filter).Debug("Failed to parse log filter type for cfx_getLogs")
		return emptyLogs, errInvalidLogFilter
	}

	if err := api.normalizeLogFilter(cfx, flag, &filter); err != nil {
		logrus.WithField("filter", filter).WithError(err).Debug("Failed to normalize log filter type for cfx_getLogs")
		return emptyLogs, err
	}

	if err := api.validateLogFilter(flag, &filter); err != nil {
		logrus.WithField("filter", filter).WithError(err).Debug("Invalid log filter parameter for cfx_getLogs rpc request")
		return emptyLogs, err
	}

	if api.LogApiHandler != nil {
		logs, hitStore, err := api.LogApiHandler.GetLogs(ctx, cfx, &filter)

		logrus.WithFields(logrus.Fields{
			"filter": filter, "hitStore": hitStore,
		}).WithError(err).Debug("Delegated `cfx_getLogs` to log api handler")
		api.collectHitStats("cfx_getLogs", hitStore)

		if logs == nil { // uniform empty logs
			logs = emptyLogs
		}

		return logs, err
	}

	// fail over to fullnode if no handler configured
	logrus.WithFields(logrus.Fields{
		"filter": filter, "nodeUrl": cfx.GetNodeURL(),
	}).Debug("Fail over `cfx_getLogs` to fullnode due to no API handler configured")

	return cfx.GetLogs(filter)
}

func (api *cfxAPI) normalizeLogFilter(cfx sdk.ClientOperator, flag store.LogFilterType, filter *types.LogFilter) error {
	// set default epoch range if not set and convert to numbered epoch if necessary
	if flag&store.LogFilterTypeEpochRange != 0 {
		// if no from epoch provided, set latest checkpoint epoch as default
		if filter.FromEpoch == nil {
			filter.FromEpoch = types.EpochLatestCheckpoint
		}

		// if no to epoch provided, set latest state epoch as default
		if filter.ToEpoch == nil {
			filter.ToEpoch = types.EpochLatestState
		}

		var epochs [2]*types.Epoch
		for i, e := range []*types.Epoch{filter.FromEpoch, filter.ToEpoch} {
			epoch, err := util.ConvertToNumberedEpoch(cfx, e)
			if err != nil {
				return errors.WithMessagef(err, "failed to convert numbered epoch for %v", e)
			}

			epochs[i] = epoch
		}

		filter.FromEpoch, filter.ToEpoch = epochs[0], epochs[1]
	}

	// For store v2, filter offset/limit are not supported anymore, but fullnode doesn't
	// deprecate it until v2.0.3.
	// TODO: remove the following codes once fullnode v2.0.3 is ready.
	filter.Offset, filter.Limit = nil, nil

	return nil
}

func (api *cfxAPI) validateLogFilter(flag store.LogFilterType, filter *types.LogFilter) error {
	switch {
	case flag&store.LogFilterTypeBlockHash != 0: // validate block hash log filter
		if len(filter.BlockHashes) > store.MaxLogBlockHashesSize {
			return errExceedLogFilterBlockHashLimit(len(filter.BlockHashes))
		}
	case flag&store.LogFilterTypeBlockRange != 0: // validate block range log filter
		// both fromBlock and toBlock must be provided
		if filter.FromBlock == nil || filter.ToBlock == nil {
			return errInvalidLogFilter
		}

		fromBlock := filter.FromBlock.ToInt().Uint64()
		toBlock := filter.ToBlock.ToInt().Uint64()

		if fromBlock > toBlock {
			return errInvalidLogFilterBlockRange
		}

	case flag&store.LogFilterTypeEpochRange != 0: // validate epoch range log filter
		epochFrom, _ := filter.FromEpoch.ToInt()
		epochTo, _ := filter.ToEpoch.ToInt()

		ef := epochFrom.Uint64()
		et := epochTo.Uint64()

		if ef > et {
			return errInvalidLogFilterEpochRange
		}
	}

	return nil
}

func (api *cfxAPI) GetTransactionByHash(ctx context.Context, txHash types.Hash) (*types.Transaction, error) {
	logger := logrus.WithFields(logrus.Fields{"txHash": txHash})

	if !util.IsInterfaceValNil(api.StoreHandler) {
		txn, err := api.StoreHandler.GetTransactionByHash(ctx, txHash)

		logger.WithError(err).Debug("Delegated `cfx_getTransactionByHash` to store handler")
		api.collectHitStats("cfx_getTransactionByHash", err == nil)

		if err == nil {
			return txn, nil
		}
	}

	cfx := GetCfxClientFromContext(ctx)

	logger.WithField("nodeUrl", cfx.GetNodeURL()).Debug("Delegating `cfx_getTransactionByHash` to fullnode")
	return cfx.GetTransactionByHash(txHash)
}

func (api *cfxAPI) EstimateGasAndCollateral(ctx context.Context, request types.CallRequest, epoch *types.Epoch) (types.Estimate, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_estimateGasAndCollateral", cfx)
	return cfx.EstimateGasAndCollateral(request, toSlice(epoch)...)
}

func (api *cfxAPI) CheckBalanceAgainstTransaction(
	ctx context.Context, account, contract types.Address, gas, price, storage *hexutil.Big, epoch *types.Epoch,
) (types.CheckBalanceAgainstTransactionResponse, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_checkBalanceAgainstTransaction", cfx)
	return cfx.CheckBalanceAgainstTransaction(account, contract, gas, price, storage, toSlice(epoch)...)
}

func (api *cfxAPI) GetBlocksByEpoch(ctx context.Context, epoch types.Epoch) ([]types.Hash, error) {
	logger := logrus.WithFields(logrus.Fields{"epoch": epoch})

	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(&epoch, "cfx_getBlocksByEpoch", cfx)

	if !util.IsInterfaceValNil(api.StoreHandler) {
		blocks, err := api.StoreHandler.GetBlocksByEpoch(ctx, &epoch)

		logger.WithError(err).Debug("Delegated `cfx_getBlocksByEpoch` to store handler")
		api.collectHitStats("cfx_getBlocksByEpoch", err == nil)

		if err == nil {
			return blocks, nil
		}
	}

	logger.WithField("nodeUrl", cfx.GetNodeURL()).Debug("Delegating `cfx_getBlocksByEpoch` to fullnode")

	return cfx.GetBlocksByEpoch(&epoch)
}

func (api *cfxAPI) GetSkippedBlocksByEpoch(ctx context.Context, epoch types.Epoch) ([]types.Hash, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(&epoch, "cfx_getSkippedBlocksByEpoch", cfx)
	return cfx.GetSkippedBlocksByEpoch(&epoch)
}

func (api *cfxAPI) GetTransactionReceipt(ctx context.Context, txHash types.Hash) (*types.TransactionReceipt, error) {
	logger := logrus.WithFields(logrus.Fields{"txHash": txHash})

	if !util.IsInterfaceValNil(api.StoreHandler) {
		rcpt, err := api.StoreHandler.GetTransactionReceipt(ctx, txHash)

		logger.WithError(err).Debug("Delegated `cfx_getTransactionReceipt` to store handler")
		api.collectHitStats("cfx_getTransactionReceipt", err == nil)

		if err == nil {
			return rcpt, nil
		}
	}

	cfx := GetCfxClientFromContext(ctx)
	logger.WithField("nodeUrl", cfx.GetNodeURL()).Debug("Delegating `cfx_getTransactionReceipt` to fullnode")
	receipt, err := cfx.GetTransactionReceipt(txHash)
	if err == nil {
		metrics.Registry.RPC.Percentage("cfx_getTransactionReceipt", "notfound").Mark(receipt == nil)
	}

	return receipt, err
}

func (api *cfxAPI) GetAccount(ctx context.Context, address types.Address, epoch *types.Epoch) (types.AccountInfo, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getAccount", cfx)
	return cfx.GetAccountInfo(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetInterestRate(ctx context.Context, epoch *types.Epoch) (*hexutil.Big, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getInterestRate", cfx)
	return cfx.GetInterestRate(epoch)
}

func (api *cfxAPI) GetAccumulateInterestRate(ctx context.Context, epoch *types.Epoch) (*hexutil.Big, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getAccumulateInterestRate", cfx)
	return cfx.GetAccumulateInterestRate(toSlice(epoch)...)
}

func (api *cfxAPI) GetConfirmationRiskByHash(ctx context.Context, blockHash types.Hash) (*hexutil.Big, error) {
	return GetCfxClientFromContext(ctx).GetRawBlockConfirmationRisk(blockHash)
}

func (api *cfxAPI) GetStatus(ctx context.Context) (types.Status, error) {
	cfx := GetCfxClientFromContext(ctx)
	return cache.CfxDefault.GetStatus(cfx)
}

func (api *cfxAPI) GetBlockRewardInfo(ctx context.Context, epoch types.Epoch) ([]types.RewardInfo, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(&epoch, "cfx_getBlockRewardInfo", cfx)
	return cfx.GetBlockRewardInfo(epoch)
}

func (api *cfxAPI) ClientVersion(ctx context.Context) (string, error) {
	cfx := GetCfxClientFromContext(ctx)
	return cache.CfxDefault.GetClientVersion(cfx)
}

func (api *cfxAPI) GetSupplyInfo(ctx context.Context, epoch *types.Epoch) (types.TokenSupplyInfo, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getSupplyInfo", cfx)
	return cfx.GetSupplyInfo(toSlice(epoch)...)
}

func (api *cfxAPI) GetAccountPendingInfo(ctx context.Context, address types.Address) (*types.AccountPendingInfo, error) {
	return GetCfxClientFromContext(ctx).GetAccountPendingInfo(address)
}

func (api *cfxAPI) GetAccountPendingTransactions(
	ctx context.Context, address types.Address, startNonce *hexutil.Big, limit *hexutil.Uint64,
) (types.AccountPendingTransactions, error) {
	return GetCfxClientFromContext(ctx).GetAccountPendingTransactions(address, startNonce, limit)
}

func (api *cfxAPI) GetPoSEconomics(ctx context.Context, epoch ...*types.Epoch) (types.PoSEconomics, error) {
	return GetCfxClientFromContext(ctx).GetPoSEconomics(epoch...)
}

func (api *cfxAPI) GetOpenedMethodGroups(ctx context.Context) (openedGroups []string, err error) {
	return GetCfxClientFromContext(ctx).GetOpenedMethodGroups()
}

func (api *cfxAPI) GetPoSRewardByEpoch(ctx context.Context, epoch types.Epoch) (reward *postypes.EpochReward, err error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(&epoch, "cfx_getPoSRewardByEpoch", cfx)
	return cfx.GetPoSRewardByEpoch(epoch)
}

// PubSub notification

// NewHeads send a notification each time a new header (block) is appended to the chain.
func (api *cfxAPI) NewHeads(ctx context.Context) (*rpc.Subscription, error) {
	psCtx, supported, err := api.pubsubCtxFromContext(ctx)

	if !supported {
		logrus.WithError(err).Error("NewHeads pubsub notification unsupported")
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	if err != nil {
		logrus.WithError(err).Error("NewHeads pubsub context error")
		return &rpc.Subscription{}, errSubscriptionProxyError
	}

	rpcSub := psCtx.notifier.CreateSubscription()

	headersCh := make(chan *types.BlockHeader, pubsubChannelBufferSize)
	dClient := getOrNewDelegateClient(psCtx.cfx)

	dSub, err := dClient.delegateSubscribeNewHeads(rpcSub.ID, headersCh)
	if err != nil {
		logrus.WithError(err).Error("Failed to delegate pubsub NewHeads")
		return &rpc.Subscription{}, errSubscriptionProxyError
	}

	logger := logrus.WithField("rpcSubID", rpcSub.ID)

	go func() {
		defer dSub.unsubscribe()

		for {
			select {
			case blockHeader := <-headersCh:
				logger.WithField("blockHeader", blockHeader).Debug("Received new block header from pubsub delegate")
				psCtx.notifier.Notify(rpcSub.ID, blockHeader)

			case err = <-dSub.err: // delegate subscription error
				logger.WithError(err).Debug("Received error from newHeads pubsub delegate")
				psCtx.rpcClient.Close()
				return

			case err = <-rpcSub.Err():
				logger.WithError(err).Debug("NewHeads pubsub subscription error")
				return

			case <-psCtx.notifier.Closed():
				logger.Debug("NewHeads pubsub connection closed")
				return
			}
		}
	}()

	return rpcSub, nil
}

// Epochs send a notification each time a new epoch is appended to the chain.
func (api *cfxAPI) Epochs(ctx context.Context, subEpoch *types.Epoch) (*rpc.Subscription, error) {
	if subEpoch == nil {
		subEpoch = types.EpochLatestMined
	}

	if !subEpoch.Equals(types.EpochLatestMined) && !subEpoch.Equals(types.EpochLatestState) {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	psCtx, supported, err := api.pubsubCtxFromContext(ctx)
	if !supported {
		logrus.WithError(err).Errorf("Epochs pubsub notification unsupported (%v)", subEpoch)
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	if err != nil {
		logrus.WithError(err).Errorf("Epochs pubsub context error (%v)", subEpoch)
		return &rpc.Subscription{}, errSubscriptionProxyError
	}

	rpcSub := psCtx.notifier.CreateSubscription()

	epochsCh := make(chan *types.WebsocketEpochResponse, pubsubChannelBufferSize)
	dClient := getOrNewDelegateClient(psCtx.cfx)

	dSub, err := dClient.delegateSubscribeEpochs(rpcSub.ID, epochsCh, *subEpoch)
	if err != nil {
		logrus.WithError(err).Errorf("Failed to delegate pubsub epochs subscription (%v)", subEpoch)
		return &rpc.Subscription{}, errSubscriptionProxyError
	}

	logger := logrus.WithField("rpcSubID", rpcSub.ID)

	go func() {
		defer dSub.unsubscribe()

		for {
			select {
			case epoch := <-epochsCh:
				logger.WithField("epoch", epoch).Debugf("Received new epoch from pubsub delegate (%v)", subEpoch)
				psCtx.notifier.Notify(rpcSub.ID, epoch)

			case err = <-dSub.err: // delegate subscription error
				logger.WithError(err).Debugf("Received error from epochs pubsub delegate (%v)", subEpoch)
				psCtx.rpcClient.Close()
				return

			case err = <-rpcSub.Err():
				logger.WithError(err).Debugf("Epochs pubsub subscription error (%v)", subEpoch)
				return

			case <-psCtx.notifier.Closed():
				logger.Debugf("Epochs pubsub connection closed (%v)", subEpoch)
				return
			}
		}
	}()

	return rpcSub, nil
}

// Logs creates a subscription that fires for all new log that match the given filter criteria.
func (api *cfxAPI) Logs(ctx context.Context, filter types.LogFilter) (*rpc.Subscription, error) {
	psCtx, supported, err := api.pubsubCtxFromContext(ctx)
	if !supported {
		logrus.WithError(err).Error("Logs pubsub notification unsupported")
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	if err != nil {
		logrus.WithError(err).Error("Logs pubsub context error")
		return &rpc.Subscription{}, errSubscriptionProxyError
	}

	rpcSub := psCtx.notifier.CreateSubscription()

	logsCh := make(chan *types.SubscriptionLog, pubsubChannelBufferSize)
	dClient := getOrNewDelegateClient(psCtx.cfx)

	dSub, err := dClient.delegateSubscribeLogs(rpcSub.ID, logsCh, filter)
	if err != nil {
		logrus.WithField("filter", filter).WithError(err).Error("Failed to delegate pubsub logs subscription")
		return &rpc.Subscription{}, errSubscriptionProxyError
	}

	logger := logrus.WithField("rpcSubID", rpcSub.ID)

	go func() {
		defer dSub.unsubscribe()

		for {
			select {
			case log := <-logsCh:
				logger.WithField("log", log).Debug("Received new log from pubsub delegate")
				psCtx.notifier.Notify(rpcSub.ID, log)

			case err = <-dSub.err: // delegate subscription error
				logger.WithError(err).Debug("Received error from logs pubsub delegate")
				psCtx.rpcClient.Close()
				return

			case err = <-rpcSub.Err():
				logger.WithError(err).Debugf("Logs pubsub subscription error")
				return

			case <-psCtx.notifier.Closed():
				logger.Debugf("Logs pubsub connection closed")
				return
			}
		}
	}()

	return rpcSub, nil
}

type pubsubContext struct {
	notifier  *rpc.Notifier
	rpcClient *rpc.Client
	cfx       sdk.ClientOperator
}

// pubsubCtxFromContext returns the pubsub context with member variables stored in ctx, if any.
func (api *cfxAPI) pubsubCtxFromContext(ctx context.Context) (psCtx *pubsubContext, supported bool, err error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		err = errors.New("failed to get notifier from context")
		return
	}

	rpcClient, supported := rpcClientFromContext(ctx)
	if !supported {
		err = errors.New("failed to get rpc client from context")
		return
	}

	cfx, err := api.provider.GetClientByIPGroup(ctx, node.GroupCfxWs)
	if err != nil {
		err = errors.WithMessage(err, "failed to get cfx wsclient by ip")
		return
	}

	psCtx = &pubsubContext{notifier, rpcClient, cfx}
	return
}

func (api *cfxAPI) metricLogFilter(cfx sdk.ClientOperator, filter *types.LogFilter) {
	isBlockRange := filter.FromBlock != nil || filter.ToBlock != nil
	isBlockHashes := len(filter.BlockHashes) > 0
	isEpochRange := !isBlockRange && !isBlockHashes
	metrics.Registry.RPC.Percentage("cfx_getLogs", "filter/epochRange").Mark(isEpochRange)
	metrics.Registry.RPC.Percentage("cfx_getLogs", "filter/blockRange").Mark(isBlockRange)
	metrics.Registry.RPC.Percentage("cfx_getLogs", "filter/hashes").Mark(isBlockHashes)
	metrics.Registry.RPC.Percentage("cfx_getLogs", "filter/address/null").Mark(len(filter.Address) == 0)
	metrics.Registry.RPC.Percentage("cfx_getLogs", "filter/address/single").Mark(len(filter.Address) == 1)
	metrics.Registry.RPC.Percentage("cfx_getLogs", "filter/address/multiple").Mark(len(filter.Address) > 1)
	metrics.Registry.RPC.Percentage("cfx_getLogs", "filter/topics").Mark(len(filter.Topics) > 0)

	// add metrics for the `epoch` filter only if block hash and block number range are not specified.
	if len(filter.BlockHashes) == 0 && filter.FromBlock == nil && filter.ToBlock == nil {
		api.inputEpochMetric.Update(filter.FromEpoch, "cfx_getLogs/from", cfx)
		api.inputEpochMetric.Update(filter.ToEpoch, "cfx_getLogs/to", cfx)
	}
}

func (h *cfxAPI) collectHitStats(method string, hit bool) {
	metrics.Registry.RPC.StoreHit(method, "store").Mark(hit)
}
