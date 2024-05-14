package rpc

import (
	"context"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/rpc/cache"
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	vfclient "github.com/Conflux-Chain/confura/virtualfilter/client"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	postypes "github.com/Conflux-Chain/go-conflux-sdk/types/pos"
	logutil "github.com/Conflux-Chain/go-conflux-util/log"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/sirupsen/logrus"
)

const (
	rpcMethodCfxGetLogs = "cfx_getLogs"
)

var (
	emptyEpochs             = []*types.Epoch{}
	emptyEpochOrBlockHashes = []*types.EpochOrBlockHash{}
	emptyLogs               = []types.Log{}
)

type CfxAPIOption struct {
	StoreHandler        *handler.CfxStoreHandler
	LogApiHandler       *handler.CfxLogsApiHandler
	TxnHandler          *handler.CfxTxnHandler
	VirtualFilterClient *vfclient.CfxClient
}

// cfxAPI provides main proxy API for core space.
type cfxAPI struct {
	CfxAPIOption
	provider         *node.CfxClientProvider
	inputEpochMetric metrics.InputEpochMetric
	stateHandler     *handler.CfxStateHandler
	etPubsubLogger   *logutil.ErrorTolerantLogger
}

func newCfxAPI(provider *node.CfxClientProvider, option ...CfxAPIOption) *cfxAPI {
	var opt CfxAPIOption
	if len(option) > 0 {
		opt = option[0]
	}

	return &cfxAPI{
		CfxAPIOption:   opt,
		provider:       provider,
		stateHandler:   handler.NewCfxStateHandler(provider),
		etPubsubLogger: logutil.NewErrorTolerantLogger(logutil.DefaultETConfig),
	}
}

func toEpochSlice(epoch *types.Epoch) []*types.Epoch {
	if epoch == nil {
		return emptyEpochs
	}

	return []*types.Epoch{epoch}
}

func toEpochOrBlockHashSlice(epoch *types.EpochOrBlockHash) []*types.EpochOrBlockHash {
	if epoch == nil {
		return emptyEpochOrBlockHashes
	}

	return []*types.EpochOrBlockHash{epoch}
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

func (api *cfxAPI) GetBalance(ctx context.Context, address types.Address, epoch *types.EpochOrBlockHash) (*hexutil.Big, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update2(epoch, "cfx_getBalance", cfx)
	return api.stateHandler.GetBalance(ctx, cfx, address, toEpochOrBlockHashSlice(epoch)...)
}

func (api *cfxAPI) GetAdmin(ctx context.Context, contract types.Address, epoch *types.Epoch) (*types.Address, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getAdmin", cfx)
	return api.stateHandler.GetAdmin(ctx, cfx, contract, toEpochSlice(epoch)...)
}

func (api *cfxAPI) GetSponsorInfo(ctx context.Context, contract types.Address, epoch *types.Epoch) (types.SponsorInfo, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getSponsorInfo", cfx)
	return api.stateHandler.GetSponsorInfo(ctx, cfx, contract, toEpochSlice(epoch)...)
}

func (api *cfxAPI) GetStakingBalance(ctx context.Context, address types.Address, epoch *types.Epoch) (*hexutil.Big, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getStakingBalance", cfx)
	return api.stateHandler.GetStakingBalance(ctx, cfx, address, toEpochSlice(epoch)...)
}

func (api *cfxAPI) GetDepositList(ctx context.Context, address types.Address, epoch *types.Epoch) ([]types.DepositInfo, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getDepositList", cfx)
	return api.stateHandler.GetDepositList(ctx, cfx, address, toEpochSlice(epoch)...)
}

func (api *cfxAPI) GetVoteList(ctx context.Context, address types.Address, epoch *types.Epoch) ([]types.VoteStakeInfo, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getVoteList", cfx)
	return api.stateHandler.GetVoteList(ctx, cfx, address, toEpochSlice(epoch)...)
}

func (api *cfxAPI) GetCollateralInfo(ctx context.Context, epoch *types.Epoch) (info types.StorageCollateralInfo, err error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getCollateralInfo", cfx)
	return api.stateHandler.GetCollateralInfo(ctx, cfx, epoch)
}

func (api *cfxAPI) GetCollateralForStorage(ctx context.Context, address types.Address, epoch *types.Epoch) (*hexutil.Big, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getCollateralForStorage", cfx)
	return api.stateHandler.GetCollateralForStorage(ctx, cfx, address, toEpochSlice(epoch)...)
}

func (api *cfxAPI) GetCode(ctx context.Context, contract types.Address, epoch *types.EpochOrBlockHash) (hexutil.Bytes, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update2(epoch, "cfx_getCode", cfx)
	return api.stateHandler.GetCode(ctx, cfx, contract, toEpochOrBlockHashSlice(epoch)...)
}

func (api *cfxAPI) GetStorageAt(ctx context.Context, address types.Address, position *hexutil.Big, epoch *types.EpochOrBlockHash) (hexutil.Bytes, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update2(epoch, "cfx_getStorageAt", cfx)
	return api.stateHandler.GetStorageAt(ctx, cfx, address, position, toEpochOrBlockHashSlice(epoch)...)
}

func (api *cfxAPI) GetStorageRoot(ctx context.Context, address types.Address, epoch *types.Epoch) (*types.StorageRoot, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getStorageRoot", cfx)
	return api.stateHandler.GetStorageRoot(ctx, cfx, address, toEpochSlice(epoch)...)
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

func (api *cfxAPI) GetNextNonce(ctx context.Context, address types.Address, epoch *types.EpochOrBlockHash) (*hexutil.Big, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update2(epoch, "cfx_getNextNonce", cfx)
	return api.stateHandler.GetNextNonce(ctx, cfx, address, toEpochOrBlockHashSlice(epoch)...)
}

func (api *cfxAPI) SendRawTransaction(ctx context.Context, signedTx hexutil.Bytes) (types.Hash, error) {
	cfx := GetCfxClientFromContext(ctx)

	if api.TxnHandler != nil {
		cgroup := GetClientGroupFromContext(ctx)
		return api.TxnHandler.SendRawTxn(cfx, cgroup, signedTx)
	}

	return cfx.SendRawTransaction(signedTx)
}

func (api *cfxAPI) Call(ctx context.Context, request types.CallRequest, epoch *types.EpochOrBlockHash) (hexutil.Bytes, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update2(epoch, "cfx_call", cfx)
	return api.stateHandler.Call(ctx, cfx, request, epoch)
}

func (api *cfxAPI) GetLogs(ctx context.Context, fq types.LogFilter) ([]types.Log, error) {
	cfx := GetCfxClientFromContext(ctx)
	return api.getLogs(ctx, cfx, fq, rpcMethodCfxGetLogs)
}

// getLogs helper method to get logs from store or fullnode.
func (api *cfxAPI) getLogs(
	ctx context.Context,
	cfx sdk.ClientOperator,
	fq types.LogFilter,
	rpcMethod string,
) ([]types.Log, error) {
	metrics.UpdateCfxRpcLogFilter(rpcMethod, cfx, &fq)

	flag, ok := ParseLogFilterType(&fq)
	if !ok {
		return emptyLogs, ErrInvalidLogFilter
	}

	if err := NormalizeLogFilter(cfx, flag, &fq); err != nil {
		return emptyLogs, err
	}

	if err := ValidateLogFilter(flag, &fq); err != nil {
		return emptyLogs, err
	}

	if api.LogApiHandler != nil {
		logs, hitStore, err := api.LogApiHandler.GetLogs(ctx, cfx, &fq, rpcMethod)
		api.collectHitStats(rpcMethod, hitStore)
		return uniformCfxLogs(logs), err
	}

	// fail over to fullnode if no handler configured
	return cfx.GetLogs(fq)
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
	return api.stateHandler.EstimateGasAndCollateral(ctx, cfx, request, toEpochSlice(epoch)...)
}

func (api *cfxAPI) CheckBalanceAgainstTransaction(
	ctx context.Context, account, contract types.Address, gas, price, storage *hexutil.Big, epoch *types.Epoch,
) (types.CheckBalanceAgainstTransactionResponse, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_checkBalanceAgainstTransaction", cfx)
	return api.stateHandler.CheckBalanceAgainstTransaction(ctx, cfx, account, contract, gas, price, storage, toEpochSlice(epoch)...)
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
	return api.stateHandler.GetAccountInfo(ctx, cfx, address, toEpochSlice(epoch)...)
}

func (api *cfxAPI) GetInterestRate(ctx context.Context, epoch *types.Epoch) (*hexutil.Big, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getInterestRate", cfx)
	return api.stateHandler.GetInterestRate(ctx, cfx, epoch)
}

func (api *cfxAPI) GetAccumulateInterestRate(ctx context.Context, epoch *types.Epoch) (*hexutil.Big, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getAccumulateInterestRate", cfx)
	return api.stateHandler.GetAccumulateInterestRate(ctx, cfx, toEpochSlice(epoch)...)
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
	return api.stateHandler.GetSupplyInfo(ctx, cfx, toEpochSlice(epoch)...)
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
	cfx := GetCfxClientFromContext(ctx)
	return api.stateHandler.GetPoSEconomics(ctx, cfx, epoch...)
}

func (api *cfxAPI) GetOpenedMethodGroups(ctx context.Context) (openedGroups []string, err error) {
	return GetCfxClientFromContext(ctx).GetOpenedMethodGroups()
}

func (api *cfxAPI) GetPoSRewardByEpoch(ctx context.Context, epoch types.Epoch) (reward *postypes.EpochReward, err error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(&epoch, "cfx_getPoSRewardByEpoch", cfx)
	return cfx.GetPoSRewardByEpoch(epoch)
}

func (api *cfxAPI) GetParamsFromVote(ctx context.Context, epoch *types.Epoch) (postypes.VoteParamsInfo, error) {
	cfx := GetCfxClientFromContext(ctx)
	api.inputEpochMetric.Update(epoch, "cfx_getParamsFromVote", cfx)
	return api.stateHandler.GetParamsFromVote(ctx, cfx, epoch)
}

func (h *cfxAPI) collectHitStats(method string, hit bool) {
	metrics.Registry.RPC.StoreHit(method, "store").Mark(hit)
}
