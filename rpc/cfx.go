package rpc

import (
	"context"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	emptyEpochs         = []*types.Epoch{}
	emptyLogs           = []types.Log{}
	emptyDepositInfos   = []types.DepositInfo{}
	emptyVoteStakeInfos = []types.VoteStakeInfo{}
	emptyHashes         = []types.Hash{}
	emptyRewards        = []types.RewardInfo{}
)

type cfxAPI struct {
	provider         *node.ClientProvider
	inputEpochMetric inputEpochMetric
	db               store.Store
}

func newCfxAPI(provider *node.ClientProvider, db store.Store) *cfxAPI {
	return &cfxAPI{
		provider: provider,
		db:       db,
	}
}

func toSlice(epoch *types.Epoch) []*types.Epoch {
	if epoch == nil {
		return emptyEpochs
	}

	return []*types.Epoch{epoch}
}

func (api *cfxAPI) GasPrice(ctx context.Context) (*hexutil.Big, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	return cfx.GetGasPrice()
}

func (api *cfxAPI) EpochNumber(ctx context.Context, epoch *types.Epoch) (*hexutil.Big, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputEpochMetric.update(epoch, "cfx_epochNumber", cfx)
	return cfx.GetEpochNumber(toSlice(epoch)...)
}

func (api *cfxAPI) GetBalance(ctx context.Context, address types.Address, epoch *types.Epoch) (*hexutil.Big, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputEpochMetric.update(epoch, "cfx_getBalance", cfx)
	return cfx.GetBalance(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetAdmin(ctx context.Context, contract types.Address, epoch *types.Epoch) (*types.Address, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputEpochMetric.update(epoch, "cfx_getAdmin", cfx)
	return cfx.GetAdmin(contract, toSlice(epoch)...)
}

func (api *cfxAPI) GetSponsorInfo(ctx context.Context, contract types.Address, epoch *types.Epoch) (types.SponsorInfo, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return types.SponsorInfo{}, err
	}

	api.inputEpochMetric.update(epoch, "cfx_getSponsorInfo", cfx)
	return cfx.GetSponsorInfo(contract, toSlice(epoch)...)
}

func (api *cfxAPI) GetStakingBalance(ctx context.Context, address types.Address, epoch *types.Epoch) (*hexutil.Big, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputEpochMetric.update(epoch, "cfx_getStakingBalance", cfx)
	return cfx.GetStakingBalance(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetDepositList(ctx context.Context, address types.Address, epoch *types.Epoch) ([]types.DepositInfo, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return emptyDepositInfos, err
	}

	api.inputEpochMetric.update(epoch, "cfx_getDepositList", cfx)
	return cfx.GetDepositList(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetVoteList(ctx context.Context, address types.Address, epoch *types.Epoch) ([]types.VoteStakeInfo, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return emptyVoteStakeInfos, err
	}

	api.inputEpochMetric.update(epoch, "cfx_getVoteList", cfx)
	return cfx.GetVoteList(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetCollateralForStorage(ctx context.Context, address types.Address, epoch *types.Epoch) (*hexutil.Big, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputEpochMetric.update(epoch, "cfx_getCollateralForStorage", cfx)
	return cfx.GetCollateralForStorage(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetCode(ctx context.Context, contract types.Address, epoch *types.Epoch) (hexutil.Bytes, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputEpochMetric.update(epoch, "cfx_getCode", cfx)
	return cfx.GetCode(contract, toSlice(epoch)...)
}

func (api *cfxAPI) GetStorageAt(ctx context.Context, address types.Address, position types.Hash, epoch *types.Epoch) (hexutil.Bytes, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputEpochMetric.update(epoch, "cfx_getStorageAt", cfx)
	return cfx.GetStorageAt(address, position, toSlice(epoch)...)
}

func (api *cfxAPI) GetStorageRoot(ctx context.Context, address types.Address, epoch *types.Epoch) (*types.StorageRoot, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputEpochMetric.update(epoch, "cfx_getStorageRoot", cfx)
	return cfx.GetStorageRoot(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetBlockByHash(ctx context.Context, blockHash types.Hash, includeTxs bool) (interface{}, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	if includeTxs {
		metrics.GetOrRegisterGauge("rpc/cfx_getBlockByHash/details", nil).Inc(1)
		return cfx.GetBlockByHash(blockHash)
	}

	return cfx.GetBlockSummaryByHash(blockHash)
}

func (api *cfxAPI) GetBlockByHashWithPivotAssumption(ctx context.Context, blockHash, pivotHash types.Hash, epoch hexutil.Uint64) (types.Block, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return types.Block{}, err
	}

	return cfx.GetBlockByHashWithPivotAssumption(blockHash, pivotHash, epoch)
}

func (api *cfxAPI) GetBlockByEpochNumber(ctx context.Context, epoch *types.Epoch, includeTxs bool) (interface{}, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputEpochMetric.update(epoch, "cfx_getBlockByEpochNumber", cfx)

	if includeTxs {
		metrics.GetOrRegisterGauge("rpc/cfx_getBlockByEpochNumber/details", nil).Inc(1)
		return cfx.GetBlockByEpoch(epoch)
	}

	return cfx.GetBlockSummaryByEpoch(epoch)
}

func (api *cfxAPI) GetBestBlockHash(ctx context.Context) (types.Hash, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return "", err
	}

	return cfx.GetBestBlockHash()
}

func (api *cfxAPI) GetNextNonce(ctx context.Context, address types.Address, epoch *types.Epoch) (*hexutil.Big, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputEpochMetric.update(epoch, "cfx_getNextNonce", cfx)
	return cfx.GetNextNonce(address, toSlice(epoch)...)
}

func (api *cfxAPI) SendRawTransaction(ctx context.Context, signedTx hexutil.Bytes) (types.Hash, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return "", err
	}

	return cfx.SendRawTransaction(signedTx)
}

func (api *cfxAPI) Call(ctx context.Context, request types.CallRequest, epoch *types.Epoch) (hexutil.Bytes, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputEpochMetric.update(epoch, "cfx_call", cfx)
	return cfx.Call(request, epoch)
}

func (api *cfxAPI) GetLogs(ctx context.Context, filter types.LogFilter) ([]types.Log, error) {
	if err := api.validateLogFilter(&filter); err != nil {
		return emptyLogs, err
	}

	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return emptyLogs, err
	}

	api.inputEpochMetric.update(filter.FromEpoch, "cfx_getLogs/from", cfx)
	api.inputEpochMetric.update(filter.ToEpoch, "cfx_getLogs/to", cfx)

	if dbFilter, ok := store.ParseLogFilter(&filter); ok {
		logs, err := api.db.GetLogs(dbFilter)

		// return empty slice rather than nil to comply with fullnode
		if logs == nil {
			logs = emptyLogs
		}

		if err == nil {
			return logs, nil
		}

		// for any error, delegate request to full node, including:
		// 1. database level error
		// 2. record not found (log range mismatch)
		if !api.db.IsRecordNotFound(err) {
			logrus.WithError(err).Fatal("Failed to get logs from database")
		}
	}

	logrus.Debug("Logs not found in database, delegated to fullnode")
	return cfx.GetLogs(filter)
}

func (api *cfxAPI) validateLogFilter(filter *types.LogFilter) error {
	// TODO validate against non-number case, e.g. latest_confirmed
	if epochFrom, ok := filter.FromEpoch.ToInt(); ok {
		if epochTo, ok := filter.ToEpoch.ToInt(); ok {
			epochFrom := epochFrom.Uint64()
			epochTo := epochTo.Uint64()

			if epochFrom > epochTo {
				return errors.New("invalid epoch range (from > to)")
			}

			if count := epochTo - epochFrom + 1; count > store.MaxLogEpochRange {
				return errors.Errorf("epoch range exceeds maximum value %v", store.MaxLogEpochRange)
			}
		}
	}

	if filter.Limit != nil && uint64(*filter.Limit) > store.MaxLogLimit {
		return errors.Errorf("limit field exceed the maximum value %v", store.MaxLogLimit)
	}

	return nil
}

func (api *cfxAPI) GetTransactionByHash(ctx context.Context, txHash types.Hash) (*types.Transaction, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	return cfx.GetTransactionByHash(txHash)
}

func (api *cfxAPI) EstimateGasAndCollateral(ctx context.Context, request types.CallRequest, epoch *types.Epoch) (types.Estimate, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return types.Estimate{}, err
	}

	api.inputEpochMetric.update(epoch, "cfx_estimateGasAndCollateral", cfx)
	return cfx.EstimateGasAndCollateral(request, toSlice(epoch)...)
}

func (api *cfxAPI) CheckBalanceAgainstTransaction(ctx context.Context, account, contract types.Address, gas, price, storage *hexutil.Big, epoch *types.Epoch) (types.CheckBalanceAgainstTransactionResponse, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return types.CheckBalanceAgainstTransactionResponse{}, err
	}

	api.inputEpochMetric.update(epoch, "cfx_checkBalanceAgainstTransaction", cfx)
	return cfx.CheckBalanceAgainstTransaction(account, contract, gas, price, storage, toSlice(epoch)...)
}

func (api *cfxAPI) GetBlocksByEpoch(ctx context.Context, epoch *types.Epoch) ([]types.Hash, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return emptyHashes, err
	}

	api.inputEpochMetric.update(epoch, "cfx_getBlocksByEpoch", cfx)
	return cfx.GetBlocksByEpoch(epoch)
}

func (api *cfxAPI) GetSkippedBlocksByEpoch(ctx context.Context, epoch *types.Epoch) ([]types.Hash, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return emptyHashes, err
	}

	api.inputEpochMetric.update(epoch, "cfx_getSkippedBlocksByEpoch", cfx)
	return cfx.GetSkippedBlocksByEpoch(epoch)
}

func (api *cfxAPI) GetTransactionReceipt(ctx context.Context, txHash types.Hash) (*types.TransactionReceipt, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	return cfx.GetTransactionReceipt(txHash)
}

func (api *cfxAPI) GetAccount(ctx context.Context, address types.Address, epoch *types.Epoch) (types.AccountInfo, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return types.AccountInfo{}, err
	}

	api.inputEpochMetric.update(epoch, "cfx_getAccount", cfx)
	return cfx.GetAccountInfo(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetInterestRate(ctx context.Context, epoch *types.Epoch) (*hexutil.Big, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputEpochMetric.update(epoch, "cfx_getInterestRate", cfx)
	return cfx.GetInterestRate(epoch)
}

func (api *cfxAPI) GetAccumulateInterestRate(ctx context.Context, epoch *types.Epoch) (*hexutil.Big, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	api.inputEpochMetric.update(epoch, "cfx_getAccumulateInterestRate", cfx)
	return cfx.GetAccumulateInterestRate(toSlice(epoch)...)
}

func (api *cfxAPI) GetConfirmationRiskByHash(ctx context.Context, blockHash types.Hash) (*hexutil.Big, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	return cfx.GetRawBlockConfirmationRisk(blockHash)
}

func (api *cfxAPI) GetStatus(ctx context.Context) (types.Status, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return types.Status{}, err
	}

	return cfx.GetStatus()
}

func (api *cfxAPI) GetBlockRewardInfo(ctx context.Context, epoch types.Epoch) ([]types.RewardInfo, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return emptyRewards, err
	}

	api.inputEpochMetric.update(&epoch, "cfx_getBlockRewardInfo", cfx)
	return cfx.GetBlockRewardInfo(epoch)
}

func (api *cfxAPI) ClientVersion(ctx context.Context) (string, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return "", err
	}

	return cfx.GetClientVersion()
}

func (api *cfxAPI) GetSupplyInfo(ctx context.Context, epoch *types.Epoch) (types.TokenSupplyInfo, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return types.TokenSupplyInfo{}, err
	}

	api.inputEpochMetric.update(epoch, "cfx_getSupplyInfo", cfx)
	return cfx.GetSupplyInfo(toSlice(epoch)...)
}

func (api *cfxAPI) GetAccountPendingInfo(ctx context.Context, address types.Address) (*types.AccountPendingInfo, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	return cfx.GetAccountPendingInfo(address)
}
