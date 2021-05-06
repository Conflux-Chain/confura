package rpc

import (
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var emptyEpochs = []*types.Epoch{}
var emptyLogs = []types.Log{}

type cfxAPI struct {
	cfx              sdk.ClientOperator
	inputEpochMetric *inputEpochMetric
	db               store.Store
}

func newCfxAPI(cfx sdk.ClientOperator, db store.Store) *cfxAPI {
	return &cfxAPI{
		cfx:              cfx,
		inputEpochMetric: newInputEpochMetric(cfx),
		db:               db,
	}
}

func toSlice(epoch *types.Epoch) []*types.Epoch {
	if epoch == nil {
		return emptyEpochs
	}

	return []*types.Epoch{epoch}
}

func (api *cfxAPI) GasPrice() (*hexutil.Big, error) {
	return api.cfx.GetGasPrice()
}

func (api *cfxAPI) EpochNumber(epoch *types.Epoch) (*hexutil.Big, error) {
	api.inputEpochMetric.update(epoch, "cfx_epochNumber")
	return api.cfx.GetEpochNumber(toSlice(epoch)...)
}

func (api *cfxAPI) GetBalance(address types.Address, epoch *types.Epoch) (*hexutil.Big, error) {
	api.inputEpochMetric.update(epoch, "cfx_getBalance")
	return api.cfx.GetBalance(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetAdmin(contract types.Address, epoch *types.Epoch) (*types.Address, error) {
	api.inputEpochMetric.update(epoch, "cfx_getAdmin")
	return api.cfx.GetAdmin(contract, toSlice(epoch)...)
}

func (api *cfxAPI) GetSponsorInfo(contract types.Address, epoch *types.Epoch) (types.SponsorInfo, error) {
	api.inputEpochMetric.update(epoch, "cfx_getSponsorInfo")
	return api.cfx.GetSponsorInfo(contract, toSlice(epoch)...)
}

func (api *cfxAPI) GetStakingBalance(address types.Address, epoch *types.Epoch) (*hexutil.Big, error) {
	api.inputEpochMetric.update(epoch, "cfx_getStakingBalance")
	return api.cfx.GetStakingBalance(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetDepositList(address types.Address, epoch *types.Epoch) ([]types.DepositInfo, error) {
	api.inputEpochMetric.update(epoch, "cfx_getDepositList")
	return api.cfx.GetDepositList(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetVoteList(address types.Address, epoch *types.Epoch) ([]types.VoteStakeInfo, error) {
	api.inputEpochMetric.update(epoch, "cfx_getVoteList")
	return api.cfx.GetVoteList(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetCollateralForStorage(address types.Address, epoch *types.Epoch) (*hexutil.Big, error) {
	api.inputEpochMetric.update(epoch, "cfx_getCollateralForStorage")
	return api.cfx.GetCollateralForStorage(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetCode(contract types.Address, epoch *types.Epoch) (hexutil.Bytes, error) {
	api.inputEpochMetric.update(epoch, "cfx_getCode")
	return api.cfx.GetCode(contract, toSlice(epoch)...)
}

func (api *cfxAPI) GetStorageAt(address types.Address, position types.Hash, epoch *types.Epoch) (hexutil.Bytes, error) {
	api.inputEpochMetric.update(epoch, "cfx_getStorageAt")
	return api.cfx.GetStorageAt(address, position, toSlice(epoch)...)
}

func (api *cfxAPI) GetStorageRoot(address types.Address, epoch *types.Epoch) (*types.StorageRoot, error) {
	api.inputEpochMetric.update(epoch, "cfx_getStorageRoot")
	return api.cfx.GetStorageRoot(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetBlockByHash(blockHash types.Hash, includeTxs bool) (interface{}, error) {
	if includeTxs {
		metrics.GetOrRegisterGauge("rpc/cfx_getBlockByHash/details", nil).Inc(1)
		return api.cfx.GetBlockByHash(blockHash)
	}

	return api.cfx.GetBlockSummaryByHash(blockHash)
}

func (api *cfxAPI) GetBlockByHashWithPivotAssumption(blockHash, pivotHash types.Hash, epoch hexutil.Uint64) (types.Block, error) {
	return api.cfx.GetBlockByHashWithPivotAssumption(blockHash, pivotHash, epoch)
}

func (api *cfxAPI) GetBlockByEpochNumber(epoch *types.Epoch, includeTxs bool) (interface{}, error) {
	api.inputEpochMetric.update(epoch, "cfx_getBlockByEpochNumber")

	if includeTxs {
		metrics.GetOrRegisterGauge("rpc/cfx_getBlockByEpochNumber/details", nil).Inc(1)
		return api.cfx.GetBlockByEpoch(epoch)
	}

	return api.cfx.GetBlockSummaryByEpoch(epoch)
}

func (api *cfxAPI) GetBestBlockHash() (types.Hash, error) {
	return api.cfx.GetBestBlockHash()
}

func (api *cfxAPI) GetNextNonce(address types.Address, epoch *types.Epoch) (*hexutil.Big, error) {
	api.inputEpochMetric.update(epoch, "cfx_getNextNonce")
	return api.cfx.GetNextNonce(address, toSlice(epoch)...)
}

func (api *cfxAPI) SendRawTransaction(signedTx hexutil.Bytes) (types.Hash, error) {
	return api.cfx.SendRawTransaction(signedTx)
}

func (api *cfxAPI) Call(request types.CallRequest, epoch *types.Epoch) (hexutil.Bytes, error) {
	api.inputEpochMetric.update(epoch, "cfx_call")
	return api.cfx.Call(request, epoch)
}

func (api *cfxAPI) GetLogs(filter types.LogFilter) ([]types.Log, error) {
	if err := api.validateLogFilter(&filter); err != nil {
		return emptyLogs, err
	}

	api.inputEpochMetric.update(filter.FromEpoch, "cfx_getLogs/from")
	api.inputEpochMetric.update(filter.ToEpoch, "cfx_getLogs/to")

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
	return api.cfx.GetLogs(filter)
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

func (api *cfxAPI) GetTransactionByHash(txHash types.Hash) (*types.Transaction, error) {
	return api.cfx.GetTransactionByHash(txHash)
}

func (api *cfxAPI) EstimateGasAndCollateral(request types.CallRequest, epoch *types.Epoch) (types.Estimate, error) {
	api.inputEpochMetric.update(epoch, "cfx_estimateGasAndCollateral")
	return api.cfx.EstimateGasAndCollateral(request, toSlice(epoch)...)
}

func (api *cfxAPI) CheckBalanceAgainstTransaction(account, contract types.Address, gas, price, storage *hexutil.Big, epoch *types.Epoch) (types.CheckBalanceAgainstTransactionResponse, error) {
	api.inputEpochMetric.update(epoch, "cfx_checkBalanceAgainstTransaction")
	return api.cfx.CheckBalanceAgainstTransaction(account, contract, gas, price, storage, toSlice(epoch)...)
}

func (api *cfxAPI) GetBlocksByEpoch(epoch *types.Epoch) ([]types.Hash, error) {
	api.inputEpochMetric.update(epoch, "cfx_getBlocksByEpoch")
	return api.cfx.GetBlocksByEpoch(epoch)
}

func (api *cfxAPI) GetSkippedBlocksByEpoch(epoch *types.Epoch) ([]types.Hash, error) {
	api.inputEpochMetric.update(epoch, "cfx_getSkippedBlocksByEpoch")
	return api.cfx.GetSkippedBlocksByEpoch(epoch)
}

func (api *cfxAPI) GetTransactionReceipt(txHash types.Hash) (*types.TransactionReceipt, error) {
	return api.cfx.GetTransactionReceipt(txHash)
}

func (api *cfxAPI) GetAccount(address types.Address, epoch *types.Epoch) (types.AccountInfo, error) {
	api.inputEpochMetric.update(epoch, "cfx_getAccount")
	return api.cfx.GetAccountInfo(address, toSlice(epoch)...)
}

func (api *cfxAPI) GetInterestRate(epoch *types.Epoch) (*hexutil.Big, error) {
	api.inputEpochMetric.update(epoch, "cfx_getInterestRate")
	return api.cfx.GetInterestRate(epoch)
}

func (api *cfxAPI) GetAccumulateInterestRate(epoch *types.Epoch) (*hexutil.Big, error) {
	api.inputEpochMetric.update(epoch, "cfx_getAccumulateInterestRate")
	return api.cfx.GetAccumulateInterestRate(toSlice(epoch)...)
}

func (api *cfxAPI) GetConfirmationRiskByHash(blockHash types.Hash) (*hexutil.Big, error) {
	return api.cfx.GetRawBlockConfirmationRisk(blockHash)
}

func (api *cfxAPI) GetStatus() (types.Status, error) {
	return api.cfx.GetStatus()
}

func (api *cfxAPI) GetBlockRewardInfo(epoch types.Epoch) ([]types.RewardInfo, error) {
	api.inputEpochMetric.update(&epoch, "cfx_getBlockRewardInfo")
	return api.cfx.GetBlockRewardInfo(epoch)
}

func (api *cfxAPI) ClientVersion() (string, error) {
	return api.cfx.GetClientVersion()
}
