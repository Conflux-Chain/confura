package rpc

import (
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/metrics"
)

var emptyEpochs = []*types.Epoch{}

type cfxAPI struct {
	cfx              sdk.ClientOperator
	inputEpochMetric *inputEpochMetric
}

func newCfxAPI(cfx sdk.ClientOperator) *cfxAPI {
	return &cfxAPI{
		cfx:              cfx,
		inputEpochMetric: newInputEpochMetric(cfx),
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

func (api *cfxAPI) GetCode(contract types.Address, epoch *types.Epoch) (string, error) {
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
	api.inputEpochMetric.update(filter.FromEpoch, "cfx_getLogs/from")
	api.inputEpochMetric.update(filter.ToEpoch, "cfx_getLogs/to")
	return api.cfx.GetLogs(filter)
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
