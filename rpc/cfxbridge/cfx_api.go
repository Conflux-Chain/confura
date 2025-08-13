package cfxbridge

import (
	"context"

	"github.com/Conflux-Chain/confura/store"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	rpcp "github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

type CfxAPI struct {
	w3c          *web3go.Client
	cfx          *sdk.Client // optional
	ethNetworkId uint32
}

func NewCfxAPI(w3client *web3go.Client, ethNetId uint32, cfxClient *sdk.Client) *CfxAPI {
	return &CfxAPI{
		w3c:          w3client,
		cfx:          cfxClient,
		ethNetworkId: ethNetId,
	}
}

func (api *CfxAPI) GasPrice(ctx context.Context) (*hexutil.Big, error) {
	return NormalizeBig(api.w3c.WithContext(ctx).Eth.GasPrice())
}

func (api *CfxAPI) EpochNumber(ctx context.Context, epoch *types.Epoch) (*hexutil.Big, error) {
	// By default, return lastest_state for eth space.
	if epoch == nil {
		epoch = types.EpochLatestState
	}

	if api.cfx != nil {
		return api.cfx.WithContext(ctx).GetEpochNumber(epoch)
	}

	var blockNum ethTypes.BlockNumber
	switch {
	case epoch.Equals(types.EpochEarliest):
		blockNum = ethTypes.EarliestBlockNumber
	case epoch.Equals(types.EpochLatestConfirmed):
		blockNum = ethTypes.SafeBlockNumber
	case epoch.Equals(types.EpochLatestState):
		blockNum = ethTypes.LatestBlockNumber
	case epoch.Equals(types.EpochLatestMined):
		blockNum = ethTypes.LatestBlockNumber
	case epoch.Equals(types.EpochLatestFinalized):
		blockNum = ethTypes.FinalizedBlockNumber
	case epoch.Equals(types.EpochLatestCheckpoint):
		return HexBig0, nil
	default:
		return nil, ErrEpochUnsupported
	}

	block, err := api.w3c.WithContext(ctx).Eth.BlockByNumber(blockNum, false)
	if err != nil {
		return nil, err
	}

	return (*hexutil.Big)(block.Number), nil
}

func (api *CfxAPI) GetBalance(ctx context.Context, address EthAddress, bn *EthBlockNumber) (*hexutil.Big, error) {
	return NormalizeBig(api.w3c.WithContext(ctx).Eth.Balance(address.value, bn.ToArg()))
}

func (api *CfxAPI) GetAdmin(ctx context.Context, contract EthAddress, bn *EthBlockNumber) (*string, error) {
	return nil, nil
}

func (api *CfxAPI) GetSponsorInfo(ctx context.Context, contract EthAddress, bn *EthBlockNumber) (types.SponsorInfo, error) {
	return types.SponsorInfo{
		SponsorForGas:               ConvertAddress(common.Address{}, api.ethNetworkId),
		SponsorForCollateral:        ConvertAddress(common.Address{}, api.ethNetworkId),
		SponsorGasBound:             HexBig0,
		SponsorBalanceForGas:        HexBig0,
		SponsorBalanceForCollateral: HexBig0,
	}, nil
}

func (api *CfxAPI) GetStakingBalance(ctx context.Context, address EthAddress, bn *EthBlockNumber) (*hexutil.Big, error) {
	return HexBig0, nil
}

func (api *CfxAPI) GetDepositList(ctx context.Context, address EthAddress, bn *EthBlockNumber) ([]types.DepositInfo, error) {
	return emptyDepositList, nil
}

func (api *CfxAPI) GetVoteList(ctx context.Context, address EthAddress, bn *EthBlockNumber) ([]types.VoteStakeInfo, error) {
	return emptyVoteList, nil
}

func (api *CfxAPI) GetCollateralForStorage(ctx context.Context, address EthAddress, bn *EthBlockNumber) (*hexutil.Big, error) {
	return HexBig0, nil
}

func (api *CfxAPI) GetCode(ctx context.Context, contract EthAddress, bn *EthBlockNumber) (hexutil.Bytes, error) {
	return api.w3c.WithContext(ctx).Eth.CodeAt(contract.value, bn.ToArg())
}

func (api *CfxAPI) GetStorageAt(ctx context.Context, address EthAddress, position *hexutil.Big, bn *EthBlockNumber) (common.Hash, error) {
	return api.w3c.WithContext(ctx).Eth.StorageAt(address.value, position.ToInt(), bn.ToArg())
}

func (api *CfxAPI) GetStorageRoot(ctx context.Context, address EthAddress, bn *EthBlockNumber) (*types.StorageRoot, error) {
	return nil, nil
}

func (api *CfxAPI) GetBlockByHash(ctx context.Context, blockHash common.Hash, includeTxs bool) (interface{}, error) {
	block, err := api.w3c.WithContext(ctx).Eth.BlockByHash(blockHash, includeTxs)
	if err != nil {
		return nil, err
	}

	if includeTxs {
		return ConvertBlock(block, api.ethNetworkId), nil
	}

	return ConvertBlockSummary(block, api.ethNetworkId), nil
}

func (api *CfxAPI) GetBlockByHashWithPivotAssumption(ctx context.Context, blockHash, pivotHash common.Hash, bn hexutil.Uint64) (*types.Block, error) {
	// Note, there is no referee blocks in ETH space, and only pivot block available in an epoch.
	// So, client should only query pivot block with this method.
	if blockHash != pivotHash {
		return nil, ErrInvalidBlockAssumption
	}

	block, err := api.w3c.WithContext(ctx).Eth.BlockByHash(blockHash, true)
	if err != nil {
		return nil, err
	}

	if block == nil {
		return nil, ErrInvalidBlockAssumption
	}

	if block.Number.Uint64() != uint64(bn) {
		return nil, ErrInvalidBlockAssumption
	}

	return ConvertBlock(block, api.ethNetworkId), nil
}

func (api *CfxAPI) GetBlockByEpochNumber(ctx context.Context, bn EthBlockNumber, includeTxs bool) (interface{}, error) {
	block, err := api.w3c.WithContext(ctx).Eth.BlockByNumber(bn.Value(), includeTxs)
	if err != nil {
		return nil, err
	}

	if includeTxs {
		return ConvertBlock(block, api.ethNetworkId), nil
	}

	return ConvertBlockSummary(block, api.ethNetworkId), nil
}

func (api *CfxAPI) GetBlockByBlockNumber(ctx context.Context, blockNumer hexutil.Uint64, includeTxs bool) (interface{}, error) {
	bn := EthBlockNumber{
		value: ethTypes.BlockNumber(blockNumer),
	}

	return api.GetBlockByEpochNumber(ctx, bn, includeTxs)
}

func (api *CfxAPI) GetBestBlockHash(ctx context.Context) (common.Hash, error) {
	block, err := api.w3c.WithContext(ctx).Eth.BlockByNumber(ethTypes.LatestBlockNumber, false)
	if err != nil {
		return common.Hash{}, err
	}

	if block == nil {
		return common.Hash{}, nil
	}

	return block.Hash, nil
}

func (api *CfxAPI) GetNextNonce(ctx context.Context, address EthAddress, bn *EthBlockNumber) (*hexutil.Big, error) {
	return NormalizeBig(api.w3c.WithContext(ctx).Eth.TransactionCount(address.value, bn.ToArg()))
}

func (api *CfxAPI) SendRawTransaction(ctx context.Context, signedTx hexutil.Bytes) (common.Hash, error) {
	return api.w3c.WithContext(ctx).Eth.SendRawTransaction(signedTx)
}

func (api *CfxAPI) Call(ctx context.Context, request EthCallRequest, bn *EthBlockNumber) (hexutil.Bytes, error) {
	callMsg, err := request.ToCallMsg()
	if err != nil {
		return nil, errors.WithMessage(err, "invalid call request")
	}
	return api.w3c.WithContext(ctx).Eth.Call(callMsg, bn.ToArg())
}

func (api *CfxAPI) GetLogs(ctx context.Context, filter EthLogFilter) ([]types.Log, error) {
	logs, err := api.w3c.WithContext(ctx).Eth.Logs(filter.ToFilterQuery())
	if err != nil {
		return nil, err
	}

	result := make([]types.Log, len(logs))
	for i := range logs {
		result[i] = *ConvertLog(&logs[i], api.ethNetworkId)
	}

	return result, nil
}

func (api *CfxAPI) GetTransactionByHash(ctx context.Context, txHash common.Hash) (*types.Transaction, error) {
	tx, err := api.w3c.WithContext(ctx).Eth.TransactionByHash(txHash)
	if err != nil {
		return nil, err
	}

	return ConvertTx(tx, api.ethNetworkId), nil
}

func (api *CfxAPI) EstimateGasAndCollateral(ctx context.Context, request EthCallRequest, bn *EthBlockNumber) (types.Estimate, error) {
	callMsg, err := request.ToCallMsg()
	if err != nil {
		return types.Estimate{}, errors.WithMessage(err, "invalid call request")
	}

	gasLimit, err := api.w3c.WithContext(ctx).Eth.EstimateGas(callMsg, bn.ToArg())
	if err != nil {
		return types.Estimate{}, err
	}

	gasUsed := float64(gasLimit.Uint64()) * 3.0 / 4.0

	return types.Estimate{
		GasLimit:              types.NewBigIntByRaw(gasLimit),
		GasUsed:               types.NewBigInt(uint64(gasUsed)),
		StorageCollateralized: HexBig0,
	}, nil
}

func (api *CfxAPI) GetBlocksByEpoch(ctx context.Context, bn EthBlockNumber) ([]common.Hash, error) {
	block, err := api.w3c.WithContext(ctx).Eth.BlockByNumber(bn.Value(), false)
	if err != nil {
		return nil, err
	}

	if block == nil {
		return []common.Hash{}, nil
	}

	return []common.Hash{block.Hash}, nil
}

func (api *CfxAPI) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (*types.TransactionReceipt, error) {
	receipt, err := api.w3c.WithContext(ctx).Eth.TransactionReceipt(txHash)
	if err != nil {
		return nil, err
	}

	return ConvertReceipt(receipt, api.ethNetworkId), nil
}

func (api *CfxAPI) GetEpochReceipts(ctx context.Context, epochOrBh types.EpochOrBlockHash) ([][]*types.TransactionReceipt, error) {
	bnh, err := NewEthBlockNumberOrHash(epochOrBh)
	if err != nil {
		return nil, err
	}

	receipts, err := store.QueryEthReceipt(ctx, api.w3c, *bnh.ToArg())
	if err != nil {
		return nil, err
	}

	result := make([]*types.TransactionReceipt, len(receipts))
	for i := range receipts {
		result[i] = ConvertReceipt(receipts[i], api.ethNetworkId)
	}

	return [][]*types.TransactionReceipt{result}, nil
}

func (api *CfxAPI) GetAccount(ctx context.Context, address EthAddress, bn *EthBlockNumber) (types.AccountInfo, error) {
	balance, err := api.w3c.WithContext(ctx).Eth.Balance(address.value, bn.ToArg())
	if err != nil {
		return types.AccountInfo{}, err
	}

	nonce, err := api.w3c.WithContext(ctx).Eth.TransactionCount(address.value, bn.ToArg())
	if err != nil {
		return types.AccountInfo{}, err
	}

	code, err := api.w3c.WithContext(ctx).Eth.CodeAt(address.value, bn.ToArg())
	if err != nil {
		return types.AccountInfo{}, err
	}

	return types.AccountInfo{
		Balance:                   types.NewBigIntByRaw(balance),
		Nonce:                     types.NewBigIntByRaw(nonce),
		CodeHash:                  types.Hash(crypto.Keccak256Hash(code).Hex()),
		StakingBalance:            HexBig0,
		CollateralForStorage:      HexBig0,
		AccumulatedInterestReturn: HexBig0,
		Admin:                     ConvertAddress(common.Address{}, api.ethNetworkId),
	}, nil
}

func (api *CfxAPI) GetInterestRate(ctx context.Context, bn *EthBlockNumber) (*hexutil.Big, error) {
	return HexBig0, nil
}

func (api *CfxAPI) GetAccumulateInterestRate(ctx context.Context, bn *EthBlockNumber) (*hexutil.Big, error) {
	return HexBig0, nil
}

func (api *CfxAPI) GetConfirmationRiskByHash(ctx context.Context, blockHash types.Hash) (*hexutil.Big, error) {
	if api.cfx != nil {
		return api.cfx.WithContext(ctx).GetRawBlockConfirmationRisk(blockHash)
	}

	block, err := api.w3c.Eth.BlockByHash(*blockHash.ToCommonHash(), false)
	if block == nil || err != nil {
		return nil, err
	}

	// TODO: calculate confirmation risk based on various confirmed blocks.
	return HexBig0, nil
}

func (api *CfxAPI) GetStatus(ctx context.Context) (status types.Status, err error) {
	if api.cfx != nil {
		return api.getCrossSpaceStatus(ctx)
	}

	var batchElems []rpcp.BatchElem
	for _, bn := range []rpcp.BlockNumber{
		ethTypes.LatestBlockNumber,
		ethTypes.SafeBlockNumber,
		ethTypes.FinalizedBlockNumber,
	} {
		batchElems = append(batchElems, rpcp.BatchElem{
			Method: "eth_getBlockByNumber",
			Args:   []interface{}{bn, false},
			Result: new(ethTypes.Block),
		})
	}

	if err := api.w3c.Provider().BatchCallContext(ctx, batchElems); err != nil {
		return types.Status{}, err
	}

	latestBlock := batchElems[0].Result.(*ethTypes.Block)
	safeBlock := batchElems[1].Result.(*ethTypes.Block)
	finBlock := batchElems[2].Result.(*ethTypes.Block)

	chainID := hexutil.Uint64(api.ethNetworkId)
	latestBlockNumber := hexutil.Uint64(latestBlock.Number.Uint64())

	return types.Status{
		BestHash:             ConvertHash(latestBlock.Hash),
		ChainID:              chainID,
		EthereumSpaceChainId: chainID,
		NetworkID:            chainID,
		EpochNumber:          latestBlockNumber,
		BlockNumber:          latestBlockNumber,
		LatestState:          latestBlockNumber,
		LatestConfirmed:      hexutil.Uint64(safeBlock.Number.Uint64()),
		LatestFinalized:      hexutil.Uint64(finBlock.Number.Uint64()),
		LatestCheckpoint:     0,
		PendingTxNumber:      0,
	}, nil
}

func (api *CfxAPI) getCrossSpaceStatus(ctx context.Context) (status types.Status, err error) {
	status, err = api.cfx.WithContext(ctx).GetStatus()
	if err != nil {
		return types.Status{}, err
	}

	block, err := api.w3c.WithContext(ctx).Eth.BlockByNumber(ethTypes.LatestBlockNumber, false)
	if err != nil {
		return types.Status{}, err
	}

	latestBlockNumber := hexutil.Uint64(block.Number.Uint64())

	status.BestHash = ConvertHash(block.Hash)
	status.ChainID = status.EthereumSpaceChainId
	status.NetworkID = status.EthereumSpaceChainId
	status.EpochNumber = latestBlockNumber
	status.BlockNumber = latestBlockNumber
	status.PendingTxNumber = 0
	status.LatestState = latestBlockNumber

	return status, nil
}

func (api *CfxAPI) GetBlockRewardInfo(ctx context.Context, epoch types.Epoch) ([]types.RewardInfo, error) {
	if api.cfx != nil {
		return api.cfx.WithContext(ctx).GetBlockRewardInfo(epoch)
	}

	// TODO: Calculate block reward based on the following implementation:
	// https://docs.alchemy.com/docs/how-to-calculate-ethereum-miner-rewards
	return []types.RewardInfo{}, nil
}

func (api *CfxAPI) ClientVersion(ctx context.Context) (string, error) {
	return api.w3c.WithContext(ctx).Eth.ClientVersion()
}

func (api *CfxAPI) GetSupplyInfo(ctx context.Context, epoch *types.Epoch) (types.TokenSupplyInfo, error) {
	if epoch == nil {
		epoch = types.EpochLatestState
	}

	if api.cfx != nil {
		result, err := api.cfx.WithContext(ctx).GetSupplyInfo(epoch)
		if err != nil {
			return types.TokenSupplyInfo{}, err
		}

		result.TotalCirculating = result.TotalEspaceTokens
		result.TotalIssued = result.TotalEspaceTokens
		result.TotalStaking = HexBig0
		result.TotalCollateral = HexBig0

		return result, nil
	}

	// TODO: Calculate supply info based on the following implementation:
	// https://github.com/lastmjs/eth-total-supply
	return types.TokenSupplyInfo{
		TotalCirculating:  HexBig0,
		TotalIssued:       HexBig0,
		TotalStaking:      HexBig0,
		TotalCollateral:   HexBig0,
		TotalEspaceTokens: HexBig0,
	}, nil
}
