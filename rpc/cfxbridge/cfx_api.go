package cfxbridge

import (
	"context"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/client"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

type CfxAPI struct {
	eth          *client.RpcEthClient
	cfx          *sdk.Client
	ethNetworkId uint32
}

func NewCfxAPI(ethNodeURL, cfxNodeURL string) (*CfxAPI, error) {
	eth, err := web3go.NewClient(ethNodeURL)
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to connect to eth space")
	}

	cfx, err := sdk.NewClient(cfxNodeURL)
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to connect to cfx space")
	}

	ethChainId, err := eth.Eth.ChainId()
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to get chain ID from eth space")
	}

	return &CfxAPI{
		eth:          eth.Eth,
		cfx:          cfx,
		ethNetworkId: uint32(*ethChainId),
	}, nil
}

func (api *CfxAPI) GasPrice(ctx context.Context) (*hexutil.Big, error) {
	return api.eth.GasPrice()
}

func (api *CfxAPI) EpochNumber(ctx context.Context, epoch *types.Epoch) (*hexutil.Big, error) {
	// By default, return lastest_state for eth space.
	if epoch == nil {
		epoch = types.EpochLatestState
	}

	return api.cfx.GetEpochNumber(epoch)
}

func (api *CfxAPI) GetBalance(ctx context.Context, address EthAddress, bn *EthBlockNumber) (*hexutil.Big, error) {
	return api.eth.Balance(address.value, bn.ValueOrNil())
}

func (api *CfxAPI) GetAdmin(ctx context.Context, contract EthAddress, bn *EthBlockNumber) (*string, error) {
	return nil, nil
}

func (api *CfxAPI) GetSponsorInfo(ctx context.Context, contract EthAddress, bn *EthBlockNumber) (types.SponsorInfo, error) {
	return types.SponsorInfo{
		SponsorForGas:               api.convertAddress(common.Address{}),
		SponsorForCollateral:        api.convertAddress(common.Address{}),
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
	return api.eth.CodeAt(contract.value, bn.ValueOrNil())
}

func (api *CfxAPI) GetStorageAt(ctx context.Context, address EthAddress, position *hexutil.Big, bn *EthBlockNumber) (common.Hash, error) {
	return api.eth.StorageAt(address.value, position, bn.ValueOrNil())
}

func (api *CfxAPI) GetStorageRoot(ctx context.Context, address EthAddress, bn *EthBlockNumber) (*types.StorageRoot, error) {
	return nil, nil
}

func (api *CfxAPI) GetBlockByHash(ctx context.Context, blockHash common.Hash, includeTxs bool) (interface{}, error) {
	block, err := api.eth.BlockByHash(blockHash, includeTxs)
	if err != nil {
		return nil, err
	}

	if includeTxs {
		return api.convertBlock(block), nil
	}

	return api.convertBlockSummary(block), nil
}

func (api *CfxAPI) GetBlockByHashWithPivotAssumption(ctx context.Context, blockHash, pivotHash common.Hash, bn hexutil.Uint64) (*types.Block, error) {
	// Note, there is no referee blocks in ETH space, and only pivot block available in an epoch.
	// So, client should only query pivot block with this method.
	if blockHash != pivotHash {
		return nil, ErrInvalidBlockAssumption
	}

	block, err := api.eth.BlockByHash(blockHash, true)
	if err != nil {
		return nil, err
	}

	if block == nil {
		return nil, ErrInvalidBlockAssumption
	}

	if block.Number.ToInt().Uint64() != uint64(bn) {
		return nil, ErrInvalidBlockAssumption
	}

	return api.convertBlock(block), nil
}

func (api *CfxAPI) GetBlockByEpochNumber(ctx context.Context, bn EthBlockNumber, includeTxs bool) (interface{}, error) {
	block, err := api.eth.BlockByNumber(bn.Value(), includeTxs)
	if err != nil {
		return nil, err
	}

	if includeTxs {
		return api.convertBlock(block), nil
	}

	return api.convertBlockSummary(block), nil
}

func (api *CfxAPI) GetBlockByBlockNumber(ctx context.Context, blockNumer hexutil.Uint64, includeTxs bool) (interface{}, error) {
	bn := EthBlockNumber{
		value: rpc.BlockNumber(blockNumer),
	}

	return api.GetBlockByEpochNumber(ctx, bn, includeTxs)
}

func (api *CfxAPI) GetBestBlockHash(ctx context.Context) (common.Hash, error) {
	block, err := api.eth.BlockByNumber(rpc.LatestBlockNumber, false)
	if err != nil {
		return common.Hash{}, err
	}

	if block == nil {
		return common.Hash{}, nil
	}

	return block.Hash, nil
}

func (api *CfxAPI) GetNextNonce(ctx context.Context, address EthAddress, bn *EthBlockNumber) (*hexutil.Big, error) {
	return api.eth.TransactionCount(address.value, bn.ValueOrNil())
}

func (api *CfxAPI) SendRawTransaction(ctx context.Context, signedTx hexutil.Bytes) (common.Hash, error) {
	return api.eth.SendRawTransaction(signedTx)
}

func (api *CfxAPI) Call(ctx context.Context, request EthCallRequest, bn *EthBlockNumber) (hexutil.Bytes, error) {
	return api.eth.Call(request.ToCallMsg(), bn.ValueOrNil())
}

func (api *CfxAPI) GetLogs(ctx context.Context, filter EthLogFilter) ([]types.Log, error) {
	logs, err := api.eth.Logs(filter.ToFilterQuery())
	if err != nil {
		return nil, err
	}

	result := make([]types.Log, len(logs))
	for i := range logs {
		result[i] = *api.convertLog(&logs[i])
	}

	return result, nil
}

func (api *CfxAPI) GetTransactionByHash(ctx context.Context, txHash common.Hash) (*types.Transaction, error) {
	tx, err := api.eth.TransactionByHash(txHash)
	if err != nil {
		return nil, err
	}

	var receipt *ethTypes.Receipt
	if tx.BlockHash != nil {
		if receipt, err = api.eth.TransactionReceipt(txHash); err != nil {
			return nil, err
		}
	}

	return api.convertTx(tx, receipt), nil
}

func (api *CfxAPI) EstimateGasAndCollateral(ctx context.Context, request EthCallRequest, bn *EthBlockNumber) (types.Estimate, error) {
	gasLimit, err := api.eth.EstimateGas(request.ToCallMsg(), bn.ValueOrNil())
	if err != nil {
		return types.Estimate{}, err
	}

	gasUsed := float64(gasLimit.ToInt().Uint64()) * 3.0 / 4.0

	return types.Estimate{
		GasLimit:              gasLimit,
		GasUsed:               types.NewBigInt(uint64(gasUsed)),
		StorageCollateralized: HexBig0,
	}, nil
}

func (api *CfxAPI) GetBlocksByEpoch(ctx context.Context, bn EthBlockNumber) ([]common.Hash, error) {
	block, err := api.eth.BlockByNumber(bn.Value(), false)
	if err != nil {
		return nil, err
	}

	if block == nil {
		return []common.Hash{}, nil
	}

	return []common.Hash{block.Hash}, nil
}

func (api *CfxAPI) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (*types.TransactionReceipt, error) {
	receipt, err := api.eth.TransactionReceipt(txHash)
	if err != nil {
		return nil, err
	}

	return api.convertReceipt(receipt), nil
}

func (api *CfxAPI) GetEpochReceipts(ctx context.Context, bnh EthBlockNumberOrHash) (receipts [][]*types.TransactionReceipt, err error) {
	// TODO wait for eth space to support parity_getBlockReceipts
	var block *ethTypes.Block

	if num, ok := bnh.Number(); ok {
		block, err = api.eth.BlockByNumber(num, false)
	} else if hash, ok := bnh.Hash(); ok {
		block, err = api.eth.BlockByHash(hash, false)
	}

	if err != nil {
		return nil, err
	}

	if block == nil || len(block.Transactions.Hashes()) == 0 {
		return [][]*types.TransactionReceipt{emptyReceiptList}, nil
	}

	var result []*types.TransactionReceipt
	for i := range block.Transactions.Hashes() {
		receipt, err := api.eth.TransactionReceipt(block.Transactions.Hashes()[i])
		if err != nil {
			return nil, err
		}

		result = append(result, api.convertReceipt(receipt))
	}

	return [][]*types.TransactionReceipt{result}, nil
}

func (api *CfxAPI) GetAccount(ctx context.Context, address EthAddress, bn *EthBlockNumber) (types.AccountInfo, error) {
	balance, err := api.eth.Balance(address.value, bn.ValueOrNil())
	if err != nil {
		return types.AccountInfo{}, err
	}

	nonce, err := api.eth.TransactionCount(address.value, bn.ValueOrNil())
	if err != nil {
		return types.AccountInfo{}, err
	}

	code, err := api.eth.CodeAt(address.value, bn.ValueOrNil())
	if err != nil {
		return types.AccountInfo{}, err
	}

	return types.AccountInfo{
		Balance:                   balance,
		Nonce:                     nonce,
		CodeHash:                  types.Hash(crypto.Keccak256Hash(code).Hex()),
		StakingBalance:            HexBig0,
		CollateralForStorage:      HexBig0,
		AccumulatedInterestReturn: HexBig0,
		Admin:                     api.convertAddress(common.Address{}),
	}, nil
}

func (api *CfxAPI) GetInterestRate(ctx context.Context, bn *EthBlockNumber) (*hexutil.Big, error) {
	return HexBig0, nil
}

func (api *CfxAPI) GetAccumulateInterestRate(ctx context.Context, bn *EthBlockNumber) (*hexutil.Big, error) {
	return HexBig0, nil
}

func (api *CfxAPI) GetConfirmationRiskByHash(ctx context.Context, blockHash types.Hash) (*hexutil.Big, error) {
	return api.cfx.GetRawBlockConfirmationRisk(blockHash)
}

func (api *CfxAPI) GetStatus(ctx context.Context) (types.Status, error) {
	chainId, err := api.eth.ChainId()
	if err != nil {
		return types.Status{}, err
	}

	block, err := api.eth.BlockByNumber(rpc.LatestBlockNumber, false)
	if err != nil {
		return types.Status{}, err
	}

	latestBlockNumber := hexutil.Uint64(block.Number.ToInt().Uint64())

	return types.Status{
		BestHash:         types.Hash(block.Hash.Hex()),
		ChainID:          *chainId,
		NetworkID:        *chainId, // eth space return chainId as networkId
		EpochNumber:      latestBlockNumber,
		BlockNumber:      latestBlockNumber,
		PendingTxNumber:  0,
		LatestCheckpoint: 0,
		LatestConfirmed:  0,
		LatestState:      latestBlockNumber,
	}, nil
}

func (api *CfxAPI) GetBlockRewardInfo(ctx context.Context, epoch types.Epoch) ([]types.RewardInfo, error) {
	return api.cfx.GetBlockRewardInfo(epoch)
}

func (api *CfxAPI) ClientVersion(ctx context.Context) (string, error) {
	return api.eth.ClientVersion()
}
