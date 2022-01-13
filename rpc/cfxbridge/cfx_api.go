package cfxbridge

import (
	"context"
	"math/big"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/pkg/errors"
)

var (
	emptyDepositList = []types.DepositInfo{}
	emptyVoteList    = []types.VoteStakeInfo{}
)

type CfxAPI struct {
	eth       *ethclient.Client
	cfx       *sdk.Client
	networkId uint32
}

func NewCfxAPI(nodeURL string) (*CfxAPI, error) {
	eth, err := ethclient.Dial(nodeURL)
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to connect to eth space")
	}

	cfx, err := sdk.NewClient(nodeURL)
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to connect to cfx space")
	}

	status, err := cfx.GetStatus()
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to get status of full node")
	}

	return &CfxAPI{eth, cfx, uint32(status.NetworkID)}, nil
}

func (api *CfxAPI) GasPrice(ctx context.Context) (*hexutil.Big, error) {
	return api.normalizeBig(api.eth.SuggestGasPrice(ctx))
}

func (api *CfxAPI) EpochNumber(ctx context.Context, bn *EthBlockNumber) (*hexutil.Big, error) {
	return api.normalizeUint64(api.eth.BlockNumber(ctx))
}

func (api *CfxAPI) GetBalance(ctx context.Context, address EthAddress, bn *EthBlockNumber) (*hexutil.Big, error) {
	return api.normalizeBig(api.eth.BalanceAt(ctx, address.value, bn.value))
}

func (api *CfxAPI) GetAdmin(ctx context.Context, contract EthAddress, bn *EthBlockNumber) (*string, error) {
	return nil, nil
}

func (api *CfxAPI) GetSponsorInfo(ctx context.Context, contract EthAddress, bn *EthBlockNumber) (types.SponsorInfo, error) {
	return types.SponsorInfo{}, nil
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
	return api.eth.CodeAt(ctx, contract.value, bn.value)
}

func (api *CfxAPI) GetStorageAt(ctx context.Context, address EthAddress, key common.Hash, bn *EthBlockNumber) (hexutil.Bytes, error) {
	return api.eth.StorageAt(ctx, address.value, key, bn.value)
}

func (api *CfxAPI) GetStorageRoot(ctx context.Context, address EthAddress, bn *EthBlockNumber) (*types.StorageRoot, error) {
	return nil, nil
}

func (api *CfxAPI) GetBlockByHash(ctx context.Context, blockHash common.Hash, includeTxs bool) (interface{}, error) {
	// always query block for uncles
	block, err := api.eth.BlockByHash(ctx, blockHash)

	if includeTxs {
		return api.normalizeBlock(block, err)
	}

	return api.normalizeBlockSummary(block, err)
}

func (api *CfxAPI) GetBlockByHashWithPivotAssumption(ctx context.Context, blockHash, pivotHash common.Hash, bn hexutil.Uint64) (types.Block, error) {
	// Note, there is no referee blocks in ETH space, and only pivot block available in an epoch.
	// So, client should only query pivot block with this method.
	if blockHash != pivotHash {
		return types.Block{}, ErrInvalidBlockAssumption
	}

	block, err := api.eth.BlockByHash(ctx, blockHash)
	if err != nil {
		return types.Block{}, err
	}

	if block.NumberU64() != uint64(bn) {
		return types.Block{}, ErrInvalidBlockAssumption
	}

	result, _ := api.normalizeBlock(block, err)

	return *result, nil
}

func (api *CfxAPI) GetBlockByEpochNumber(ctx context.Context, bn EthBlockNumber, includeTxs bool) (interface{}, error) {
	// always query block for uncles
	block, err := api.eth.BlockByNumber(ctx, bn.value)

	if includeTxs {
		return api.normalizeBlock(block, err)
	}

	return api.normalizeBlockSummary(block, err)
}

func (api *CfxAPI) GetBlockByBlockNumber(ctx context.Context, blockNumer hexutil.Uint64, includeTxs bool) (block interface{}, err error) {
	bn := EthBlockNumber{
		value: new(big.Int).SetUint64(uint64(blockNumer)),
	}

	return api.GetBlockByEpochNumber(ctx, bn, includeTxs)
}

func (api *CfxAPI) GetBestBlockHash(ctx context.Context) (common.Hash, error) {
	block, err := api.eth.HeaderByNumber(ctx, nil)
	if err != nil {
		return common.Hash{}, err
	}

	return block.Hash(), nil
}

func (api *CfxAPI) GetNextNonce(ctx context.Context, address EthAddress, bn *EthBlockNumber) (*hexutil.Big, error) {
	return api.normalizeUint64(api.eth.NonceAt(ctx, address.value, bn.value))
}

func (api *CfxAPI) SendRawTransaction(ctx context.Context, signedTx hexutil.Bytes) (types.Hash, error) {
	var txHash types.Hash
	err := api.cfx.CallRPC(&txHash, "eth_sendRawTransaction", signedTx.String())
	return txHash, err
}

func (api *CfxAPI) Call(ctx context.Context, request EthCallRequest, bn *EthBlockNumber) (hexutil.Bytes, error) {
	return api.eth.CallContract(ctx, request.ToCallMsg(), bn.value)
}

func (api *CfxAPI) GetLogs(ctx context.Context, filter EthLogFilter) ([]types.Log, error) {
	query, err := filter.ToFilterQuery()
	if err != nil {
		return nil, err
	}

	logs, err := api.eth.FilterLogs(ctx, *query)
	if err != nil {
		return nil, err
	}

	var result []types.Log
	for i := range logs {
		result = append(result, *api.convertLog(&logs[i]))
	}

	return result, nil
}

func (api *CfxAPI) GetTransactionByHash(ctx context.Context, txHash common.Hash) (*types.Transaction, error) {
	tx, _, err := api.eth.TransactionByHash(ctx, txHash)
	if err != nil {
		return nil, err
	}

	return api.convertTx(tx), nil
}

func (api *CfxAPI) EstimateGasAndCollateral(ctx context.Context, request EthCallRequest, bn *EthBlockNumber) (types.Estimate, error) {
	if bn.value != nil {
		return types.Estimate{}, ErrEpochUnsupported
	}

	gasLimit, err := api.eth.EstimateGas(ctx, request.ToCallMsg())
	if err != nil {
		return types.Estimate{}, err
	}

	gasUsed := uint64(float64(gasLimit*3) / 4.0)

	return types.Estimate{
		GasLimit:              types.NewBigInt(gasLimit),
		GasUsed:               types.NewBigInt(gasUsed),
		StorageCollateralized: HexBig0,
	}, nil
}

func (api *CfxAPI) GetBlocksByEpoch(ctx context.Context, bn *EthBlockNumber) ([]common.Hash, error) {
	header, err := api.eth.HeaderByNumber(ctx, bn.value)
	if err != nil {
		return nil, err
	}

	return []common.Hash{header.Hash()}, nil
}

func (api *CfxAPI) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (*types.TransactionReceipt, error) {
	receipt, err := api.eth.TransactionReceipt(ctx, txHash)
	if err != nil {
		return nil, err
	}

	return api.convertReceipt(receipt), nil
}

func (api *CfxAPI) GetEpochReceipts(ctx context.Context, epoch types.Epoch) (receipts [][]types.TransactionReceipt, err error) {
	// TODO wait for eth space to support parity_getBlockReceipts
	return nil, nil
}

func (api *CfxAPI) GetAccount(ctx context.Context, address EthAddress, bn *EthBlockNumber) (types.AccountInfo, error) {
	balance, err := api.eth.BalanceAt(ctx, address.value, bn.value)
	if err != nil {
		return types.AccountInfo{}, err
	}

	nonce, err := api.eth.NonceAt(ctx, address.value, bn.value)
	if err != nil {
		return types.AccountInfo{}, err
	}

	code, err := api.eth.CodeAt(ctx, address.value, bn.value)
	if err != nil {
		return types.AccountInfo{}, err
	}

	return types.AccountInfo{
		Balance:                   types.NewBigIntByRaw(balance),
		Nonce:                     types.NewBigInt(nonce),
		CodeHash:                  types.Hash(crypto.Keccak256Hash(code).Hex()),
		StakingBalance:            HexBig0,
		CollateralForStorage:      HexBig0,
		AccumulatedInterestReturn: HexBig0,
		Admin:                     cfxaddress.MustNewFromCommon(common.Address{}, api.networkId),
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
	chainId, err := api.eth.ChainID(ctx)
	if err != nil {
		return types.Status{}, err
	}

	networkId, err := api.eth.NetworkID(ctx)
	if err != nil {
		return types.Status{}, err
	}

	bn, err := api.eth.BlockNumber(ctx)
	if err != nil {
		return types.Status{}, err
	}

	return types.Status{
		BestHash:         types.Hash(common.Hash{}.Hex()),
		ChainID:          hexutil.Uint64(chainId.Uint64()),
		NetworkID:        hexutil.Uint64(networkId.Uint64()),
		EpochNumber:      hexutil.Uint64(bn),
		BlockNumber:      hexutil.Uint64(bn),
		PendingTxNumber:  0,
		LatestCheckpoint: 0,
		LatestConfirmed:  0,
		LatestState:      hexutil.Uint64(bn),
	}, nil
}

func (api *CfxAPI) GetBlockRewardInfo(ctx context.Context, epoch types.Epoch) ([]types.RewardInfo, error) {
	return api.cfx.GetBlockRewardInfo(epoch)
}

func (api *CfxAPI) ClientVersion(ctx context.Context) (string, error) {
	return api.cfx.GetClientVersion()
}
