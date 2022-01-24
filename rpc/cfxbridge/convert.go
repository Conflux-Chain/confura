package cfxbridge

import (
	"math/big"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
)

func (api *CfxAPI) normalizeBig(value *big.Int, err error) (*hexutil.Big, error) {
	if err != nil {
		return nil, err
	}

	return types.NewBigIntByRaw(value), nil
}

func (api *CfxAPI) convertHashNullable(value *common.Hash) *types.Hash {
	if value == nil {
		return nil
	}

	hash := types.Hash(value.Hex())
	return &hash
}

func (api *CfxAPI) convertAddressNullable(value *common.Address) *types.Address {
	if value == nil {
		return nil
	}

	address, _ := cfxaddress.NewFromCommon(*value, api.ethNetworkId)
	return &address
}

func (api *CfxAPI) convertAddress(value common.Address) types.Address {
	address, _ := cfxaddress.NewFromCommon(value, api.ethNetworkId)
	return address
}

func (api *CfxAPI) convertTxStatus(value *uint) *hexutil.Uint64 {
	if value == nil {
		return nil
	}

	var status hexutil.Uint64

	if *value == 0 {
		status = 1
	} else if *value != 1 {
		logrus.WithField("ethTxStatus", *value).Error("Unexpected tx status from eth space")
	}

	return &status
}

func (api *CfxAPI) convertTx(tx *ethTypes.Transaction) *types.Transaction {
	if tx == nil {
		return nil
	}

	chainId := HexBig0
	if tx.ChainID != nil {
		chainId = types.NewBigIntByRaw(tx.ChainID)
	}

	return &types.Transaction{
		Hash:             types.Hash(tx.Hash.Hex()),
		Nonce:            types.NewBigInt(tx.Nonce),
		BlockHash:        api.convertHashNullable(tx.BlockHash),
		TransactionIndex: (*hexutil.Uint64)(tx.TransactionIndex),
		From:             api.convertAddress(tx.From),
		To:               api.convertAddressNullable(tx.To),
		Value:            types.NewBigIntByRaw(tx.Value),
		GasPrice:         types.NewBigIntByRaw(tx.GasPrice),
		Gas:              types.NewBigInt(tx.Gas),
		ContractCreated:  api.convertAddressNullable(tx.Creates),
		Data:             hexutil.Encode(tx.Input),
		StorageLimit:     HexBig0,
		EpochHeight:      HexBig0,
		ChainID:          chainId,
		Status:           api.convertTxStatus(tx.Status),
		V:                types.NewBigIntByRaw(tx.V),
		R:                types.NewBigIntByRaw(tx.R),
		S:                types.NewBigIntByRaw(tx.S),
	}
}

func (api *CfxAPI) convertBlockHeader(block *ethTypes.Block) *types.BlockHeader {
	if block == nil {
		return nil
	}

	referees := make([]types.Hash, len(block.Uncles))
	for i := range block.Uncles {
		referees[i] = types.Hash(block.Uncles[i].Hex())
	}

	extraData := block.ExtraData
	if extraData == nil {
		extraData = []byte{}
	}

	return &types.BlockHeader{
		Hash:                  types.Hash(block.Hash.Hex()),
		ParentHash:            types.Hash(block.ParentHash.Hex()),
		Height:                types.NewBigIntByRaw(block.Number),
		Miner:                 api.convertAddress(block.Miner),
		DeferredStateRoot:     types.Hash(block.StateRoot.Hex()),
		DeferredReceiptsRoot:  types.Hash(block.ReceiptsRoot.Hex()),
		DeferredLogsBloomHash: types.Hash(hexutil.Encode(block.LogsBloom.Bytes())),
		Blame:                 0,
		TransactionsRoot:      types.Hash(block.TransactionsRoot.Hex()),
		EpochNumber:           types.NewBigIntByRaw(block.Number),
		BlockNumber:           types.NewBigIntByRaw(block.Number),
		GasLimit:              types.NewBigInt(block.GasLimit),
		GasUsed:               types.NewBigInt(block.GasUsed),
		Timestamp:             types.NewBigInt(block.Timestamp),
		Difficulty:            types.NewBigIntByRaw(block.Difficulty),
		PowQuality:            HexBig0,
		RefereeHashes:         referees,
		Adaptive:              false,
		Nonce:                 types.NewBigInt(block.Nonce.Uint64()),
		Size:                  types.NewBigInt(block.Size),
		Custom:                [][]byte{extraData},
	}
}

func (api *CfxAPI) convertBlock(block *ethTypes.Block) *types.Block {
	if block == nil {
		return nil
	}

	blockTxs := block.Transactions.Transactions()

	txs := make([]types.Transaction, len(blockTxs))
	for i := range blockTxs {
		txs[i] = *api.convertTx(&blockTxs[i])
	}

	return &types.Block{
		BlockHeader:  *api.convertBlockHeader(block),
		Transactions: txs,
	}
}

func (api *CfxAPI) convertBlockSummary(block *ethTypes.Block) *types.BlockSummary {
	if block == nil {
		return nil
	}

	blockTxs := block.Transactions.Hashes()

	txs := make([]types.Hash, len(blockTxs))
	for i := range blockTxs {
		txs[i] = types.Hash(blockTxs[i].Hex())
	}

	return &types.BlockSummary{
		BlockHeader:  *api.convertBlockHeader(block),
		Transactions: txs,
	}
}

func (api *CfxAPI) convertLog(log *ethTypes.Log) *types.Log {
	if log == nil {
		return nil
	}

	topics := make([]types.Hash, len(log.Topics))
	for i := range log.Topics {
		topics[i] = types.Hash(log.Topics[i].Hex())
	}

	return &types.Log{
		Address:             api.convertAddress(log.Address),
		Topics:              topics,
		Data:                log.Data,
		BlockHash:           api.convertHashNullable(&log.BlockHash),
		EpochNumber:         types.NewBigInt(log.BlockNumber),
		TransactionHash:     api.convertHashNullable(&log.TxHash),
		TransactionIndex:    types.NewBigInt(uint64(log.TxIndex)),              // tx index in block
		LogIndex:            types.NewBigInt(uint64(log.Index)),                // log index in block
		TransactionLogIndex: types.NewBigInt(uint64(*log.TransactionLogIndex)), // log index in tx
	}
}

func (api *CfxAPI) convertReceipt(receipt *ethTypes.Receipt) *types.TransactionReceipt {
	if receipt == nil {
		return nil
	}

	logs := make([]types.Log, len(receipt.Logs))
	for i := range receipt.Logs {
		logs[i] = *api.convertLog(receipt.Logs[i])
	}

	var stateRoot types.Hash
	if len(receipt.Root) == 0 {
		stateRoot = types.Hash(common.Hash{}.Hex())
	} else {
		stateRoot = types.Hash(hexutil.Encode(receipt.Root))
	}

	gasFee := new(big.Int).Mul(
		new(big.Int).SetUint64(receipt.EffectiveGasPrice),
		new(big.Int).SetUint64(receipt.GasUsed),
	)

	return &types.TransactionReceipt{
		TransactionHash:         types.Hash(receipt.TransactionHash.Hex()),
		Index:                   hexutil.Uint64(receipt.TransactionIndex),
		BlockHash:               types.Hash(receipt.BlockHash.Hex()),
		EpochNumber:             types.NewUint64(receipt.BlockNumber),
		From:                    api.convertAddress(receipt.From),
		To:                      api.convertAddressNullable(receipt.To),
		GasUsed:                 types.NewBigInt(receipt.GasUsed),
		GasFee:                  types.NewBigIntByRaw(gasFee),
		ContractCreated:         api.convertAddressNullable(receipt.ContractAddress),
		Logs:                    logs,
		LogsBloom:               types.Bloom(hexutil.Encode(receipt.LogsBloom.Bytes())),
		StateRoot:               stateRoot,
		OutcomeStatus:           *api.convertTxStatus(&receipt.Status),
		TxExecErrorMsg:          receipt.TxExecErrorMsg,
		GasCoveredBySponsor:     false,
		StorageCoveredBySponsor: false,
		StorageCollateralized:   0,
		StorageReleased:         emptyStorageChangeList,
	}
}
