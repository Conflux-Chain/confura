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

func NormalizeBig(value *big.Int, err error) (*hexutil.Big, error) {
	if err != nil {
		return nil, err
	}

	return types.NewBigIntByRaw(value), nil
}

func ConvertHashNullable(value *common.Hash) *types.Hash {
	if value == nil {
		return nil
	}

	hash := types.Hash(value.Hex())
	return &hash
}

func ConvertAddressNullable(value *common.Address, ethNetworkId uint32) *types.Address {
	if value == nil {
		return nil
	}

	address, _ := cfxaddress.NewFromCommon(*value, ethNetworkId)
	return &address
}

func ConvertAddress(value common.Address, ethNetworkId uint32) types.Address {
	address, _ := cfxaddress.NewFromCommon(value, ethNetworkId)
	return address
}

func ConvertTxStatus(value *uint64) *hexutil.Uint64 {
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

func ConvertTx(tx *ethTypes.Transaction, ethNetworkId uint32) *types.Transaction {
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
		BlockHash:        ConvertHashNullable(tx.BlockHash),
		TransactionIndex: (*hexutil.Uint64)(tx.TransactionIndex),
		From:             ConvertAddress(tx.From, ethNetworkId),
		To:               ConvertAddressNullable(tx.To, ethNetworkId),
		Value:            types.NewBigIntByRaw(tx.Value),
		GasPrice:         types.NewBigIntByRaw(tx.GasPrice),
		Gas:              types.NewBigInt(tx.Gas),
		ContractCreated:  ConvertAddressNullable(tx.Creates, ethNetworkId),
		Data:             hexutil.Encode(tx.Input),
		StorageLimit:     HexBig0,
		EpochHeight:      HexBig0,
		ChainID:          chainId,
		Status:           ConvertTxStatus(tx.Status),
		V:                types.NewBigIntByRaw(tx.V),
		R:                types.NewBigIntByRaw(tx.R),
		S:                types.NewBigIntByRaw(tx.S),
	}
}

func ConvertBlockHeader(block *ethTypes.Block, ethNetworkId uint32) *types.BlockHeader {
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
		Miner:                 ConvertAddress(block.Miner, ethNetworkId),
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

func ConvertBlock(block *ethTypes.Block, ethNetworkId uint32) *types.Block {
	if block == nil {
		return nil
	}

	blockTxs := block.Transactions.Transactions()

	txs := make([]types.Transaction, len(blockTxs))
	for i := range blockTxs {
		txs[i] = *ConvertTx(&blockTxs[i], ethNetworkId)
	}

	return &types.Block{
		BlockHeader:  *ConvertBlockHeader(block, ethNetworkId),
		Transactions: txs,
	}
}

func ConvertBlockSummary(block *ethTypes.Block, ethNetworkId uint32) *types.BlockSummary {
	if block == nil {
		return nil
	}

	blockTxs := block.Transactions.Hashes()

	txs := make([]types.Hash, len(blockTxs))
	for i := range blockTxs {
		txs[i] = types.Hash(blockTxs[i].Hex())
	}

	return &types.BlockSummary{
		BlockHeader:  *ConvertBlockHeader(block, ethNetworkId),
		Transactions: txs,
	}
}

func ConvertLog(log *ethTypes.Log, ethNetworkId uint32) *types.Log {
	if log == nil {
		return nil
	}

	topics := make([]types.Hash, len(log.Topics))
	for i := range log.Topics {
		topics[i] = types.Hash(log.Topics[i].Hex())
	}

	return &types.Log{
		Address:             ConvertAddress(log.Address, ethNetworkId),
		Topics:              topics,
		Data:                log.Data,
		BlockHash:           ConvertHashNullable(&log.BlockHash),
		EpochNumber:         types.NewBigInt(log.BlockNumber),
		TransactionHash:     ConvertHashNullable(&log.TxHash),
		TransactionIndex:    types.NewBigInt(uint64(log.TxIndex)),              // tx index in block
		LogIndex:            types.NewBigInt(uint64(log.Index)),                // log index in block
		TransactionLogIndex: types.NewBigInt(uint64(*log.TransactionLogIndex)), // log index in tx
	}
}

func ConvertReceipt(receipt *ethTypes.Receipt, ethNetworkId uint32) *types.TransactionReceipt {
	if receipt == nil {
		return nil
	}

	logs := make([]types.Log, len(receipt.Logs))
	for i := range receipt.Logs {
		logs[i] = *ConvertLog(receipt.Logs[i], ethNetworkId)
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
		From:                    ConvertAddress(receipt.From, ethNetworkId),
		To:                      ConvertAddressNullable(receipt.To, ethNetworkId),
		GasUsed:                 types.NewBigInt(receipt.GasUsed),
		GasFee:                  types.NewBigIntByRaw(gasFee),
		ContractCreated:         ConvertAddressNullable(receipt.ContractAddress, ethNetworkId),
		Logs:                    logs,
		LogsBloom:               types.Bloom(hexutil.Encode(receipt.LogsBloom.Bytes())),
		StateRoot:               stateRoot,
		OutcomeStatus:           *ConvertTxStatus(&receipt.Status),
		TxExecErrorMsg:          receipt.TxExecErrorMsg,
		GasCoveredBySponsor:     false,
		StorageCoveredBySponsor: false,
		StorageCollateralized:   0,
		StorageReleased:         emptyStorageChangeList,
	}
}
