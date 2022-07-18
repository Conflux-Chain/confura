package ethbridge

import (
	"math/big"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
)

var (
	txnStatusFailed  = uint64(0) // Ethereum tx status `failed`
	txnStatusSuccess = uint64(1) // Ethereum tx status `success`
)

// convert cfx address => eth address
func ConvertAddress(value cfxtypes.Address) (common.Address, uint32) {
	addr, ethNetworkId, _ := value.ToCommon()
	return addr, ethNetworkId
}

func ConvertAddressNullable(value *cfxtypes.Address) (*common.Address, uint32) {
	if value == nil {
		return nil, 0
	}

	addr, ethNetworkId, _ := value.ToCommon()
	return &addr, ethNetworkId
}

// convert cfx hash => eth hash
func ConvertHash(value cfxtypes.Hash) common.Hash {
	return common.HexToHash(string(value))
}

func ConvertHashNullable(value *cfxtypes.Hash) common.Hash {
	if value == nil {
		return common.Hash{}
	}

	hash := common.HexToHash(string(*value))
	return hash
}

// convert cfx tx => eth tx
func ConvertTx(tx *cfxtypes.Transaction, txExt *store.TransactionExtra) *types.Transaction {
	if tx == nil {
		return nil
	}

	from, chainId := ConvertAddress(tx.From)
	creates, _ := ConvertAddressNullable(tx.ContractCreated)
	to, _ := ConvertAddressNullable(tx.To)
	input, _ := hexutil.Decode(tx.Data)

	var gas, nonce uint64
	if tx.Gas != nil {
		gas = tx.Gas.ToInt().Uint64()
	}

	if tx.Nonce != nil {
		nonce = tx.Nonce.ToInt().Uint64()
	}

	ethTxn := &types.Transaction{
		BlockHash:        tx.BlockHash.ToCommonHash(),
		ChainID:          big.NewInt(int64(chainId)),
		Creates:          creates,
		From:             from,
		Gas:              gas,
		GasPrice:         tx.GasPrice.ToInt(),
		Hash:             ConvertHash(tx.Hash),
		Input:            input,
		Nonce:            nonce,
		R:                tx.R.ToInt(),
		S:                tx.S.ToInt(),
		Status:           ConvertTxStatus(tx.Status),
		To:               to,
		TransactionIndex: (*uint64)(tx.TransactionIndex),
		V:                tx.V.ToInt(),
		Value:            tx.Value.ToInt(),
	}

	if !util.IsEip155Tx(ethTxn) { // only return chainID for EIP155 tx
		tx.ChainID = nil
	}

	// fill missed data field `Accesses`, `BlockNumber`, `MaxFeePerGas`, `MaxPriorityFeePerGas`, `type`, `StandardV`
	if txExt != nil {
		ethTxn.Accesses = txExt.Accesses
		ethTxn.BlockNumber = txExt.BlockNumber.ToInt()
		ethTxn.MaxFeePerGas = txExt.MaxFeePerGas.ToInt()
		ethTxn.MaxPriorityFeePerGas = txExt.MaxPriorityFeePerGas.ToInt()
		ethTxn.Type = txExt.Type
		ethTxn.StandardV = txExt.StandardV.ToInt()
	}

	return ethTxn
}

func ConvertTxStatus(value *hexutil.Uint64) (status *uint64) {
	if value == nil {
		return nil
	}

	switch {
	case *value == 0: // cfx txn status `success`
		status = &txnStatusSuccess
	case *value == 1: // cfx txn status `failed`
		status = &txnStatusFailed
	default:
		logrus.WithField("txStatus", *value).Error(
			"Failed to convert unexpected tx status to eth tx status",
		)
	}

	return
}

// convert cfx block header => eth block
func ConvertBlockHeader(value *cfxtypes.BlockHeader, blockExt *store.BlockExtra) *types.Block {
	if value == nil {
		return nil
	}

	extraData := []byte{}
	if len(value.Custom) > 0 {
		extraData = value.Custom[0]
	}

	var nonce *gethTypes.BlockNonce
	if value.Nonce != nil {
		v := (gethTypes.EncodeNonce(value.Nonce.ToInt().Uint64()))
		nonce = &v
	}

	logsBloomBytes, _ := hexutil.Decode(string(value.DeferredLogsBloomHash))
	minerAddr, _ := ConvertAddress(value.Miner)

	uncleHashes := make([]common.Hash, len(value.RefereeHashes))
	for i, refh := range value.RefereeHashes {
		uncleHashes[i] = ConvertHash(refh)
	}

	ethBlock := &types.Block{
		Author:           &minerAddr,
		Difficulty:       (*big.Int)(value.Difficulty),
		ExtraData:        extraData,
		GasLimit:         value.GasLimit.ToInt().Uint64(),
		GasUsed:          value.GasUsed.ToInt().Uint64(),
		Hash:             *value.Hash.ToCommonHash(),
		LogsBloom:        gethTypes.BytesToBloom(logsBloomBytes),
		Miner:            minerAddr,
		Nonce:            nonce,
		Number:           value.BlockNumber.ToInt(),
		ParentHash:       ConvertHash(value.ParentHash),
		ReceiptsRoot:     ConvertHash(value.DeferredReceiptsRoot),
		Size:             value.Size.ToInt().Uint64(),
		StateRoot:        ConvertHash(value.DeferredStateRoot),
		Timestamp:        value.Timestamp.ToInt().Uint64(),
		TransactionsRoot: ConvertHash(value.TransactionsRoot),
		Uncles:           uncleHashes,
	}

	// fill missed data fields `BaseFeePerGas`, `MixHash`, `TotalDifficulty`, `Sha3Uncles`
	if blockExt != nil {
		ethBlock.BaseFeePerGas = blockExt.BaseFeePerGas.ToInt()
		ethBlock.MixHash = blockExt.MixHash
		ethBlock.TotalDifficulty = blockExt.TotalDifficulty.ToInt()

		if blockExt.Sha3Uncles != nil {
			ethBlock.Sha3Uncles = *blockExt.Sha3Uncles
		}
	}

	return ethBlock
}

// convert cfx block summary => eth block
func ConvertBlockSummary(value *cfxtypes.BlockSummary, blockExt *store.BlockExtra) *types.Block {
	block := ConvertBlockHeader(&value.BlockHeader, blockExt)

	txHashes := make([]common.Hash, len(value.Transactions))
	for i, txh := range value.Transactions {
		txHashes[i] = ConvertHash(txh)
	}
	block.Transactions = *types.NewTxOrHashListByHashes(txHashes)

	return block
}

// convert cfx block => eth block
func ConvertBlock(value *cfxtypes.Block, blockExt *store.BlockExtra) *types.Block {
	block := ConvertBlockHeader(&value.BlockHeader, blockExt)

	txs := make([]types.Transaction, len(value.Transactions))
	for i, tx := range value.Transactions {
		var txnExt *store.TransactionExtra
		if i < len(blockExt.TxnExts) {
			txnExt = blockExt.TxnExts[i]
		}

		txs[i] = *ConvertTx(&tx, txnExt)
	}
	block.Transactions = *types.NewTxOrHashListByTxs(txs)

	return block
}

// convert cfx receipt => eth receipt
func ConvertReceipt(value *cfxtypes.TransactionReceipt, rcptExtra *store.ReceiptExtra) *types.Receipt {
	if value == nil {
		return nil
	}

	logs := make([]*types.Log, len(value.Logs))
	for i := range value.Logs {
		var logExt *store.LogExtra
		if rcptExtra != nil && i < len(rcptExtra.LogExts) {
			logExt = rcptExtra.LogExts[i]
		}

		logs[i] = ConvertLog(&value.Logs[i], logExt)
	}

	from, _ := ConvertAddress(value.From)
	to, _ := ConvertAddressNullable(value.To)
	contractAddr, _ := ConvertAddressNullable(value.ContractCreated)

	logsBloomBytes, _ := hexutil.Decode(string(value.LogsBloom))
	logsBloom := gethTypes.BytesToBloom(logsBloomBytes)

	var root []byte
	if len(value.StateRoot) > 0 && value.StateRoot != cfxtypes.Hash(common.Hash{}.Hex()) {
		root, _ = hexutil.Decode(string(value.StateRoot))
	}

	receipt := &types.Receipt{
		BlockHash:        ConvertHash(value.BlockHash),
		BlockNumber:      uint64(*value.EpochNumber),
		ContractAddress:  contractAddr,
		From:             from,
		GasUsed:          value.GasUsed.ToInt().Uint64(),
		Logs:             logs,
		LogsBloom:        logsBloom,
		Root:             root,
		Status:           ConvertTxStatus(&value.OutcomeStatus),
		To:               to,
		TransactionHash:  ConvertHash(value.TransactionHash),
		TransactionIndex: uint64(value.Index),
		TxExecErrorMsg:   value.TxExecErrorMsg,
	}

	// fill missed data field `CumulativeGasUsed`, `EffectiveGasPrice`, `Type`
	if rcptExtra != nil {
		if rcptExtra.CumulativeGasUsed != nil {
			receipt.CumulativeGasUsed = *rcptExtra.CumulativeGasUsed
		}

		if rcptExtra.EffectiveGasPrice != nil {
			receipt.EffectiveGasPrice = *rcptExtra.EffectiveGasPrice
		}

		receipt.Type = rcptExtra.Type
	}

	return receipt
}

// convert cfx log => eth log
func ConvertLog(log *cfxtypes.Log, logExtra *store.LogExtra) *types.Log {
	if log == nil {
		return nil
	}

	ethAddr, _ := ConvertAddress(log.Address)
	topics := make([]common.Hash, len(log.Topics))
	for i := range log.Topics {
		topics[i] = ConvertHash(log.Topics[i])
	}

	var txnLogIndex *uint
	if log.TransactionLogIndex != nil {
		v := uint(log.TransactionLogIndex.ToInt().Uint64())
		txnLogIndex = &v
	}

	ethLog := &types.Log{
		Address:             ethAddr,
		BlockHash:           ConvertHashNullable(log.BlockHash),
		BlockNumber:         log.EpochNumber.ToInt().Uint64(),
		Data:                log.Data,
		Index:               uint(log.LogIndex.ToInt().Int64()),
		Topics:              topics,
		TxHash:              ConvertHashNullable(log.TransactionHash),
		TxIndex:             uint(log.TransactionIndex.ToInt().Uint64()),
		TransactionLogIndex: txnLogIndex,
	}

	// fill missed data field `LogType`, `Removed`
	if logExtra != nil {
		ethLog.LogType = logExtra.LogType

		if logExtra.Removed != nil {
			ethLog.Removed = *logExtra.Removed
		}
	}

	return ethLog
}
