package store

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
	web3Types "github.com/openweb3/web3go/types"
)

// custom block fields for extention
type BlockExtra struct {
	// extended fields for ETH block
	BaseFeePerGas   *hexutil.Big `json:"baseFeePerGas,omitempty"`
	MixHash         *common.Hash `json:"mixHash,omitempty"`
	TotalDifficulty *hexutil.Big `json:"totalDifficulty,omitempty"`
	Sha3Uncles      *common.Hash `json:"sha3Uncles,omitempty"`

	TxnExts []*TransactionExtra `json:"-"`
}

// custom transaction fields for extention
type TransactionExtra struct {
	// extended fields for ETH transaction
	Accesses             gethTypes.AccessList `json:"accessList,omitempty"`
	BlockNumber          *hexutil.Big         `json:"blockNumber,omitempty"`
	MaxFeePerGas         *hexutil.Big         `json:"maxFeePerGas,omitempty"`
	MaxPriorityFeePerGas *hexutil.Big         `json:"maxPriorityFeePerGas,omitempty"`
	Type                 *uint64              `json:"type,omitempty"`
}

// custom receipt fields for extention
type ReceiptExtra struct {
	// extended fields for ETH receipt
	CumulativeGasUsed *uint64 `json:"cumulativeGasUsed,omitempty"`
	EffectiveGasPrice *uint64 `json:"effectiveGasPrice,omitempty"`
	Type              *uint   `json:"type,omitempty"`

	LogExts []*LogExtra `json:"-"`
}

// custom event log fields for extention
type LogExtra struct {
	// extended fields for ETH log
	LogType *string `json:"logType,omitempty"`
	Removed *bool   `json:"removed,,omitempty"`
}

func ExtractEthBlockExt(ethBlock *web3Types.Block) *BlockExtra {
	ethTxns := ethBlock.Transactions.Transactions()
	txnExts := make([]*TransactionExtra, len(ethTxns))

	for i := 0; i < len(ethTxns); i++ {
		txnExts[i] = ExtractEthTransactionExt(&ethTxns[i])
	}

	return &BlockExtra{
		BaseFeePerGas:   (*hexutil.Big)(ethBlock.BaseFeePerGas),
		MixHash:         ethBlock.MixHash,
		TotalDifficulty: (*hexutil.Big)(ethBlock.TotalDifficulty),
		Sha3Uncles:      &ethBlock.Sha3Uncles,
		TxnExts:         txnExts,
	}
}

func ExtractEthTransactionExt(ethTxn *web3Types.Transaction) *TransactionExtra {
	return &TransactionExtra{
		Accesses:             ethTxn.Accesses,
		BlockNumber:          (*hexutil.Big)(ethTxn.BlockNumber),
		MaxFeePerGas:         (*hexutil.Big)(ethTxn.MaxFeePerGas),
		MaxPriorityFeePerGas: (*hexutil.Big)(ethTxn.MaxPriorityFeePerGas),
		Type:                 &ethTxn.Type,
	}
}

func ExtractEthLogExt(ethLog *web3Types.Log) *LogExtra {
	return &LogExtra{
		LogType: ethLog.LogType, Removed: &ethLog.Removed,
	}
}

func ExtractEthReceiptExt(ethRcpt *web3Types.Receipt) *ReceiptExtra {
	ethLogs := ethRcpt.Logs
	logExts := make([]*LogExtra, len(ethLogs))

	for i := 0; i < len(ethLogs); i++ {
		logExts[i] = ExtractEthLogExt(ethLogs[i])
	}

	return &ReceiptExtra{
		CumulativeGasUsed: &ethRcpt.CumulativeGasUsed,
		EffectiveGasPrice: &ethRcpt.EffectiveGasPrice,
		Type:              &ethRcpt.Type,
		LogExts:           logExts,
	}
}
