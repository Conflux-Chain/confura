package store

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	web3Types "github.com/openweb3/web3go/types"
)

// custom block fields for extention
type BlockExtra struct {
	// extended fields for ETH block
	MixHash         *common.Hash `json:"mixHash,omitempty"`
	TotalDifficulty *hexutil.Big `json:"totalDifficulty,omitempty"`
	Sha3Uncles      *common.Hash `json:"sha3Uncles,omitempty"`

	TxnExts []*TransactionExtra `json:"-"`
}

// custom transaction fields for extention
type TransactionExtra struct {
	// extended fields for ETH transaction
	BlockNumber *hexutil.Big `json:"blockNumber,omitempty"`
	StandardV   *hexutil.Big `json:"standardV,omitempty"`
}

// custom receipt fields for extention
type ReceiptExtra struct {
	// extended fields for ETH receipt
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
		MixHash:         ethBlock.MixHash,
		TotalDifficulty: (*hexutil.Big)(ethBlock.TotalDifficulty),
		Sha3Uncles:      &ethBlock.Sha3Uncles,
		TxnExts:         txnExts,
	}
}

func ExtractEthTransactionExt(ethTxn *web3Types.TransactionDetail) *TransactionExtra {
	return &TransactionExtra{
		BlockNumber: (*hexutil.Big)(ethTxn.BlockNumber),
		StandardV:   (*hexutil.Big)(ethTxn.StandardV),
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
		LogExts: logExts,
	}
}
