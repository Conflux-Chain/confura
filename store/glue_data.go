package store

import (
	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

type Block struct {
	CfxBlock *types.Block
	Extra    *BlockExtra
}

type BlockSummary struct {
	CfxBlockSummary *types.BlockSummary
	Extra           *BlockExtra
}

type Transaction struct {
	CfxTransaction *types.Transaction
	Extra          *TransactionExtra
}

type TransactionReceipt struct {
	CfxReceipt *types.TransactionReceipt
	Extra      *ReceiptExtra
}
