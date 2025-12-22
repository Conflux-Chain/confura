package store

import (
	"encoding/json"
	"math/big"

	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

// Adapt EpochData to `ChainData` interface

func (e EpochData) Number() uint64 {
	return e.EpochNo
}

func (e EpochData) Hash() string {
	if e.PivotHash != nil {
		return e.PivotHash.String()
	}
	return ""
}

func (e EpochData) ExtractBlocks() []BlockLike {
	blocks := make([]BlockLike, len(e.Blocks))
	for i, b := range e.Blocks {
		blocks[i] = EpochBlockAdapter{b}
	}
	return blocks
}

func (e EpochData) ExtractReceipts() map[string]ReceiptLike {
	receipts := make(map[string]ReceiptLike, len(e.Receipts))
	for _, r := range e.Receipts {
		receipts[r.TransactionHash.String()] = EpochReceiptAdapter{r}
	}
	return receipts
}

func (e EpochData) ExtractLogs() (logs []LogLike) {
	for _, r := range e.Receipts {
		for _, l := range r.Logs {
			logs = append(logs, EpochLogAdapter{&l})
		}
	}
	return logs
}

type EpochBlockAdapter struct {
	*types.Block
}

func (a EpochBlockAdapter) Hash() string {
	return a.Block.Hash.String()
}

func (a EpochBlockAdapter) Number() uint64 {
	return a.BlockNumber.ToInt().Uint64()
}

func (a EpochBlockAdapter) Transactions() []TransactionLike {
	txs := make([]TransactionLike, 0, len(a.Block.Transactions))
	for _, t := range a.Block.Transactions {
		txs = append(txs, EpochTransactionAdapter{&t})
	}
	return txs
}

func (a EpochBlockAdapter) MarshalJSON() ([]byte, error) {
	// It's not a good idea to marshal the whole block including transactions,
	// so we only marshal the block summary here.
	blockSummary := &types.BlockSummary{BlockHeader: a.Block.BlockHeader}
	for _, tx := range a.Block.Transactions {
		blockSummary.Transactions = append(blockSummary.Transactions, tx.Hash)
	}
	return json.Marshal(blockSummary)
}

type EpochTransactionAdapter struct {
	*types.Transaction
}

func (a EpochTransactionAdapter) Hash() string {
	return a.Transaction.Hash.String()
}

func (a EpochTransactionAdapter) From() string {
	return a.Transaction.From.String()
}

func (a EpochTransactionAdapter) To() string {
	if a.Transaction.To != nil {
		return a.Transaction.To.String()
	}
	return ""
}

func (a EpochTransactionAdapter) Value() *big.Int {
	return a.Transaction.Value.ToInt()
}

func (a EpochTransactionAdapter) Executed() bool {
	return util.IsTxExecutedInBlock(a.Transaction)
}

type EpochReceiptAdapter struct {
	*types.TransactionReceipt
}

func (a EpochReceiptAdapter) TransactionHash() string {
	return a.TransactionReceipt.TransactionHash.String()
}

func (a EpochReceiptAdapter) Status() *uint64 {
	return (*uint64)(&a.OutcomeStatus)
}

func (a EpochReceiptAdapter) Logs() []LogLike {
	logs := make([]LogLike, 0, len(a.TransactionReceipt.Logs))
	for _, l := range a.TransactionReceipt.Logs {
		logs = append(logs, EpochLogAdapter{&l})
	}
	return logs
}

type EpochLogAdapter struct {
	*types.Log
}

func (a EpochLogAdapter) Address() string {
	return a.Log.Address.String()
}

func (a EpochLogAdapter) BlockHash() string {
	return a.Log.BlockHash.String()
}

func (a EpochLogAdapter) TransactionHash() string {
	return a.Log.TransactionHash.String()
}

func (a EpochLogAdapter) LogIndex() uint64 {
	return a.Log.LogIndex.ToInt().Uint64()
}

func (a EpochLogAdapter) Topics() []string {
	topics := make([]string, 0, len(a.Log.Topics))
	for _, t := range a.Log.Topics {
		topics = append(topics, t.String())
	}
	return topics
}

func (a EpochLogAdapter) Data() []byte {
	return a.Log.Data
}

func (a EpochLogAdapter) AsStoreLog(cid uint64) *Log {
	return ParseCfxLog(a.Log, cid, 0)
}
