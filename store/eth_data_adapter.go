package store

import (
	"encoding/json"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	geth "github.com/ethereum/go-ethereum/core/types"
	"github.com/openweb3/web3go/types"
)

// Adapt EthData to `ChainData` interface

func (e EthData) Number() uint64 {
	return e.BlockNo
}

func (e EthData) Hash() string {
	return e.Block.Hash.String()
}

func (e EthData) ExtractBlocks() []BlockLike {
	// Transaction `status` field is not a standard field for evm-compatible chain, so we have
	// to manually fill this field from their receipt.
	txs := e.Block.Transactions.Transactions()
	for i := range txs {
		if txs[i].Status == nil && e.Receipts != nil {
			if receipt := e.Receipts[txs[i].Hash]; receipt != nil {
				txs[i].Status = receipt.Status
			}
		}
	}

	e.Block.Transactions = *types.NewTxOrHashListByTxs(txs)
	return []BlockLike{EthBlockAdapter{e.Block}}
}

func (e EthData) ExtractReceipts() map[string]ReceiptLike {
	receipts := make(map[string]ReceiptLike, len(e.Receipts))
	for _, r := range e.Receipts {
		receipts[r.TransactionHash.String()] = EthReceiptAdapter{r}
	}
	return receipts
}

func (e EthData) ExtractLogs() []LogLike {
	var logs []LogLike
	for _, r := range e.Receipts {
		for _, l := range r.Logs {
			logs = append(logs, EthLogAdapter{l})
		}
	}
	return logs
}

type EthBlockAdapter struct {
	*types.Block
}

func (a EthBlockAdapter) Hash() string {
	return a.Block.Hash.String()
}

func (a EthBlockAdapter) Number() uint64 {
	return a.Block.Number.Uint64()
}

func (a EthBlockAdapter) Transactions() []TransactionLike {
	rawTxs := a.Block.Transactions.Transactions()
	txs := make([]TransactionLike, 0, len(rawTxs))
	for _, t := range rawTxs {
		txs = append(txs, &EthTransactionAdapter{&t})
	}
	return txs
}

func (a EthBlockAdapter) MarshalJSON() ([]byte, error) {
	txHashes := make([]common.Hash, 0, len(a.Block.Transactions.Transactions()))
	for _, tx := range a.Block.Transactions.Transactions() {
		txHashes = append(txHashes, tx.Hash)
	}

	blockCopy := *a.Block
	blockCopy.Transactions = *types.NewTxOrHashListByHashes(txHashes)
	return json.Marshal(blockCopy)
}

type EthTransactionAdapter struct {
	*types.TransactionDetail
}

func (a EthTransactionAdapter) Hash() string {
	return a.TransactionDetail.Hash.String()
}

func (a EthTransactionAdapter) From() string {
	return a.TransactionDetail.From.String()
}

func (a EthTransactionAdapter) To() string {
	if a.TransactionDetail.To != nil {
		return a.TransactionDetail.To.String()
	}
	return ""
}

func (a EthTransactionAdapter) Value() *big.Int {
	return a.TransactionDetail.Value
}

func (a *EthTransactionAdapter) Executed() bool {
	return a.Status != nil &&
		(*a.Status == geth.ReceiptStatusSuccessful ||
			*a.Status == geth.ReceiptStatusFailed)
}

type EthReceiptAdapter struct {
	*types.Receipt
}

func (a EthReceiptAdapter) TransactionHash() string {
	return a.Receipt.TransactionHash.String()
}

func (a EthReceiptAdapter) Status() *uint64 {
	return a.Receipt.Status
}

func (a EthReceiptAdapter) Logs() []LogLike {
	logs := make([]LogLike, 0, len(a.Receipt.Logs))
	for _, l := range a.Receipt.Logs {
		logs = append(logs, EthLogAdapter{l})
	}
	return logs
}

type EthLogAdapter struct {
	*types.Log
}

func (a EthLogAdapter) Address() string {
	return a.Log.Address.String()
}

func (a EthLogAdapter) BlockHash() string {
	return a.Log.BlockHash.String()
}

func (a EthLogAdapter) BlockNumber() uint64 {
	return a.Log.BlockNumber
}

func (a EthLogAdapter) TransactionHash() string {
	return a.Log.TxHash.String()
}

func (a EthLogAdapter) LogIndex() uint64 {
	return uint64(a.Log.Index)
}

func (a EthLogAdapter) Topics() []string {
	topics := make([]string, 0, len(a.Log.Topics))
	for _, t := range a.Log.Topics {
		topics = append(topics, t.String())
	}
	return topics
}

func (a EthLogAdapter) Data() []byte {
	return a.Log.Data
}

func (a EthLogAdapter) AsStoreLog(cid uint64) *Log {
	return ParseEthLog(a.Log, cid, a.Log.BlockNumber)
}
