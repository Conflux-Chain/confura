package store

import (
	"math/big"
)

type ChainData interface {
	Hash() string
	Number() uint64
	ExtractBlocks() []BlockLike
	ExtractReceipts() map[string]ReceiptLike
	ExtractLogs() []LogLike
}

type BlockLike interface {
	Hash() string
	Number() uint64
	Transactions() []TransactionLike
}

type TransactionLike interface {
	Hash() string
	From() string
	To() string
	Value() *big.Int
	Executed() bool
}

type ReceiptLike interface {
	Status() *uint64
	Logs() []LogLike
	TransactionHash() string
}

type LogLike interface {
	Address() string
	Topics() []string
	Data() []byte
	AsStoreLog(cid uint64) *Log
}
