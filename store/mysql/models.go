package mysql

import (
	"encoding/json"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/jinzhu/gorm"
	"github.com/sirupsen/logrus"
)

type transaction struct {
	ID    uint64
	Epoch uint64 `gorm:"not null;index"`
	Hash  string `gorm:"size:66;not null;index"`
	// TODO varchar(4096) is enough, otherwise, query from fullnode
	// the same for other BLOB type data, e.g. log.Data
	TxRawData      []byte `gorm:"type:MEDIUMBLOB;not null"`
	ReceiptRawData []byte `gorm:"type:BLOB"`
}

func (transaction) TableName() string {
	return "txs"
}

func newTx(tx *types.Transaction, receipt *types.TransactionReceipt) *transaction {
	result := &transaction{
		Epoch:          uint64(*receipt.EpochNumber),
		Hash:           tx.Hash.String(),
		TxRawData:      mustMarshalJSON(tx),
		ReceiptRawData: mustMarshalJSON(receipt),
	}

	return result
}

func loadTx(db *gorm.DB, txHash types.Hash) (*transaction, error) {
	var tx transaction

	db = db.Where("hash = ?", txHash.String()).First(&tx)
	if err := db.Scan(&tx).Error; err != nil {
		return nil, err
	}

	return &tx, nil
}

type block struct {
	ID      uint64
	Hash    string `gorm:"size:66;not null;index"`
	Epoch   uint64 `gorm:"not null;index"`
	Pivot   bool   `gorm:"not null"`
	RawData []byte `gorm:"type:MEDIUMBLOB;not null"`
}

func newBlock(data *types.Block, pivot bool) *block {
	return &block{
		Hash:    data.Hash.String(),
		Epoch:   data.EpochNumber.ToInt().Uint64(),
		Pivot:   pivot,
		RawData: mustMarshalJSON(data),
	}
}

func loadBlock(db *gorm.DB, whereClause string, args ...interface{}) (*types.Block, error) {
	var blk block

	db = db.Where(whereClause, args...).First(&blk)
	if err := db.Scan(&blk).Error; err != nil {
		return nil, err
	}

	var rpcBlock types.Block
	mustUnmarshalJSON(blk.RawData, &rpcBlock)

	return &rpcBlock, nil
}

type log struct {
	ID              uint64
	Epoch           uint64 `gorm:"not null;index"`
	BlockHash       string `gorm:"size:66;not null"`
	ContractAddress string `gorm:"size:64;not null"`
	Topic0          string `gorm:"size:66;not null"`
	Topic1          string `gorm:"size:66"`
	Topic2          string `gorm:"size:66"`
	Topic3          string `gorm:"size:66"`
	Data            string `gorm:"type:BLOB"`
	TxHash          string `gorm:"size:66;not null"`
	TxIndex         uint64 `gorm:"not null"`
	TxLogIndex      uint64 `gorm:"not null"`
	LogIndex        uint64 `gorm:"not null"`
}

func newLog(data *types.Log) *log {
	log := &log{
		Epoch:           data.EpochNumber.ToInt().Uint64(),
		BlockHash:       data.BlockHash.String(),
		ContractAddress: data.Address.MustGetBase32Address(),
		Data:            data.Data.String(),
		Topic0:          data.Topics[0].String(),
		TxHash:          data.TransactionHash.String(),
		TxIndex:         data.TransactionIndex.ToInt().Uint64(),
		TxLogIndex:      data.TransactionLogIndex.ToInt().Uint64(),
		LogIndex:        data.LogIndex.ToInt().Uint64(),
	}

	numTopics := len(data.Topics)

	if numTopics > 1 {
		log.Topic1 = data.Topics[1].String()
	}

	if numTopics > 2 {
		log.Topic2 = data.Topics[2].String()
	}

	if numTopics > 3 {
		log.Topic3 = data.Topics[3].String()
	}

	return log
}

func mustMarshalJSON(v interface{}) []byte {
	if v == nil {
		return nil
	}

	data, err := json.Marshal(v)
	if err != nil {
		logrus.WithError(err).Fatalf("Failed to marshal data to JSON, value = %+v", v)
	}

	return data
}

func mustUnmarshalJSON(data []byte, v interface{}) {
	if err := json.Unmarshal(data, v); err != nil {
		logrus.WithError(err).Fatalf("Failed to unmarshal data, data = %v", string(data))
	}
}
