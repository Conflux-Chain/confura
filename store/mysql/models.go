package mysql

import (
	"fmt"
	"strconv"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/jinzhu/gorm"
	"github.com/sirupsen/logrus"
)

type transaction struct {
	ID                uint64
	Epoch             uint64 `gorm:"not null;index"`
	HashId            uint64 `gorm:"not null;index"` // as an index, number is better than long string
	Hash              string `gorm:"size:66;not null"`
	TxRawData         []byte `gorm:"type:MEDIUMBLOB;not null"`
	TxRawDataLen      uint64 `gorm:"not null"`
	ReceiptRawData    []byte `gorm:"type:MEDIUMBLOB"`
	ReceiptRawDataLen uint64 `gorm:"not null"`
}

func (transaction) TableName() string {
	return "txs"
}

func hash2ShortId(hash string) uint64 {
	// first 8 bytes of hex string with 0x prefixed
	id, err := strconv.ParseUint(hash[2:18], 16, 64)
	if err != nil {
		logrus.WithError(err).WithField("hash", hash).Fatalf("Failed convert hash to short id")
	}

	return id
}

func newTx(tx *types.Transaction, receipt *types.TransactionReceipt) *transaction {
	result := &transaction{
		Epoch:          uint64(*receipt.EpochNumber),
		Hash:           tx.Hash.String(),
		TxRawData:      mustMarshalRLP(tx),
		ReceiptRawData: mustMarshalRLP(receipt),
	}

	result.HashId = hash2ShortId(result.Hash)
	result.TxRawDataLen = uint64(len(result.TxRawData))
	result.ReceiptRawDataLen = uint64(len(result.ReceiptRawData))

	return result
}

func loadTx(db *gorm.DB, txHash string) (*transaction, error) {
	hashId := hash2ShortId(txHash)

	var tx transaction

	db = db.Where("hash_id = ? AND hash = ?", hashId, txHash).First(&tx)
	if err := db.Scan(&tx).Error; err != nil {
		return nil, err
	}

	return &tx, nil
}

type block struct {
	ID         uint64
	Epoch      uint64 `gorm:"not null;index"`
	HashId     uint64 `gorm:"not null;index"`
	Hash       string `gorm:"size:66;not null"`
	Pivot      bool   `gorm:"not null"`
	RawData    []byte `gorm:"type:MEDIUMBLOB;not null"`
	RawDataLen uint64 `gorm:"not null"`
}

func newBlock(data *types.Block, pivot bool) *block {
	block := &block{
		Epoch:   data.EpochNumber.ToInt().Uint64(),
		Hash:    data.Hash.String(),
		Pivot:   pivot,
		RawData: mustMarshalRLP(block2Summary(data)),
	}

	block.HashId = hash2ShortId(block.Hash)
	block.RawDataLen = uint64(len(block.RawData))

	return block
}

func block2Summary(block *types.Block) *types.BlockSummary {
	summary := types.BlockSummary{
		BlockHeader:  block.BlockHeader,
		Transactions: make([]types.Hash, 0, len(block.Transactions)),
	}

	for _, tx := range block.Transactions {
		summary.Transactions = append(summary.Transactions, tx.Hash)
	}

	return &summary
}

func loadBlock(db *gorm.DB, whereClause string, args ...interface{}) (*types.BlockSummary, error) {
	var blk block

	db = db.Where(whereClause, args...).First(&blk)
	if err := db.Scan(&blk).Error; err != nil {
		return nil, err
	}

	var summary types.BlockSummary
	mustUnmarshalRLP(blk.RawData, &summary)

	return &summary, nil
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
	Data            []byte `gorm:"type:MEDIUMBLOB"`
	DataLen         uint64 `gorm:"not null"`
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
		Data:            []byte(data.Data),
		Topic0:          data.Topics[0].String(),
		TxHash:          data.TransactionHash.String(),
		TxIndex:         data.TransactionIndex.ToInt().Uint64(),
		TxLogIndex:      data.TransactionLogIndex.ToInt().Uint64(),
		LogIndex:        data.LogIndex.ToInt().Uint64(),
	}

	log.DataLen = uint64(len(log.Data))

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

func (log *log) toRPCLog() types.Log {
	blockHash := types.Hash(log.BlockHash)
	txHash := types.Hash(log.TxHash)

	topics := []types.Hash{types.Hash(log.Topic0)}

	// in case of empty hex string with 0x prefix
	if len(log.Topic1) > 2 {
		topics = append(topics, types.Hash(log.Topic1))
	}

	if len(log.Topic2) > 2 {
		topics = append(topics, types.Hash(log.Topic2))
	}

	if len(log.Topic3) > 2 {
		topics = append(topics, types.Hash(log.Topic3))
	}

	return types.Log{
		Address:             cfxaddress.MustNewFromBase32(log.ContractAddress),
		Topics:              topics,
		Data:                types.NewBytes(log.Data),
		BlockHash:           &blockHash,
		EpochNumber:         types.NewBigInt(log.Epoch),
		TransactionHash:     &txHash,
		TransactionIndex:    types.NewBigInt(log.TxIndex),
		LogIndex:            types.NewBigInt(log.LogIndex),
		TransactionLogIndex: types.NewBigInt(log.TxLogIndex),
	}
}

func loadLogs(db *gorm.DB, filter store.LogFilter) ([]types.Log, error) {
	db = db.Where("epoch BETWEEN ? AND ?", filter.EpochFrom, filter.EpochTo)
	db = applyVariadicFilter(db, "contract_address", filter.Contracts)
	db = applyVariadicFilter(db, "block_hash", filter.Blocks)

	numTopics := len(filter.Topics)

	if numTopics > 0 {
		db = applyVariadicFilter(db, "topic0", filter.Topics[0])
	}

	if numTopics > 1 {
		db = applyVariadicFilter(db, "topic1", filter.Topics[1])
	}

	if numTopics > 2 {
		db = applyVariadicFilter(db, "topic2", filter.Topics[2])
	}

	if numTopics > 3 {
		db = applyVariadicFilter(db, "topic3", filter.Topics[3])
	}

	db.Limit(filter.Limit)

	var logs []log
	if err := db.Find(&logs).Error; err != nil {
		return nil, err
	}

	var result []types.Log
	for _, l := range logs {
		result = append(result, l.toRPCLog())
	}

	return result, nil
}

func applyVariadicFilter(db *gorm.DB, column string, value store.VariadicValue) *gorm.DB {
	if single, ok := value.Single(); ok {
		return db.Where(fmt.Sprintf("%v = ?", column), single)
	}

	if multiple, ok := value.FlatMultiple(); ok {
		return db.Where(fmt.Sprintf("%v IN (?)", column), multiple)
	}

	return db
}

func mustMarshalRLP(v interface{}) []byte {
	if v == nil {
		return nil
	}

	data, err := rlp.EncodeToBytes(v)
	if err != nil {
		logrus.WithError(err).Fatalf("Failed to marshal data to RLP, value = %+v", v)
	}

	return data
}

func mustUnmarshalRLP(data []byte, v interface{}) {
	if err := rlp.DecodeBytes(data, v); err != nil {
		logrus.WithError(err).Fatalf("Failed to unmarshal RLP data, v = %v, data = %x", v, data)
	}
}
