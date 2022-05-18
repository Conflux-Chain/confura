package mysql

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
	"gorm.io/gorm"
	"gorm.io/hints"
)

const defaultBatchSizeTxnInsert = 500

type transaction struct {
	ID                uint64
	Epoch             uint64 `gorm:"not null;index"`
	HashId            uint64 `gorm:"not null;index"` // as an index, number is better than long string
	Hash              string `gorm:"size:66;not null"`
	TxRawData         []byte `gorm:"type:MEDIUMBLOB"`
	TxRawDataLen      uint64 `gorm:"not null"`
	ReceiptRawData    []byte `gorm:"type:MEDIUMBLOB"`
	ReceiptRawDataLen uint64 `gorm:"not null"`
	NumReceiptLogs    int    `gorm:"not null"`
	Extra             []byte `gorm:"type:text"` // dynamic fields within json string for txn extention
	ReceiptExtra      []byte `gorm:"type:text"` // dynamic fields within json string for receipt extention
}

func (transaction) TableName() string {
	return "txs"
}

func newTx(
	tx *types.Transaction, receipt *types.TransactionReceipt, txExtra *store.TransactionExtra,
	rcptExtra *store.ReceiptExtra, skipTx, skipReceipt bool,
) *transaction {
	rcptPtr := receipt

	// Receipt logs are not persisted into db store to save storage space, since
	// they can be retrieved and assembled from event logs.
	if len(receipt.Logs) > 0 {
		rcptCopy := *receipt
		rcptCopy.Logs = []types.Log{}
		rcptPtr = &rcptCopy
	}

	result := &transaction{
		Epoch: uint64(*receipt.EpochNumber),
		Hash:  tx.Hash.String(),
	}

	if !skipTx {
		result.TxRawData = util.MustMarshalRLP(tx)
	}

	if !skipReceipt {
		result.ReceiptRawData = util.MustMarshalRLP(rcptPtr)
	}

	result.HashId = util.GetShortIdOfHash(result.Hash)
	result.TxRawDataLen = uint64(len(result.TxRawData))
	result.ReceiptRawDataLen = uint64(len(result.ReceiptRawData))
	result.NumReceiptLogs = len(receipt.Logs)

	if !skipTx && txExtra != nil {
		// no need to store block number since epoch number can be used instead
		txExtra.BlockNumber = nil
		result.Extra = util.MustMarshalJson(txExtra)
	}

	if !skipReceipt && rcptExtra != nil {
		result.ReceiptExtra = util.MustMarshalJson(rcptExtra)
	}

	return result
}

func (tx *transaction) parseTxExtra() *store.TransactionExtra {
	if len(tx.Extra) == 0 {
		return nil
	}

	var extra store.TransactionExtra
	util.MustUnmarshalJson(tx.Extra, &extra)

	// To save space, we don't save blockNumber within extra fields.
	// Here we restore the block number from epoch number.
	extra.BlockNumber = (*hexutil.Big)(big.NewInt(int64(tx.Epoch)))
	return &extra
}

func (tx *transaction) parseTxReceiptExtra() *store.ReceiptExtra {
	if len(tx.ReceiptExtra) == 0 {
		return nil
	}

	var extra store.ReceiptExtra
	util.MustUnmarshalJson(tx.ReceiptExtra, &extra)

	return &extra
}

type txStore struct {
	logPartitioner
	db *gorm.DB
}

func newTxStore(db *gorm.DB) *txStore {
	return &txStore{
		db: db,
	}
}

func (ts *txStore) loadTx(txHash types.Hash) (*transaction, error) {
	hashId := util.GetShortIdOfHash(txHash.String())

	var tx transaction
	if err := ts.db.Where("hash_id = ? AND hash = ?", hashId, txHash).First(&tx).Error; err != nil {
		return nil, err
	}

	return &tx, nil
}

func (ts *txStore) GetTransaction(txHash types.Hash) (*store.Transaction, error) {
	tx, err := ts.loadTx(txHash)
	if err != nil {
		return nil, err
	}

	var rpcTx types.Transaction
	util.MustUnmarshalRLP(tx.TxRawData, &rpcTx)

	return &store.Transaction{
		CfxTransaction: &rpcTx, Extra: tx.parseTxExtra(),
	}, nil
}

func (ts *txStore) GetReceipt(txHash types.Hash) (*store.TransactionReceipt, error) {
	tx, err := ts.loadTx(txHash)
	if err != nil {
		return nil, err
	}

	var receipt types.TransactionReceipt
	util.MustUnmarshalRLP(tx.ReceiptRawData, &receipt)

	ptrRcptExtra := tx.parseTxReceiptExtra()

	// If unknown number of event logs (for back compatibility) or no event logs
	// exists inside transaction receipts, just skip retrieving && assembling
	// event logs for it.
	if tx.NumReceiptLogs <= 0 {
		return &store.TransactionReceipt{
			CfxReceipt: &receipt, Extra: ptrRcptExtra,
		}, nil
	}

	partitions, err := ts.findLogsPartitionsEpochRangeWithinStoreTx(ts.db, tx.Epoch, tx.Epoch)
	if err != nil {
		err = errors.WithMessage(err, "failed to get partitions for receipt logs assembling")
		return nil, err
	}

	if len(partitions) == 0 { // no partitions found?
		return nil, errors.New("no partitions found for receipt logs assembling")
	}

	// TODO: add benchmark to see if adding transaction hash short ID as index in logs table
	// would bring better performance.
	blockHashId := util.GetShortIdOfHash(string(receipt.BlockHash))
	db := ts.db.Where(
		"epoch = ? AND block_hash_id = ? AND tx_hash = ?", tx.Epoch, blockHashId, tx.Hash,
	)
	db = db.Clauses(hints.UseIndex("idx_logs_block_hash_id"))
	db = db.Table(fmt.Sprintf("logs PARTITION (%v)", strings.Join(partitions, ",")))

	var logs []log

	if err := db.Find(&logs).Error; err != nil {
		err = errors.WithMessage(err, "failed to get data for receipt logs assembling")
		return nil, err
	}

	if len(logs) != tx.NumReceiptLogs { // validate number of assembled receipt logs
		err := errors.Errorf(
			"num of assembled receipt logs mismatched, expect %v got %v",
			tx.NumReceiptLogs, len(logs),
		)
		return nil, err
	}

	var logExts []*store.LogExtra
	if ptrRcptExtra != nil {
		logExts = make([]*store.LogExtra, len(logs))
	}

	rpcLogs := make([]types.Log, 0, len(logs))
	for i := 0; i < len(logs); i++ {
		rpcLogs = append(rpcLogs, logs[i].toRPCLog())

		if ptrRcptExtra != nil && len(logs[i].Extra) > 0 {
			logExts[i] = logs[i].parseLogExtra()
		}
	}

	receipt.Logs = rpcLogs
	if ptrRcptExtra != nil {
		ptrRcptExtra.LogExts = logExts
	}

	return &store.TransactionReceipt{
		CfxReceipt: &receipt, Extra: ptrRcptExtra,
	}, nil
}

// AddTransactions adds transactions of specified epoch into db store.
func (ts *txStore) AddTransactions(dbTx *gorm.DB, data *store.EpochData, skipTx, skipRcpt bool) error {
	if skipTx && skipRcpt {
		return nil
	}

	// Containers to collect transactions and receipts for batch inserting.
	txns := make([]*transaction, 0, len(data.Blocks))
	for i, block := range data.Blocks {
		var blockExt *store.BlockExtra
		if i < len(data.BlockExts) {
			blockExt = data.BlockExts[i]
		}

		for j, tx := range block.Transactions {
			receipt := data.Receipts[tx.Hash]

			// Skip transactions that unexecuted in block.
			// !!! Still need to check BlockHash and Status in case more than one transactions
			// of the same hash appeared in the same epoch.
			if receipt == nil || !util.IsTxExecutedInBlock(&tx) {
				continue
			}

			var txExt *store.TransactionExtra
			if blockExt != nil && j < len(blockExt.TxnExts) {
				txExt = blockExt.TxnExts[j]
			}

			var rcptExt *store.ReceiptExtra
			if len(data.ReceiptExts) > 0 {
				rcptExt = data.ReceiptExts[tx.Hash]
			}

			if !skipTx || !skipRcpt {
				txn := newTx(&tx, receipt, txExt, rcptExt, skipTx, skipRcpt)
				txns = append(txns, txn)
			}
		}
	}

	if len(txns) == 0 {
		return nil
	}

	if err := dbTx.CreateInBatches(txns, defaultBatchSizeTxnInsert).Error; err != nil {
		return err
	}

	return nil
}
