package mysql

import (
	"context"
	"math"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/types"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

const (
	// entity name for block number partitioned event logs
	bnPartitionedLogEntity = "logs"
	// volume size per log partition
	bnPartitionedLogVolumeSize = 10_000_000
)

type log struct {
	ID          uint64
	BlockNumber uint64 `gorm:"column:bn;not null;index:idx_bn"`
	Epoch       uint64 `gorm:"not null"`
	Topic0      string `gorm:"size:66;not null"`
	Topic1      string `gorm:"size:66"`
	Topic2      string `gorm:"size:66"`
	Topic3      string `gorm:"size:66"`
	LogIndex    uint64 `gorm:"not null"`
	Extra       []byte `gorm:"type:MEDIUMBLOB"` // extension json field
}

func (log) TableName() string {
	return "logs"
}

type logStore[T store.ChainData] struct {
	*bnPartitionedStore
	cs    *ContractStore
	ebms  *epochBlockMapStore[T]
	model log
	// notify channel for new bn partition created
	bnPartitionNotifyChan chan<- *bnPartition
}

func newLogStore[T store.ChainData](
	db *gorm.DB,
	cs *ContractStore,
	ebms *epochBlockMapStore[T],
	notifyChan chan<- *bnPartition,
) *logStore[T] {
	return &logStore[T]{
		bnPartitionedStore:    newBnPartitionedStore(db),
		bnPartitionNotifyChan: notifyChan, cs: cs, ebms: ebms,
	}
}

// preparePartition create new log partitions if necessary.
func (ls *logStore[T]) preparePartition() (bnPartition, error) {
	partition, newCreated, err := ls.autoPartition(bnPartitionedLogEntity, &ls.model, bnPartitionedLogVolumeSize)
	if err == nil && newCreated {
		partition.tabler = &ls.model
		ls.bnPartitionNotifyChan <- &partition
	}

	return partition, err
}

func (ls *logStore[T]) Add(dbTx *gorm.DB, dataSlice []T, logPartition bnPartition) error {
	// containers to collect event logs for batch inserting
	var logs []*log
	bnMin, bnMax := uint64(math.MaxUint64), uint64(0)

	for _, data := range dataSlice {
		receipts := data.ExtractReceipts()
		for _, block := range data.ExtractBlocks() {
			bn := block.Number()
			bnMin, bnMax = min(bnMin, bn), max(bnMax, bn)

			for _, tx := range block.Transactions() {
				receipt := receipts[tx.Hash()]

				// Skip transactions that unexecuted in block.
				if receipt == nil || !tx.Executed() {
					continue
				}

				for _, rlog := range receipt.Logs() {
					slog := rlog.AsStoreLog()

					logs = append(logs, &log{
						BlockNumber: bn,
						Epoch:       data.Number(),
						Topic0:      slog.Topic0,
						Topic1:      slog.Topic1,
						Topic2:      slog.Topic2,
						Topic3:      slog.Topic3,
						LogIndex:    slog.LogIndex,
						Extra:       slog.Extra,
					})
				}
			}
		}
	}

	// update block range for log partition router
	err := ls.expandBnRange(dbTx, bnPartitionedLogEntity, int(logPartition.Index), bnMin, bnMax)
	if err != nil {
		return errors.WithMessage(err, "failed to expand partition bn range")
	}

	if len(logs) == 0 {
		return nil
	}

	tblName := ls.getPartitionedTableName(&ls.model, logPartition.Index)
	err = dbTx.Table(tblName).Create(logs).Error
	if err != nil {
		return err
	}

	// update partition data size
	err = ls.deltaUpdateCount(dbTx, bnPartitionedLogEntity, int(logPartition.Index), len(logs))
	if err != nil {
		return errors.WithMessage(err, "failed to delta update partition size")
	}

	return nil
}

// Popn pops event logs until the specific epoch from db store.
func (ls *logStore[T]) Popn(dbTx *gorm.DB, epochUntil uint64) error {
	e2bmap, ok, err := ls.ebms.CeilBlockMapping(epochUntil)
	if err != nil {
		return errors.WithMessagef(err, "failed to get block mapping for epoch %v", epochUntil)
	}

	if !ok { // no block mapping found for epoch
		return errors.Errorf("no block mapping found for epoch %v", epochUntil)
	}

	// update block range for log partition router
	partitions, existed, err := ls.shrinkBnRange(dbTx, bnPartitionedLogEntity, e2bmap.BnMin)
	if err != nil {
		return errors.WithMessage(err, "failed to shrink partition bn range")
	}

	if !existed { // no partition found?
		return nil
	}

	for i := len(partitions) - 1; i >= 0; i-- {
		partition := partitions[i]
		tblName := ls.getPartitionedTableName(&log{}, partition.Index)

		res := dbTx.Table(tblName).Where("bn >= ?", e2bmap.BnMin).Delete(log{})
		if res.Error != nil {
			return res.Error
		}

		// update partition data size
		err = ls.deltaUpdateCount(dbTx, bnPartitionedLogEntity, int(partition.Index), -int(res.RowsAffected))
		if err != nil {
			return errors.WithMessage(err, "failed to delta update partition size")
		}
	}

	return nil
}

func (ls *logStore[T]) GetLogs(ctx context.Context, storeFilter store.LogFilter) ([]*store.Log, error) {
	// find the partitions that holds the event logs
	partitions, _, err := ls.searchPartitions(
		bnPartitionedLogEntity, types.RangeUint64{
			From: storeFilter.BlockFrom,
			To:   storeFilter.BlockTo,
		},
	)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to search partitions")
	}

	filter := LogFilter{
		BlockFrom: storeFilter.BlockFrom,
		BlockTo:   storeFilter.BlockTo,
		Topics:    store.ToVariadicValuers(storeFilter.Topics...),
	}

	var result []*store.Log
	for _, partition := range partitions {
		// check timeout before query
		select {
		case <-ctx.Done():
			return nil, store.ErrGetLogsTimeout
		default:
		}

		logs, err := ls.GetBnPartitionedLogs(ctx, filter, *partition)
		if err != nil {
			return nil, err
		}

		// convert to common store log
		for _, v := range logs {
			result = append(result, (*store.Log)(v))
		}

		// check log count
		if store.IsBoundChecksEnabled(ctx) && len(result) > int(store.MaxLogLimit) {
			return nil, newSuggestedFilterResultSetTooLargeError(&storeFilter, result, true)
		}
	}

	return result, nil
}

// GetBnPartitionedLogs returns event logs for the specified block number partitioned log filter.
func (ls *logStore[T]) GetBnPartitionedLogs(ctx context.Context, filter LogFilter, partition bnPartition) ([]*log, error) {
	filter.TableName = ls.getPartitionedTableName(&log{}, partition.Index)

	var res []*log
	err := filter.find(ctx, ls.db, &res)

	return res, err
}

// extractUniqueContractAddresses extracts unique contract addresses of event logs within epoch data slice.
func extractUniqueContractAddresses[T store.ChainData](slice ...T) map[string]bool {
	contracts := make(map[string]bool)

	for _, data := range slice {
		for _, receipt := range data.ExtractReceipts() {
			for _, log := range receipt.Logs() {
				addr := log.Address()
				contracts[addr] = true
			}
		}
	}

	return contracts
}
