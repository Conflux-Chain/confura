package mysql

import (
	"context"
	"math"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/types"
	"github.com/Conflux-Chain/confura/util"
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
	ContractID  uint64 `gorm:"column:cid;size:64;not null"`
	BlockNumber uint64 `gorm:"column:bn;not null;index:idx_bn"`
	Epoch       uint64 `gorm:"not null"`
	Topic0      string `gorm:"size:66;not null"`
	Topic1      string `gorm:"size:66"`
	Topic2      string `gorm:"size:66"`
	Topic3      string `gorm:"size:66"`
	LogIndex    uint64 `gorm:"not null"`
	Extra       []byte `gorm:"type:mediumText"` // extension json field
}

func (log) TableName() string {
	return "logs"
}

type logStore struct {
	*bnPartitionedStore
	cs    *ContractStore
	ebms  *epochBlockMapStore
	model log
	// notify channel for new bn partition created
	bnPartitionNotifyChan chan<- *bnPartition
}

func newLogStore(db *gorm.DB, cs *ContractStore, ebms *epochBlockMapStore, notifyChan chan<- *bnPartition) *logStore {
	return &logStore{
		bnPartitionedStore:    newBnPartitionedStore(db),
		bnPartitionNotifyChan: notifyChan, cs: cs, ebms: ebms,
	}
}

// preparePartition create new log partitions if necessary.
func (ls *logStore) preparePartition(dataSlice []*store.EpochData) (bnPartition, error) {
	partition, newCreated, err := ls.autoPartition(bnPartitionedLogEntity, &ls.model, bnPartitionedLogVolumeSize)
	if err == nil && newCreated {
		partition.tabler = &ls.model
		ls.bnPartitionNotifyChan <- &partition
	}

	return partition, err
}

func (ls *logStore) Add(dbTx *gorm.DB, dataSlice []*store.EpochData, logPartition bnPartition) error {
	// containers to collect event logs for batch inserting
	var logs []*log
	bnMin, bnMax := uint64(math.MaxUint64), uint64(0)

	for _, data := range dataSlice {
		for _, block := range data.Blocks {
			bn := block.BlockNumber.ToInt().Uint64()
			bnMin, bnMax = min(bnMin, bn), max(bnMax, bn)

			for _, tx := range block.Transactions {
				receipt := data.Receipts[tx.Hash]

				// Skip transactions that unexecuted in block.
				if receipt == nil || !util.IsTxExecutedInBlock(&tx) {
					continue
				}

				var rcptExt *store.ReceiptExtra
				if len(data.ReceiptExts) > 0 {
					rcptExt = data.ReceiptExts[tx.Hash]
				}

				for k, rlog := range receipt.Logs {
					cid, _, err := ls.cs.AddContractIfAbsent(rlog.Address.MustGetBase32Address())
					if err != nil {
						return errors.WithMessage(err, "failed to add contract")
					}

					var logExt *store.LogExtra
					if rcptExt != nil && k < len(rcptExt.LogExts) {
						logExt = rcptExt.LogExts[k]
					}

					clog := store.ParseCfxLog(&rlog, cid, bn, logExt)
					logs = append(logs, (*log)(clog))
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
func (ls *logStore) Popn(dbTx *gorm.DB, epochUntil uint64) error {
	bn, ok, err := ls.ebms.BlockRange(epochUntil)
	if err != nil {
		return errors.WithMessagef(err, "failed to get block mapping for epoch %v", epochUntil)
	}

	if !ok { // no block mapping found for epoch
		return errors.Errorf("no block mapping found for epoch %v", epochUntil)
	}

	// update block range for log partition router
	partitions, existed, err := ls.shrinkBnRange(dbTx, bnPartitionedLogEntity, bn.From)
	if err != nil {
		return errors.WithMessage(err, "failed to shrink partition bn range")
	}

	if !existed { // no partition found?
		return nil
	}

	for i := len(partitions) - 1; i >= 0; i-- {
		partition := partitions[i]
		tblName := ls.getPartitionedTableName(&log{}, partition.Index)

		res := dbTx.Table(tblName).Where("bn >= ?", bn.From).Delete(log{})
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

func (ls *logStore) GetLogs(ctx context.Context, storeFilter store.LogFilter) ([]*store.Log, error) {
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
		Topics:    storeFilter.Topics,
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
func (ls *logStore) GetBnPartitionedLogs(ctx context.Context, filter LogFilter, partition bnPartition) ([]*log, error) {
	filter.TableName = ls.getPartitionedTableName(&log{}, partition.Index)

	var res []*log
	err := filter.find(ctx, ls.db, &res)

	return res, err
}

// extractUniqueContractAddresses extracts unique contract addresses of event logs within epoch data slice.
func extractUniqueContractAddresses(slice ...*store.EpochData) map[string]bool {
	contracts := make(map[string]bool)

	for _, data := range slice {
		for _, receipt := range data.Receipts {
			for i := range receipt.Logs {
				addr := receipt.Logs[i].Address.String()
				contracts[addr] = true
			}
		}
	}

	return contracts
}
