package mysql

import (
	"context"
	"fmt"
	"math"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

const (
	// Log count threshold for a topic to be considered "big" and migrated to a dedicated table.
	thresholdBigTopicLogCount = 100_000

	// Volume size per log partition for dedicated big topic table.
	bigTopicBnPartitionedLogVolumeSize = 1_000_000
)

// topicLog represents a log entry stored in a dedicated topic table.
type topicLog struct {
	ID          uint64
	TopicID     uint64 `gorm:"-"` // not persisted, used for table naming
	BlockNumber uint64 `gorm:"column:bn;not null;index:idx_bn"`
	Epoch       uint64 `gorm:"not null"`
	Topic1      string `gorm:"size:66"`
	Topic2      string `gorm:"size:66"`
	Topic3      string `gorm:"size:66"`
	LogIndex    uint64 `gorm:"not null"`
	Extra       []byte `gorm:"type:mediumText"` // extension json field
}

func (l topicLog) TableName() string {
	return fmt.Sprintf("tlogs_%d", l.TopicID)
}

// bigTopicLogStore manages partitioned storage for topics with large log volumes.
type bigTopicLogStore[T store.ChainData] struct {
	*bnPartitionedStore
	ts   *TopicStore
	ebms *epochBlockMapStore[T]
	tils *TopicIndexedLogStore[T]
	// notify channel for new bn partition created
	bnPartitionNotifyChan chan<- *bnPartition
}

func newBigTopicLogStore[T store.ChainData](
	db *gorm.DB,
	ts *TopicStore,
	ebms *epochBlockMapStore[T],
	tils *TopicIndexedLogStore[T],
	notifyChan chan<- *bnPartition,
) *bigTopicLogStore[T] {
	return &bigTopicLogStore[T]{
		ts: ts, ebms: ebms, tils: tils,
		bnPartitionedStore:    newBnPartitionedStore(db),
		bnPartitionNotifyChan: notifyChan,
	}
}

// preparePartitions creates partitions for big topics and migrates existing data if needed.
func (btls *bigTopicLogStore[T]) preparePartitions(dataSlice []T) (map[uint64]bnPartition, error) {
	topic0Sigs := extractUniqueTopic0Signatures(dataSlice...)
	topic2Partitions := make(map[uint64]bnPartition)

	for sig := range topic0Sigs {
		topic0, ok, err := btls.ts.GetTopicByHash(sig)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to get topic by hash")
		}

		if !ok {
			return nil, errors.New("topic not found")
		}

		if topic0.LogCount < thresholdBigTopicLogCount {
			// not qualified for big topic
			continue
		}

		tlEntity, tlTabler := btls.topicEntity(topic0.ID), btls.topicTabler(topic0.ID)
		partition, newCreated, err := btls.autoPartition(tlEntity, tlTabler, bigTopicBnPartitionedLogVolumeSize)
		if err != nil {
			return nil, errors.WithMessage(err, "auto partition failed")
		}

		if newCreated {
			partition.tabler = tlTabler
			btls.bnPartitionNotifyChan <- &partition
		}

		topic2Partitions[topic0.ID] = partition

		// contract is considered as migrated already for the following cases:
		// 1. write partition is not the initial one;
		// 2. write partition is the initial one but has data already.
		if !partition.IsInitial() || partition.Count > 0 {
			continue
		}

		// Migrate event logs of new big topic to separate log table partition.
		// We assume the possibility of migration for more than one big topic
		// at the same time is very small, otherwise the migration process might
		// collapse the sync progress.
		if err := btls.migrate(topic0, partition); err != nil {
			return nil, errors.WithMessage(err, "failed to migrate logs")
		}
	}

	return topic2Partitions, nil
}

// migrate moves logs from shared shard table to dedicated big topic table.
func (btls *bigTopicLogStore[T]) migrate(topic *Topic, partition bnPartition) error {
	tiTableName := btls.tils.GetPartitionedTableName(topic.Hash)

	tlEntity, tlTabler := btls.topicEntity(topic.ID), btls.topicTabler(topic.ID)
	tlTableName := btls.getPartitionedTableName(tlTabler, partition.Index)

	return btls.db.Transaction(func(dbTx *gorm.DB) error {
		var tiLogs []*TopicIndexedLog
		var bnMin, bnMax uint64

		tidb := dbTx.Table(tiTableName).Where("tid = ?", topic.ID)

		res := tidb.FindInBatches(&tiLogs, defaultBatchSizeForLogMigrating, func(tx *gorm.DB, batch int) error {
			delIDs := make([]uint64, 0, len(tiLogs))
			tlLogs := make([]*topicLog, 0, len(tiLogs))

			for _, tilog := range tiLogs {
				// copy topic indexed event log
				tlog := (topicLog)(*tilog)
				tlog.ID = 0 // clear primary id

				tlLogs = append(tlLogs, &tlog)
				delIDs = append(delIDs, tilog.ID)
			}

			// insert into dedicated topic log table
			if err := dbTx.Table(tlTableName).Create(&tlLogs).Error; err != nil {
				return errors.WithMessage(err, "failed to insert topic logs")
			}

			// delete from topic indexed log table
			if err := dbTx.Table(tiTableName).Where("id IN (?)", delIDs).Delete(nil).Error; err != nil {
				return errors.WithMessage(err, "failed to delete topic indexed logs")
			}

			// Track block range stats
			if batch == 1 {
				bnMin = tiLogs[0].BlockNumber
			}
			bnMax = tiLogs[len(tiLogs)-1].BlockNumber

			return nil
		})

		if err := res.Error; err != nil {
			return errors.WithMessage(err, "failed to batch migrate topic indexed logs")
		}

		// expand partition block number range
		if err := btls.expandBnRange(dbTx, tlEntity, int(partition.Index), 0, bnMax); err != nil {
			return errors.WithMessage(err, "failed to expand partition bn range")
		}

		// update dedicated topic log partition count
		err := btls.deltaUpdateCount(dbTx, tlEntity, int(partition.Index), int(res.RowsAffected))
		if err != nil {
			return errors.WithMessage(err, "failed to update topic log partition count")
		}

		logrus.WithFields(logrus.Fields{
			"topic":             topic,
			"tiTableName":       tiTableName,
			"tlTableName":       tlTableName,
			"bnMin":             bnMin,
			"bnMax":             bnMax,
			"totalMigratedLogs": res.RowsAffected,
		}).Info("Topic indexed event logs migrated to big topic event logs table")

		return nil
	})
}

// topicEntity gets partition entity for dedicated topic logs table
func (bcls *bigTopicLogStore[T]) topicEntity(tid uint64) string {
	return topicLog{TopicID: tid}.TableName()
}

// topicTabler get partition tabler for dedicated topic logs table
func (bcls *bigTopicLogStore[T]) topicTabler(tid uint64) *topicLog {
	return &topicLog{TopicID: tid}
}

func (btls *bigTopicLogStore[T]) Add(dbTx *gorm.DB, dataSlice []T, topic2BnPartitions map[uint64]bnPartition) error {
	bnMin, bnMax := uint64(math.MaxUint64), uint64(0)
	topic2Logs := make(map[uint64][]*topicLog, len(topic2BnPartitions))

	for _, data := range dataSlice {
		receipts := data.ExtractReceipts()

		for _, block := range data.ExtractBlocks() {
			bn := block.Number()
			bnMin, bnMax = min(bnMin, bn), max(bnMax, bn)

			for _, tx := range block.Transactions() {
				// Skip transactions that is unexecuted in block.
				if !tx.Executed() {
					continue
				}

				receipt, ok := receipts[tx.Hash()]
				if !ok {
					continue
				}

				for _, log := range receipt.Logs() {
					topics := log.Topics()
					if len(topics) == 0 || topics[0] == "" {
						continue
					}

					tid, _, err := btls.ts.GetOrCreate(topics[0])
					if err != nil {
						return errors.WithMessage(err, "failed to add topic")
					}

					// only collect big topc event logs
					if _, ok := topic2BnPartitions[tid]; !ok {
						continue
					}

					slog := log.AsStoreLog(0)
					slog.BlockNumber = bn

					topic2Logs[tid] = append(topic2Logs[tid], &topicLog{
						TopicID:     tid,
						BlockNumber: slog.BlockNumber,
						Epoch:       slog.Epoch,
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

	// Bulk insert per topic partition
	for tid, partition := range topic2BnPartitions {
		tlEntity, tlTabler := btls.topicEntity(tid), btls.topicTabler(tid)

		// update block range for topic log partition router
		err := btls.expandBnRange(dbTx, tlEntity, int(partition.Index), bnMin, bnMax)
		if err != nil {
			return errors.WithMessage(err, "failed to expand partition bn range")
		}

		logs := topic2Logs[tid]
		if len(logs) == 0 {
			continue
		}

		tblName := btls.getPartitionedTableName(tlTabler, partition.Index)
		err = dbTx.Table(tblName).Create(logs).Error
		if err != nil {
			return errors.WithMessage(err, "failed to add topic logs")
		}

		// update partition data count
		err = btls.deltaUpdateCount(dbTx, tlEntity, int(partition.Index), len(logs))
		if err != nil {
			return errors.WithMessage(err, "failed to delta update partition size")
		}

		// Update topic statistics (log count and latest updated epoch).
		latestUpdateEpoch := logs[len(logs)-1].Epoch
		if err := btls.ts.UpdateStats(dbTx, tid, len(logs), latestUpdateEpoch); err != nil {
			return errors.WithMessage(err, "failed to update topic statistics")
		}
	}

	return nil
}

func (btls *bigTopicLogStore[T]) Popn(dbTx *gorm.DB, epochUntil uint64) error {
	topics, err := btls.ts.GetUpdatedSince(epochUntil)
	if err != nil {
		return errors.WithMessagef(err, "failed to get updated topics since epoch %v", epochUntil)
	}

	if len(topics) == 0 { // no possible topics found
		return nil
	}

	e2bmap, ok, err := btls.ebms.CeilBlockMapping(epochUntil)
	if err != nil {
		return errors.WithMessagef(err, "failed to get block mapping for epoch %v", epochUntil)
	}

	if !ok { // no block mapping found for epoch
		return errors.Errorf("no block mapping found for epoch %v", epochUntil)
	}

	// delete event logs for all possible topics.
	for _, topic := range topics {
		topicEntity := btls.topicEntity(topic.ID)
		topicTabler := btls.topicTabler(topic.ID)

		partitions, existed, err := btls.shrinkBnRange(dbTx, topicEntity, e2bmap.BnMin)
		if err != nil {
			return errors.WithMessage(err, "failed to shrink partition bn range")
		}

		if !existed { // no specified log partition found for the topic
			continue
		}

		for i := len(partitions) - 1; i >= 0; i-- {
			partition := partitions[i]
			tblName := btls.getPartitionedTableName(topicTabler, partition.Index)

			res := dbTx.Table(tblName).Where("bn >= ?", e2bmap.BnMin).Delete(nil)
			if res.Error != nil {
				return res.Error
			}

			// update partition data count
			err = btls.deltaUpdateCount(dbTx, topicEntity, int(partition.Index), -int(res.RowsAffected))
			if err != nil {
				return errors.WithMessage(err, "failed to delta update partition size")
			}
		}
	}

	return nil
}

// IsBigTopic check if the topic is big topic or not.
func (btls *bigTopicLogStore[T]) IsBigTopic(tid uint64) (bool, error) {
	partition, existed, err := btls.oldestPartition(btls.topicEntity(tid))
	if err != nil || !existed {
		return false, errors.WithMessage(err, "failed to get oldest partition")
	}

	// regarded as big topic with the following two cases:
	// 1. oldest partition is not the initial one;
	// 2. oldest partition is the initial one and has data on it.
	return !partition.IsInitial() || partition.Count > 0, nil
}

// GetTopicLogs queries logs for a specific big topic with specified filter.
func (btls *bigTopicLogStore[T]) GetTopicLogs(ctx context.Context, tid uint64, storeFilter store.LogFilter) ([]*store.Log, error) {
	bnRange := types.RangeUint64{From: storeFilter.BlockFrom, To: storeFilter.BlockTo}
	partitions, _, err := btls.searchPartitions(btls.topicEntity(tid), bnRange)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to search partitions")
	}

	topic0, _, err := btls.ts.GetHash(tid)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get topic0 hash")
	}

	filter := LogFilter{
		BlockFrom:  storeFilter.BlockFrom,
		BlockTo:    storeFilter.BlockTo,
		Topics:     storeFilter.Topics,
		SkipTopic0: true,
	}

	var result []*store.Log

	// Query logs per partition
	for _, partition := range partitions {
		// check timeout before query
		select {
		case <-ctx.Done():
			return nil, store.ErrGetLogsTimeout
		default:
		}

		var logs []*topicLog

		filter.TableName = btls.getPartitionedTableName(btls.topicTabler(tid), partition.Index)
		if err := filter.find(ctx, btls.db, &logs); err != nil {
			return nil, err
		}

		// convert to common store log
		for _, v := range logs {
			result = append(result, &store.Log{
				BlockNumber: v.BlockNumber,
				Epoch:       v.Epoch,
				Topic0:      topic0,
				Topic1:      v.Topic1,
				Topic2:      v.Topic2,
				Topic3:      v.Topic3,
				LogIndex:    v.LogIndex,
				Extra:       v.Extra,
			})
		}

		// check log count
		if store.IsBoundChecksEnabled(ctx) && len(result) > int(store.MaxLogLimit) {
			return nil, newSuggestedFilterResultSetTooLargeError(&storeFilter, result, true)
		}
	}

	return result, nil
}

// extractUniqueTopic0Signatures extracts unique topic0 signature of event logs within epoch data slice.
func extractUniqueTopic0Signatures[T store.ChainData](slice ...T) map[string]bool {
	topic0Sigs := make(map[string]bool)

	for _, data := range slice {
		for _, receipt := range data.ExtractReceipts() {
			for _, log := range receipt.Logs() {
				topicArr := log.Topics()
				if len(topicArr) > 0 && topicArr[0] != "" {
					topic0Sigs[topicArr[0]] = true
				}
			}
		}
	}

	return topic0Sigs
}
