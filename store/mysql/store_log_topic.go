package mysql

import (
	"context"
	"hash/fnv"

	"github.com/Conflux-Chain/confura/store"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

// Topic indexed logs are used to filter event logs by topic0 and optional block number.
// Generally, most topics have limited event logs and need not to specify the epoch/block range filter.
// For high-frequency topics (e.g., Transfer) are typically stored in separate special tables.
type TopicIndexedLog struct {
	ID          uint64
	BlockNumber uint64 `gorm:"column:bn;not null;index:idx_tid_bn,priority:2"`
	Epoch       uint64 `gorm:"not null;index"`                                  // to support pop logs when reorg
	Topic0ID    uint64 `gorm:"column:tid;not null;index:idx_tid_bn,priority:1"` // topic0 id
	Topic1      string `gorm:"size:66"`
	Topic2      string `gorm:"size:66"`
	Topic3      string `gorm:"size:66"`
	LogIndex    uint64 `gorm:"not null"`
	Extra       []byte `gorm:"type:mediumText"` // extension json field
}

func (TopicIndexedLog) TableName() string {
	return "topic_logs"
}

// TopicIndexedLogStore manages topic-indexed event logs across partitioned tables.
type TopicIndexedLogStore[T store.ChainData] struct {
	partitionedStore
	db         *gorm.DB
	ts         *TopicStore
	partitions uint32
}

func NewTopicIndexedLogStore[T store.ChainData](db *gorm.DB, ts *TopicStore, partitions uint32) *TopicIndexedLogStore[T] {
	return &TopicIndexedLogStore[T]{db: db, ts: ts, partitions: partitions}
}

// CreatePartitionedTables initializes the N partitioned tables.
func (ls *TopicIndexedLogStore[T]) CreatePartitionedTables() (int, error) {
	return ls.createPartitionedTables(ls.db, &TopicIndexedLog{}, 0, ls.partitions)
}

// Add batch inserts event logs from a batch of epochs into partitioned tables, skipping "Big Topics".
func (s *TopicIndexedLogStore[T]) Add(tx *gorm.DB, dataSlice []T, bigTopicIDs map[uint64]bool) error {
	var (
		logsByPartition = make(map[uint32][]*TopicIndexedLog)
		statsByTopic    = make(map[uint64]struct {
			count int
			epoch uint64
		})
	)

	// Distribute event logs into different partitions by topic0
	for _, data := range dataSlice {
		receipts := data.ExtractReceipts()
		epoch := data.Number()

		for _, block := range data.ExtractBlocks() {
			bn := block.Number()

			for _, tx := range block.Transactions() {
				// ignore txs that are not executed in current block
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

					// Get or create topic ID
					tid, _, err := s.ts.GetOrCreate(topics[0])
					if err != nil {
						return err
					}

					// Skip Big Topics (handled by separate store)
					if bigTopicIDs[tid] {
						continue
					}

					slog := log.AsStoreLog()

					// Assign to partition
					p := s.getPartitionByHash(topics[0])
					logsByPartition[p] = append(logsByPartition[p], &TopicIndexedLog{
						BlockNumber: bn,
						Epoch:       epoch,
						Topic0ID:    tid,
						Topic1:      slog.Topic1,
						Topic2:      slog.Topic2,
						Topic3:      slog.Topic3,
						LogIndex:    slog.LogIndex,
						Extra:       slog.Extra,
					})

					// Aggregate stats
					stat := statsByTopic[tid]
					stat.count++
					stat.epoch = epoch
					statsByTopic[tid] = stat
				}
			}
		}
	}

	// Bulk insert per partition
	for p, logs := range logsByPartition {
		tbl := s.getPartitionedTableName(&TopicIndexedLog{}, p)
		if err := tx.Table(tbl).Create(&logs).Error; err != nil {
			return errors.WithMessagef(err, "failed to insert into %s", tbl)
		}
	}

	// Update stats per topic
	for tid, stat := range statsByTopic {
		if err := s.ts.UpdateStats(tx, tid, stat.count, stat.epoch); err != nil {
			return errors.WithMessagef(err, "failed to update stats for topic %v", tid)
		}
	}

	return nil
}

// DeleteTopicIndexedLogs removes topic0-indexed logs within an epoch range.
//
// This is primarily used during pivot chain switches for confirmed blocks.
//
// DESIGN NOTES:
//
// 1. Deletion is performed at partition granularity for efficiency.
// 2. Contract statistics (log_count, latest_updated_epoch) are intentionally NOT updated here.
//   - log_count is a monotonic upper bound used for migration/sharding decisions and must not be rolled back.
//   - latest_updated_epoch represents a historical upper bound and is updated only on forward sync (ingest).
//
// 3. This function may delete logs multiple times for the same topic0 across reorgs, which is acceptable and safe,
// while missing a deletion is not.
func (s *TopicIndexedLogStore[T]) DeleteTopicIndexedLogs(tx *gorm.DB, fromEpoch, toEpoch uint64) error {
	updatedTopics, err := s.ts.GetUpdatedSince(fromEpoch)
	if err != nil {
		return errors.WithMessagef(err, "failed to get updated topics")
	}

	if len(updatedTopics) == 0 {
		return nil
	}

	// Collect affected partitions.
	partitions := make(map[uint32]struct{})
	for _, t := range updatedTopics {
		partitions[s.getPartitionByHash(t.Hash)] = struct{}{}
	}

	// Bulk delete event logs per partition.
	for p := range partitions {
		tbl := s.getPartitionedTableName(&TopicIndexedLog{}, p)
		if err := tx.Table(tbl).Where("epoch BETWEEN ? AND ?", fromEpoch, toEpoch).Delete(nil).Error; err != nil {
			return errors.WithMessagef(
				err,
				"failed to delete topic indexed logs from %s [epoch %d-%d]",
				tbl, fromEpoch, toEpoch,
			)
		}
	}

	return nil
}

// GetTpoicIndexedLogs executes the filter query against the correct partition table.
func (s *TopicIndexedLogStore[T]) GetTopicIndexedLogs(
	ctx context.Context,
	tid uint64,
	topic string,
	sfilter store.LogFilter,
) ([]*store.Log, error) {
	filter := LogFilter{
		TableName: s.GetPartitionedTableName(topic),
		BlockFrom: sfilter.BlockFrom,
		BlockTo:   sfilter.BlockTo,
		Topics:    store.ToVariadicValuers(sfilter.Topics...),
		Schema:    &SecondaryOnlyTopicSchema,
	}

	topicFilter := TopicIndexedLogFilter{filter, tid}
	topicLogs, err := topicFilter.Find(ctx, s.db)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get topic indexed logs")
	}

	// convert to common store log
	var result []*store.Log
	for _, v := range topicLogs {
		result = append(result, &store.Log{
			BlockNumber: v.BlockNumber,
			Epoch:       v.Epoch,
			Topic0:      topic,
			Topic1:      v.Topic1,
			Topic2:      v.Topic2,
			Topic3:      v.Topic3,
			LogIndex:    v.LogIndex,
			Extra:       v.Extra,
		})
	}

	return result, nil
}

// GetPartitionedTableName returns the physical table name for a given topic hash.
func (s *TopicIndexedLogStore[T]) GetPartitionedTableName(topic string) string {
	return s.getPartitionedTableName(&TopicIndexedLog{}, s.getPartitionByHash(topic))
}

// getPartitionByHash returns the partition by topic hash.
func (s *TopicIndexedLogStore[T]) getPartitionByHash(hash string) uint32 {
	h := fnv.New32()
	h.Write([]byte(hash))
	// could use consistent hashing if repartition supported
	return h.Sum32() % s.partitions
}
