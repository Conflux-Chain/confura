package mysql

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// storePruner observes bn partition changes and prunes log partitions.
type storePruner struct {
	// block number range partitioned store
	partitionedStore *bnPartitionedStore
	// channel to observe new entity bnPartition
	newBnPartitionObsChan chan *bnPartition
	// mapset to hold entity for which new bnPartition observed
	// entity => schema.Tabler
	bnPartitionObsEntitySet sync.Map
}

func newStorePruner(db *gorm.DB) *storePruner {
	pruner := &storePruner{
		newBnPartitionObsChan: make(chan *bnPartition, 1),
		partitionedStore:      newBnPartitionedStore(db),
	}

	go pruner.observe()
	return pruner
}

// observe observes bn partition changes and updates the mapset of entity which will
// be tracked by the pruner.
func (sp *storePruner) observe() {
	for partition := range sp.newBnPartitionObsChan {
		if partition.tabler != nil {
			sp.bnPartitionObsEntitySet.Store(partition.Entity, partition.tabler)
		}
	}
}

// schedulePrune periodically monitors and removes extra more than the max specified number of
// archive bn partitions. Be noted this function will block caller thread.
func (sp *storePruner) schedulePrune(config *Config) {
	ticker := time.NewTicker(time.Minute * 15)
	defer ticker.Stop()

	for range ticker.C {
		sp.bnPartitionObsEntitySet.Range(func(key, value any) bool {
			entity := key.(string)
			tabler := value.(schema.Tabler)

			pruned, err := sp.partitionedStore.pruneArchivePartitions(
				entity, tabler, config.MaxBnRangedArchiveLogPartitions,
			)

			logger := logrus.WithField("entity", entity)

			if err != nil {
				logger.WithError(err).Error("Failed to prune archive log partitions")
			}

			if len(pruned) > 0 {
				logger.WithField("prunedPartitions", pruned).Info("Archive partitions pruned")
			}

			if err == nil {
				sp.bnPartitionObsEntitySet.Delete(entity)

				// To minimize the db performance loss, we only remove extra archive partitions
				// for one entity at a time.
				if len(pruned) > 0 {
					return false
				}
			}

			// continue to next entity
			return true
		})
	}
}
