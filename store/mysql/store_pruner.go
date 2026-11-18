package mysql

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// storePruner observes bn partition changes and prunes log partitions.
type storePruner struct {
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	mu                 sync.Mutex
	stopped, scheduled bool
	// block number range partitioned store
	partitionedStore *bnPartitionedStore
	// channel to observe new entity bnPartition
	newBnPartitionObsChan chan *bnPartition
	// mapset to hold entity for which new bnPartition observed
	// entity => schema.Tabler
	bnPartitionObsEntitySet sync.Map
}

func newStorePruner(db *gorm.DB) *storePruner {
	ctx, cancel := context.WithCancel(context.Background())
	pruner := &storePruner{
		ctx: ctx, cancel: cancel,
		newBnPartitionObsChan: make(chan *bnPartition, 1),
		partitionedStore:      newBnPartitionedStore(db.WithContext(ctx)),
	}

	pruner.wg.Add(1)
	go func() { defer pruner.wg.Done(); pruner.observe() }()
	return pruner
}

// observe observes bn partition changes and updates the mapset of entity which will
// be tracked by the pruner.
func (sp *storePruner) observe() {
	for {
		var partition *bnPartition
		select {
		case <-sp.ctx.Done():
			return
		case partition = <-sp.newBnPartitionObsChan:
		}
		if partition.tabler != nil {
			logrus.WithField("entity", partition.Entity).Debug("New bn partition observed, added to pruner tracking set")
			sp.bnPartitionObsEntitySet.Store(partition.Entity, partition.tabler)
		}
	}
}

// schedulePrune periodically monitors and removes extra more than the max specified number of
// archive bn partitions. Be noted this function will block caller thread.
func (sp *storePruner) schedulePrune(config *Config) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.stopped || sp.scheduled {
		return
	}
	sp.scheduled = true
	sp.wg.Add(1)
	go func() { defer sp.wg.Done(); sp.pruneLoop(config) }()
}

func (sp *storePruner) Close() {
	sp.mu.Lock()
	sp.stopped = true
	sp.cancel()
	sp.mu.Unlock()
	sp.wg.Wait()
}

func (sp *storePruner) pruneLoop(config *Config) {
	ticker := time.NewTicker(time.Minute * 5)
	defer ticker.Stop()

	for {
		select {
		case <-sp.ctx.Done():
			return
		case <-ticker.C:
		}
		sp.bnPartitionObsEntitySet.Range(func(key, value interface{}) bool {
			entity := key.(string)
			tabler := value.(schema.Tabler)

			logrus.WithField("entity", entity).Debug("Pruning archive log partitions...")

			pruned, err := sp.partitionedStore.pruneArchivePartitions(
				entity, tabler, config.MaxBnRangedArchiveLogPartitions,
			)

			if err != nil {
				logrus.WithField("entity", entity).WithError(err).Error("Failed to prune archive log partitions")
				return true
			}

			sp.bnPartitionObsEntitySet.Delete(entity)

			if len(pruned) > 0 {
				logrus.WithField("entity", entity).WithField("prunedPartitions", pruned).Info("Archive partitions pruned")

				// To minimize the db performance loss, we only remove extra archive partitions
				// for one entity at a time.
				return false
			}

			// continue to next entity
			return true
		})
	}
}
