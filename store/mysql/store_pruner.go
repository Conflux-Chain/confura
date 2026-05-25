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
	// sources used by manual prune commands to load existing prunable entities
	prunableEntitySourceMu sync.RWMutex
	prunableEntitySources  []prunableEntitySource
}

type prunableEntity struct {
	entity string
	tabler schema.Tabler
}

type prunableEntitySource func(maxArchivePartitions uint32) ([]prunableEntity, error)

type staticTabler string

func (st staticTabler) TableName() string {
	return string(st)
}

// ArchiveLogPruneResult summarizes one manual archive log prune attempt.
type ArchiveLogPruneResult struct {
	Entity                 string
	PrunedPartitionIndexes []uint32
	Err                    error
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
		sp.registerEntity(prunableEntity{
			entity: partition.Entity,
			tabler: partition.tabler,
		})
	}
}

func (sp *storePruner) registerEntity(entity prunableEntity) {
	if len(entity.entity) == 0 || entity.tabler == nil {
		return
	}

	sp.bnPartitionObsEntitySet.Store(entity.entity, entity.tabler)
}

func (sp *storePruner) registerEntitySource(source prunableEntitySource) {
	if source == nil {
		return
	}

	sp.prunableEntitySourceMu.Lock()
	defer sp.prunableEntitySourceMu.Unlock()
	sp.prunableEntitySources = append(sp.prunableEntitySources, source)
}

func (sp *storePruner) prunePrunableEntities(maxArchivePartitions uint32) []ArchiveLogPruneResult {
	sp.prunableEntitySourceMu.RLock()
	sources := append([]prunableEntitySource(nil), sp.prunableEntitySources...)
	sp.prunableEntitySourceMu.RUnlock()

	var targets []prunableEntity
	seen := make(map[string]struct{})
	for _, source := range sources {
		entities, err := source(maxArchivePartitions)
		if err != nil {
			return []ArchiveLogPruneResult{{Err: err}}
		}

		for _, entity := range entities {
			if len(entity.entity) == 0 || entity.tabler == nil {
				continue
			}

			if _, ok := seen[entity.entity]; ok {
				continue
			}

			seen[entity.entity] = struct{}{}
			targets = append(targets, entity)
		}
	}

	results := make([]ArchiveLogPruneResult, 0, len(targets))
	for _, target := range targets {
		pruned, err := sp.partitionedStore.pruneArchivePartitions(
			target.entity, target.tabler, maxArchivePartitions,
		)

		result := ArchiveLogPruneResult{
			Entity: target.entity,
			Err:    err,
		}

		for _, partition := range pruned {
			result.PrunedPartitionIndexes = append(result.PrunedPartitionIndexes, partition.Index)
		}

		results = append(results, result)
	}

	return results
}

func (sp *storePruner) pruneObservedEntities(config *Config, oneEntityAtATime bool) {
	sp.bnPartitionObsEntitySet.Range(func(key, value interface{}) bool {
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
			if oneEntityAtATime && len(pruned) > 0 {
				return false
			}
		}

		// continue to next entity
		return true
	})
}

// schedulePrune periodically monitors and removes extra more than the max specified number of
// archive bn partitions. Be noted this function will block caller thread.
func (sp *storePruner) schedulePrune(config *Config) {
	ticker := time.NewTicker(time.Minute * 15)
	defer ticker.Stop()

	sp.pruneObservedEntities(config, false)

	for range ticker.C {
		sp.pruneObservedEntities(config, true)
	}
}
