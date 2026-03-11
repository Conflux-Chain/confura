package tracelog

import (
	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/core"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/process/db"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// BatchProcessor handles bulk log processing during the catchup phase.
type BatchProcessor struct {
	registry           *Registry
	logStore           *mysql.InternalContractLogStore
	epochBlockMapStore *mysql.CfxTraceSyncEpochBlockMapStore

	pendingLogs      []*mysql.InternalContractLog
	pendingEpochData []store.EpochData
}

func NewBatchProcessor(
	registry *Registry,
	logStore *mysql.InternalContractLogStore,
	epochBlockMapStore *mysql.CfxTraceSyncEpochBlockMapStore,
) *BatchProcessor {
	return &BatchProcessor{
		registry:           registry,
		logStore:           logStore,
		epochBlockMapStore: epochBlockMapStore,
	}
}

func (p *BatchProcessor) BatchProcess(data core.EpochData) int {
	logs, err := ParseEpochTraces(data.Traces, p.registry)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to parse epoch traces")
	}

	dbLogs, err := EnrichAndConvertLogs(logs, data.Blocks)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to convert logs")
	}

	p.pendingLogs = append(p.pendingLogs, dbLogs...)

	pivotBlock := data.Blocks[len(data.Blocks)-1]
	p.pendingEpochData = append(p.pendingEpochData, store.EpochData{
		EpochNo:   pivotBlock.EpochNumber.ToInt().Uint64(),
		PivotHash: &pivotBlock.Hash,
		Blocks:    data.Blocks,
	})

	return len(dbLogs)
}

func (p *BatchProcessor) BatchExec(tx *gorm.DB, batchSize int) error {
	if len(p.pendingLogs) > 0 {
		if err := tx.CreateInBatches(p.pendingLogs, batchSize).Error; err != nil {
			return errors.WithMessage(err, "failed to store internal contract logs")
		}
	}

	if err := p.epochBlockMapStore.Add(tx, p.pendingEpochData); err != nil {
		return errors.WithMessage(err, "failed to store epoch block mappings")
	}

	return nil
}

func (p *BatchProcessor) BatchReset() {
	p.pendingLogs = nil
	p.pendingEpochData = nil
}

// OperationFunc adapts a function to the db.Operation interface.
type OperationFunc func(tx *gorm.DB) error

func (f OperationFunc) Exec(tx *gorm.DB) error { return f(tx) }

// Processor handles per-epoch log processing during the latest-following phase.
type Processor struct {
	registry           *Registry
	logStore           *mysql.InternalContractLogStore
	epochBlockMapStore *mysql.CfxTraceSyncEpochBlockMapStore
}

func NewProcessor(
	registry *Registry,
	logStore *mysql.InternalContractLogStore,
	epochBlockMapStore *mysql.CfxTraceSyncEpochBlockMapStore,
) *Processor {
	return &Processor{
		registry:           registry,
		logStore:           logStore,
		epochBlockMapStore: epochBlockMapStore,
	}
}

func (p *Processor) Process(data core.EpochData) db.Operation {
	logs, err := ParseEpochTraces(data.Traces, p.registry)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to parse epoch traces")
	}

	dbLogs, err := EnrichAndConvertLogs(logs, data.Blocks)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to convert logs")
	}

	pivotBlock := data.Blocks[len(data.Blocks)-1]
	epochData := store.EpochData{
		EpochNo:   pivotBlock.EpochNumber.ToInt().Uint64(),
		PivotHash: &pivotBlock.Hash,
		Blocks:    data.Blocks,
	}

	return OperationFunc(func(tx *gorm.DB) error {
		if len(dbLogs) > 0 {
			if err := tx.Create(dbLogs).Error; err != nil {
				return errors.WithMessage(err, "failed to store internal contract logs")
			}
		}
		if err := p.epochBlockMapStore.Add(tx, []store.EpochData{epochData}); err != nil {
			return errors.WithMessage(err, "failed to store epoch block mappings")
		}
		return nil
	})
}

func (p *Processor) Revert(data core.EpochData) db.Operation {
	return OperationFunc(func(tx *gorm.DB) error {
		pivotBlock := data.Blocks[len(data.Blocks)-1]
		epochNum := pivotBlock.EpochNumber.ToInt().Uint64()

		if err := p.logStore.Pop(tx, epochNum); err != nil {
			return errors.WithMessage(err, "failed to pop internal contract logs")
		}
		if err := p.epochBlockMapStore.Pop(tx, epochNum); err != nil {
			return errors.WithMessage(err, "failed to pop epoch block mappings")
		}
		return nil
	})
}
