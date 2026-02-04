package mysql

import (
	"context"
	"slices"
	"sort"
	"time"

	"github.com/Conflux-Chain/confura/store"
	citypes "github.com/Conflux-Chain/confura/types"
	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type CommonStores struct {
	*confStore
	*UserStore
	*RateLimitStore
	*NodeRouteStore
	*VirtualFilterLogStore
}

func newCommonStores(db *gorm.DB) *CommonStores {
	return &CommonStores{
		confStore:             newConfStore(db),
		UserStore:             newUserStore(db),
		RateLimitStore:        NewRateLimitStore(db),
		VirtualFilterLogStore: NewVirtualFilterLogStore(db),
		NodeRouteStore:        NewNodeRouteStore(db),
	}
}

// MysqlStore aggregation store for chain data persistence operation.
type MysqlStore[T store.ChainData] struct {
	*baseStore
	*CommonStores
	*epochBlockMapStore[T]
	*txStore[T]
	*blockStore[T]
	ls   *logStore[T]
	ails *AddressIndexedLogStore[T]
	bcls *bigContractLogStore[T]
	cs   *ContractStore
	tils *TopicIndexedLogStore[T]
	btls *bigTopicLogStore[T]
	ts   *TopicStore

	// config
	config *Config
	// chain data filter
	filter store.ChainDataFilter
	// store pruner
	pruner *storePruner
}

func NewMysqlStore[T store.ChainData](db *gorm.DB, config *Config, filter store.ChainDataFilter) *MysqlStore[T] {
	pruner := newStorePruner(db)
	cs := NewContractStore(db)
	ts := NewTopicStore(db)
	ebms := newEpochBlockMapStore[T](db, config)
	ails := NewAddressIndexedLogStore[T](db, cs, ts, config.AddressIndexedLogPartitions)
	tils := NewTopicIndexedLogStore[T](db, ts, config.TopicIndexedLogPartitions)

	return &MysqlStore[T]{
		epochBlockMapStore: ebms,
		baseStore:          newBaseStore(db),
		CommonStores:       newCommonStores(db),
		txStore:            newTxStore[T](db),
		blockStore:         newBlockStore[T](db),
		ls:                 newLogStore(db, cs, ebms, pruner.newBnPartitionObsChan),
		bcls:               newBigContractLogStore(db, cs, ts, ebms, ails, pruner.newBnPartitionObsChan),
		btls:               newBigTopicLogStore(db, ts, ebms, tils, pruner.newBnPartitionObsChan),
		ails:               ails,
		cs:                 cs,
		tils:               tils,
		ts:                 ts,
		config:             config,
		filter:             filter,
		pruner:             pruner,
	}
}

// Clone creates a new store with a deep copy of the underlying gorm db instance,
// including a new, independent database connection.
//
// Closing the cloned store will only close its own connection and will not
// affect the original store.
//
// The returned store will also have the same disabler as the current store.
func (ms *MysqlStore[T]) Clone() (*MysqlStore[T], error) {
	newDb, err := gorm.Open(ms.DB().Dialector, ms.DB().Config)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to open gorm db connection")
	}
	conf := *ms.config
	return NewMysqlStore[T](newDb, &conf, ms.filter), nil
}

func (ms *MysqlStore[T]) Push(data T) error {
	return ms.Pushn([]T{data})
}

func (ms *MysqlStore[T]) Pushn(dataSlice []T) error {
	return ms.PushnWithFinalizer(dataSlice, nil)
}

// PushnWithFinalizer saves multiple epoch data into db with an extra finalizer to commit or rollback the transaction.
func (ms *MysqlStore[T]) PushnWithFinalizer(dataSlice []T, finalizer func(*gorm.DB) error) error {
	if len(dataSlice) == 0 {
		return nil
	}

	storeMaxEpoch, ok, err := ms.MaxEpoch()
	if err != nil {
		return err
	}

	if !ok {
		storeMaxEpoch = citypes.EpochNumberNil
	}

	if err := store.RequireContinuous(dataSlice, storeMaxEpoch); err != nil {
		return err
	}

	startTime := time.Now()
	defer metrics.Registry.Store.Push("mysql").UpdateSince(startTime)

	// the log partition to write universal event logs
	var logPartition bnPartition
	// the log partition to write event logs for specified big contract
	var contract2BnPartitions map[uint64]bnPartition
	// the log partition to write event logs for specified big topic
	var topic2BnPartitions map[uint64]bnPartition

	if !ms.filter.IsLogDisabled() {
		// add log contract address
		// Note, even if failed to insert event logs afterward, no need to rollback the inserted contract records.
		_, err := ms.cs.AddContract(extractUniqueContractAddresses(dataSlice...))
		if err != nil {
			return errors.WithMessage(err, "failed to add contracts for specified epoch data slice")
		}

		// add log topic hash
		// Note, even if failed to insert event logs afterward, no need to rollback the inserted topic records.
		_, err = ms.ts.BatchAdd(extractUniqueTopic0Signatures(dataSlice...))
		if err != nil {
			return errors.WithMessage(err, "failed to add topics for specified epoch data slice")
		}

		// prepare for big contract log partitions if necessary
		contract2BnPartitions, err = ms.bcls.preparePartitions(dataSlice)
		if err != nil {
			return errors.WithMessage(err, "failed to prepare big contract log partitions")
		}

		// prepare for big topic log partitions if necessary
		topic2BnPartitions, err = ms.btls.preparePartitions(dataSlice)
		if err != nil {
			return errors.WithMessage(err, "failed to prepare big topic log partitions")
		}

		// prepare for new log partitions if necessary before saving epoch data
		if logPartition, err = ms.ls.preparePartition(); err != nil {
			return errors.WithMessage(err, "failed to prepare log partition")
		}
	}

	// prepare epoch to block mapping table partition if necessary
	if ms.epochBlockMapStore.preparePartition(dataSlice) != nil {
		return errors.New("failed to prepare epoch block map partition")
	}

	return ms.baseStore.db.Transaction(func(dbTx *gorm.DB) error {
		if !ms.filter.IsBlockDisabled() {
			// save blocks
			if err := ms.blockStore.Add(dbTx, dataSlice); err != nil {
				return errors.WithMessagef(err, "failed to save blocks")
			}
		}

		skipTxn := ms.filter.IsTxnDisabled()
		skipRcpt := ms.filter.IsReceiptDisabled()
		if !skipRcpt || !skipTxn {
			// save transactions or receipts
			if err := ms.txStore.Add(dbTx, dataSlice, skipTxn, skipRcpt); err != nil {
				return errors.WithMessage(err, "failed to save transactions")
			}
		}

		if !ms.filter.IsLogDisabled() {
			bigContractIds := make(map[uint64]bool, len(contract2BnPartitions))
			for cid := range contract2BnPartitions {
				bigContractIds[cid] = true
			}

			// save address indexed event logs
			if err := ms.ails.Add(dbTx, dataSlice, bigContractIds); err != nil {
				return errors.WithMessage(err, "failed to save address indexed event logs")
			}

			// save contract specified event logs
			if err := ms.bcls.Add(dbTx, dataSlice, contract2BnPartitions); err != nil {
				return errors.WithMessage(err, "failed to save big contract logs")
			}

			bigTopics := make(map[uint64]bool, len(topic2BnPartitions))
			for tid := range topic2BnPartitions {
				bigTopics[tid] = true
			}

			// save topic indexed event logs
			if err := ms.tils.Add(dbTx, dataSlice, bigTopics); err != nil {
				return errors.WithMessage(err, "failed to save topic indexed event logs")
			}

			// save big topic specified event logs
			if err := ms.btls.Add(dbTx, dataSlice, topic2BnPartitions); err != nil {
				return errors.WithMessage(err, "failed to save big topic logs")
			}

			// save event logs
			if err := ms.ls.Add(dbTx, dataSlice, logPartition); err != nil {
				return errors.WithMessage(err, "failed to save event logs")
			}
		}

		// save epoch to block mapping data
		if err := ms.epochBlockMapStore.Add(dbTx, dataSlice); err != nil {
			return errors.WithMessage(err, "failed to save epoch to block mapping data")
		}

		if finalizer != nil {
			return finalizer(dbTx)
		}

		return nil
	})
}

// Popn pops multiple epoch data from database.
func (ms *MysqlStore[T]) Popn(epochUntil uint64) error {
	return ms.PopnWithFinalizer(epochUntil, nil)
}

// PopnWithFinalizer pops multiple epoch data from database with an extra finalizer to commit or rollback the transaction.
func (ms *MysqlStore[T]) PopnWithFinalizer(epochUntil uint64, finalizer func(*gorm.DB) error) error {
	maxEpoch, ok, err := ms.MaxEpoch()
	if err != nil {
		return errors.WithMessage(err, "failed to get max epoch")
	}

	if !ok || epochUntil > maxEpoch { // no data in database or popped beyond the max epoch
		return nil
	}

	startTime := time.Now()
	defer metrics.Registry.Store.Pop("mysql").UpdateSince(startTime)

	return ms.baseStore.db.Transaction(func(dbTx *gorm.DB) error {
		if !ms.filter.IsBlockDisabled() {
			// remove blocks
			if err := ms.blockStore.Remove(dbTx, epochUntil, maxEpoch); err != nil {
				return errors.WithMessage(err, "failed to remove blocks")
			}
		}

		skipTxn := ms.filter.IsTxnDisabled()
		skipRcpt := ms.filter.IsReceiptDisabled()
		if !skipRcpt || !skipTxn {
			// remove transactions or receipts
			if err := ms.txStore.Remove(dbTx, epochUntil, maxEpoch); err != nil {
				return errors.WithMessage(err, "failed to remove transactions")
			}
		}

		if !ms.filter.IsLogDisabled() {
			// remove address indexed event logs
			if err := ms.ails.DeleteAddressIndexedLogs(dbTx, epochUntil, maxEpoch); err != nil {
				return errors.WithMessage(err, "failed to remove address indexed event logs")
			}

			if err := ms.bcls.Popn(dbTx, epochUntil); err != nil {
				return errors.WithMessage(err, "failed to remove big contract logs")
			}

			// remove topic indexed event logs
			if err := ms.tils.DeleteTopicIndexedLogs(dbTx, epochUntil, maxEpoch); err != nil {
				return errors.WithMessage(err, "failed to remove topic indexed event logs")
			}

			if err := ms.btls.Popn(dbTx, epochUntil); err != nil {
				return errors.WithMessage(err, "failed to remove big topic logs")
			}

			// pop universal event logs
			if err := ms.ls.Popn(dbTx, epochUntil); err != nil {
				return errors.WithMessage(err, "failed to remove universal event logs")
			}
		}

		// remove epoch to block mapping data
		if err := ms.epochBlockMapStore.Remove(dbTx, epochUntil, maxEpoch); err != nil {
			return errors.WithMessage(err, "failed to remove epoch to block mapping data")
		}

		// pop is always due to pivot chain switch, update reorg version too
		if err := ms.confStore.createOrUpdateReorgVersion(dbTx); err != nil {
			return errors.WithMessage(err, "failed to update reorg version")
		}

		if finalizer != nil {
			return finalizer(dbTx)
		}

		return nil
	})
}

func (ms *MysqlStore[T]) GetLogs(ctx context.Context, storeFilter store.LogFilter) ([]*store.Log, error) {
	startTime := time.Now()
	defer metrics.Registry.Store.GetLogs().UpdateSince(startTime)

	// if address specified, query from address indexed or big contract event log table
	if !storeFilter.Contracts.IsNull() {
		contracts := storeFilter.Contracts.ToSlice()
		slices.Sort(contracts)

		return ms.getContractLogs(ctx, contracts, storeFilter)
	}

	// if topic0 specified, query from topic indexed or big topic event log table
	if len(storeFilter.Topics) > 0 && !storeFilter.Topics[0].IsNull() {
		topics := storeFilter.Topics[0].ToSlice()
		slices.Sort(topics)

		return ms.getTopicLogs(ctx, topics, storeFilter)
	}

	// otherwise, query from universal event log table
	return ms.ls.GetLogs(ctx, storeFilter)
}

func (ms *MysqlStore[T]) getTopicLogs(ctx context.Context, topics []string, storeFilter store.LogFilter) ([]*store.Log, error) {
	var result []*store.Log

	for _, topic := range topics {
		// convert topic hash to id
		tid, exists, err := ms.ts.GetID(topic)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to get topic id")
		}

		if !exists {
			continue
		}

		// check if the topic is a big topic or not
		isBigTopic, err := ms.btls.IsBigTopic(tid)
		if err != nil {
			return nil, err
		}

		// if the topic is a big topic, find the event logs from separate table.
		if isBigTopic {
			logs, err := ms.btls.GetTopicLogs(ctx, tid, topic, storeFilter)
			if err != nil {
				return nil, err
			}

			result = append(result, logs...)

			// check log count
			if store.IsBoundChecksEnabled(ctx) && len(result) > int(store.MaxLogLimit) {
				return nil, newSuggestedFilterResultSetTooLargeError(&storeFilter, result, false)
			}

			continue
		}

		// check timeout before query
		select {
		case <-ctx.Done():
			return nil, store.ErrGetLogsTimeout
		default:
		}

		// query from topic indexed logs
		logs, err := ms.tils.GetTopicIndexedLogs(ctx, tid, topic, storeFilter)
		if err != nil {
			return nil, err
		}

		result = append(result, logs...)

		// check log count
		if store.IsBoundChecksEnabled(ctx) && len(result) > int(store.MaxLogLimit) {
			return nil, newSuggestedFilterResultSetTooLargeError(&storeFilter, result, false)
		}
	}

	// merge && sort log result
	sort.Sort(store.LogSlice(result))
	return result, nil
}

func (ms *MysqlStore[T]) getContractLogs(ctx context.Context, contracts []string, storeFilter store.LogFilter) ([]*store.Log, error) {
	var result []*store.Log

	for _, addr := range contracts {
		// convert contract address to id
		cid, exists, err := ms.cs.GetContractIdByAddress(addr)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to get contract id")
		}

		if !exists {
			continue
		}

		// check if the contract is a big contract or not
		isBigContract, err := ms.bcls.IsBigContract(cid)
		if err != nil {
			return nil, err
		}

		// if the contract is a big contract, find the event logs from separate table.
		if isBigContract {
			logs, err := ms.bcls.GetContractLogs(ctx, cid, storeFilter)
			if err != nil {
				return nil, err
			}

			result = append(result, logs...)

			// check log count
			if store.IsBoundChecksEnabled(ctx) && len(result) > int(store.MaxLogLimit) {
				return nil, newSuggestedFilterResultSetTooLargeError(&storeFilter, result, false)
			}

			continue
		}

		// check timeout before query
		select {
		case <-ctx.Done():
			return nil, store.ErrGetLogsTimeout
		default:
		}

		// query from address indexed logs
		logs, err := ms.ails.GetAddressIndexedLogs(ctx, cid, addr, storeFilter)
		if err != nil {
			return nil, err
		}

		result = append(result, logs...)

		// check log count
		if store.IsBoundChecksEnabled(ctx) && len(result) > int(store.MaxLogLimit) {
			return nil, newSuggestedFilterResultSetTooLargeError(&storeFilter, result, false)
		}
	}

	// merge && sort log result
	sort.Sort(store.LogSlice(result))
	return result, nil
}

// Prune prune data from db store.
func (ms *MysqlStore[T]) Prune() {
	go ms.pruner.schedulePrune(ms.config)
}

// SetTxnBatchSize sets the transaction batch size for db insertion.
func (ms *MysqlStore[T]) SetTxnBatchSize(size int) {
	ms.config.CreateBatchSize = size
	ms.DB().CreateBatchSize = size
}

// newSuggestedFilterResultSetTooLargeError returns an error indicating that the filter result set is too large.
// It suggests a narrower block range to reduce the size of the result set if possible.
//
// Parameters:
// - filter: the log filter used for querying logs.
// - resultLogs: the list of logs retrieved from the query, make sure it is more than `store.MaxLogLimit` long.
// - sorted: whether the logs are already sorted by block number.
func newSuggestedFilterResultSetTooLargeError(filter *store.LogFilter, resultLogs []*store.Log, sorted bool) error {
	// Ensure logs are sorted by block number if not already sorted.
	if !sorted {
		sort.Sort(store.LogSlice(resultLogs))
	}

	// Determine if we need to suggest a narrower block range based on the exceeding log entry
	var suggestedBlockRange *store.SuggestedBlockRange
	if exceedingLog := resultLogs[store.MaxLogLimit]; exceedingLog.BlockNumber > filter.BlockFrom {
		blockRange := store.NewSuggestedBlockRange(filter.BlockFrom, exceedingLog.BlockNumber-1, exceedingLog.Epoch)
		suggestedBlockRange = &blockRange
	}

	return store.NewSuggestedFilterResultSetTooLargeError(suggestedBlockRange)
}
