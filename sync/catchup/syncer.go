package catchup

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/sync/election"
	"github.com/Conflux-Chain/confura/sync/monitor"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	viperutil "github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/openweb3/web3go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// Syncer accelerates core space epoch data catch-up using concurrently workers.
// Specifically, each worker will be dispatched as round-robin load balancing.
type Syncer[T store.ChainData] struct {
	config
	// goroutine workers to fetch epoch data concurrently
	workers []*worker[T]
	// rpc clients delegated to get network status
	rpcClients []IRpcClient[T]
	// db store to persist epoch data
	db *mysql.MysqlStore[T]
	// HA leader/follower election
	elm election.LeaderManager
	// sync monitor
	monitor *monitor.Monitor
	// epoch to start sync
	epochFrom uint64
	// chain data filter
	filter store.ChainDataFilter
}

func MustNewCfxSyncer(
	clients []*sdk.Client,
	dbs *mysql.CfxStore,
	elm election.LeaderManager,
	monitor *monitor.Monitor,
	epochFrom uint64,
) *Syncer[*store.EpochData] {
	var conf config
	viperutil.MustUnmarshalKey("sync.catchup", &conf)

	var rpcClients []IRpcClient[*store.EpochData]
	for _, cfx := range clients {
		rpcClients = append(rpcClients, NewCoreRpcClient(cfx))
	}

	var workers []*worker[*store.EpochData]
	for i, nodeUrl := range conf.NodePool.Cfx { // initialize workers
		name := fmt.Sprintf("CUWorker#%v", i)
		worker := mustNewWorker(name, MustNewCoreRpcClient(nodeUrl), conf.WorkerChanSize)
		workers = append(workers, worker)
	}

	syncer, err := newSyncer(
		conf, rpcClients, workers, dbs.MysqlStore, elm, monitor, epochFrom, store.StoreConfig(),
	)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize CFX catch-up syncer")
	}
	return syncer
}

func MustNewEthSyncer(
	clients []*web3go.Client,
	dbs *mysql.EthStore,
	elm election.LeaderManager,
	monitor *monitor.Monitor,
	epochFrom uint64,
) *Syncer[*store.EthData] {
	var conf config
	viperutil.MustUnmarshalKey("sync.catchup", &conf)

	var rpcClients []IRpcClient[*store.EthData]
	for _, w3c := range clients {
		rpcClients = append(rpcClients, NewEvmRpcClient(w3c))
	}

	var workers []*worker[*store.EthData]
	for i, nodeUrl := range conf.NodePool.Eth { // initialize workers
		name := fmt.Sprintf("CUWorker#%v", i)
		worker := mustNewWorker(name, MustNewEvmRpcClient(nodeUrl), conf.WorkerChanSize)
		workers = append(workers, worker)
	}

	syncer, err := newSyncer(
		conf, rpcClients, workers, dbs.MysqlStore, elm, monitor, epochFrom, store.EthStoreConfig(),
	)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize ETH catch-up syncer")
	}
	return syncer
}

func newSyncer[T store.ChainData](
	conf config,
	clients []IRpcClient[T],
	workers []*worker[T],
	dbs *mysql.MysqlStore[T],
	elm election.LeaderManager,
	monitor *monitor.Monitor,
	epochFrom uint64,
	filter store.ChainDataFilter,
) (*Syncer[T], error) {
	syncer := &Syncer[T]{
		config:     conf,
		elm:        elm,
		rpcClients: clients,
		monitor:    monitor,
		epochFrom:  epochFrom,
		workers:    workers,
		filter:     filter,
	}

	// Check boost mode eligibility
	if syncer.UseBoost() {
		// Boost mode is an optimization focused solely on syncing event logs.
		// To achieve this, it requires disabling the syncing of blocks, transactions, and receipts.
		// This is because boost mode skips fetching these data types for faster event log processing.
		if !filter.IsBlockDisabled() || !filter.IsTxnDisabled() || !filter.IsReceiptDisabled() {
			return nil, errors.New("boost mode is incompatible with syncing data types other than event logs")
		}
	}

	// Clone a new db store to maximize txn batch size
	newDbs, err := dbs.Clone()
	if err != nil {
		return nil, errors.WithMessage(err, "failed to clone db store")
	}
	newDbs.SetTxnBatchSize(conf.MaxDbRows)
	syncer.db = newDbs

	return syncer, nil
}

func (s *Syncer[T]) UseBoost() bool {
	return s.Boost.Enabled
}

func (s *Syncer[T]) Close() error {
	for _, w := range s.workers {
		w.Close()
	}

	return s.db.Close()
}

func (s *Syncer[T]) Sync(ctx context.Context) {
	if len(s.workers) == 0 { // no workers configured?
		logrus.Debug("Catch-up syncer skipped due to no workers configured")
		return
	}

	logrus.WithField("numWorkers", len(s.workers)).
		Debug("Catch-up syncer starting to catch up latest epoch")

	for s.elm.Await(ctx) {
		start, end, err := s.nextSyncRange()
		if err != nil {
			logrus.WithError(err).Info("Catch-up syncer failed to get next sync range")
			time.Sleep(1 * time.Second)
			continue
		}

		if start > end {
			break
		}

		s.SyncOnce(ctx, start, end)
	}
}

func (s *Syncer[T]) SyncOnce(ctx context.Context, start, end uint64) {
	var bmarker *benchmarker
	if s.Benchmark {
		bmarker = newBenchmarker()

		bmarker.markStart()
		defer func() {
			bmarker.report(start, end)
		}()
	}

	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		logrus.WithFields(logrus.Fields{
			"rangeStart":   start,
			"rangeEnd":     end,
			"boostEnabled": s.UseBoost(),
		}).Debug("Catch-up syncer is synchronizing by range...")
	}

	if s.UseBoost() {
		newBoostSyncer(s).doSync(ctx, bmarker, start, end)
	} else {
		s.doSync(ctx, bmarker, start, end)
	}
}

func (s *Syncer[T]) doSync(ctx context.Context, bmarker *benchmarker, start, end uint64) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()

		err := s.fetchResult(ctx, start, end, bmarker)
		if err != nil && !errors.Is(err, context.Canceled) {
			if errors.Is(err, store.ErrLeaderRenewal) {
				logrus.WithFields(logrus.Fields{
					"start":          start,
					"end":            end,
					"leaderIdentity": s.elm.Identity(),
				}).Info("Catch-up syncer failed to renew leadership on persisting epoch data")
			} else {
				logrus.WithFields(logrus.Fields{
					"start": start,
					"end":   end,
				}).WithError(err).Error("Catch-up syncer failed to fetch result")
			}
		}
	}()

	for i, w := range s.workers {
		wstart := start + uint64(i)
		stepN := uint64(len(s.workers))

		wg.Add(1)
		go w.Sync(ctx, &wg, wstart, end, stepN)
	}

	wg.Wait()
}

func (s *Syncer[T]) fetchResult(ctx context.Context, start, end uint64, bmarker *benchmarker) error {
	var data T
	var state persistState[T]

	for eno := start; eno <= end; {
		for i := 0; i < len(s.workers) && eno <= end; i++ {
			w, startTime := s.workers[i], time.Now()

			select {
			case <-ctx.Done():
				return ctx.Err()
			case data = <-w.Data():
				if bmarker != nil {
					bmarker.metricFetchPerEpochDuration(startTime)
				}

				// collect epoch data
				eno++

				s.monitor.Update(eno)
			}

			epochDbRows, storeDbRows := state.update(s.filter, data)

			logrus.WithFields(logrus.Fields{
				"workerName":         w.name,
				"epochNo":            data.Number,
				"epochDbRows":        epochDbRows,
				"storeDbRows":        storeDbRows,
				"state.insertDbRows": state.insertDbRows,
				"state.totalDbRows":  state.totalDbRows,
			}).Debug("Catch-up syncer collects new epoch data from worker")

			// Batch insert into db if enough db rows collected, also use total db rows here to
			// restrict memory usage.
			if state.insertDbRows >= s.DbRowsThreshold || (s.MaxDbRows > 0 && state.totalDbRows >= s.MaxDbRows) {
				err := s.persist(ctx, &state, bmarker)
				if err != nil {
					return err
				}

				state.reset()
			}
		}
	}

	// do last db write anyway since there may be some epochs not persisted yet.
	return s.persist(ctx, &state, bmarker)
}

type persistState[T store.ChainData] struct {
	totalDbRows  int // total db rows for collected epochs
	insertDbRows int // total db rows to be inserted for collected epochs
	chainData    []T // all collected block chain data
}

func (s *persistState[T]) reset() {
	s.totalDbRows = 0
	s.insertDbRows = 0
	s.chainData = []T{}
}

func (s *persistState[T]) numEpochs() int {
	return len(s.chainData)
}

func (s *persistState[T]) update(filter store.ChainDataFilter, data T) (int, int) {
	totalDbRows, storeDbRows := countDbRows(filter, data)

	s.chainData = append(s.chainData, data)
	s.totalDbRows += totalDbRows
	s.insertDbRows += storeDbRows

	return totalDbRows, storeDbRows
}

func (s *Syncer[T]) persist(ctx context.Context, state *persistState[T], bmarker *benchmarker) error {
	numEpochs := state.numEpochs()
	if numEpochs == 0 {
		return nil
	}

	start := time.Now()
	err := s.db.PushnWithFinalizer(state.chainData, func(d *gorm.DB) error {
		return s.elm.Extend(ctx)
	})

	if err != nil {
		return errors.WithMessage(err, "failed to push db store")
	}

	if bmarker != nil {
		bmarker.metricPersistDb(start, state.insertDbRows, numEpochs)
	}

	return nil
}

// nextSyncRange gets the sync range by loading max epoch number from the database as the start
// and fetching the maximum epoch of the latest finalized or the latest checkpoint epoch as the end
func (s *Syncer[T]) nextSyncRange() (uint64, uint64, error) {
	start, ok, err := s.db.MaxEpoch()
	if err != nil {
		return 0, 0, errors.WithMessage(err, "failed to get max epoch from epoch to block mapping")
	}

	if ok {
		start++
	} else {
		start = s.epochFrom
	}

	var retErr error
	for _, cli := range s.rpcClients {
		status, err := cli.GetFinalizationStatus(context.Background())
		if err == nil {
			end := max(status.LatestFinalized, status.LatestCheckpoint)
			return start, uint64(end), nil
		}
		retErr = err
	}
	return 0, 0, errors.WithMessage(retErr, "failed to get network status")
}

// countDbRows count total db rows and to be stored db row from epoch data.
func countDbRows[T store.ChainData](filter store.ChainDataFilter, data T) (totalDbRows int, storeDbRows int) {
	// db rows for block
	blocks := data.ExtractBlocks()
	totalDbRows += len(blocks)
	if !filter.IsBlockDisabled() {
		storeDbRows += len(blocks)
	}

	// db rows for txs
	receipts := data.ExtractReceipts()
	totalDbRows += len(receipts)
	if !filter.IsReceiptDisabled() || !filter.IsTxnDisabled() {
		storeDbRows += len(receipts)
	}

	numLogs := 0
	for _, rcpt := range receipts {
		numLogs += len(rcpt.Logs())
	}

	// db rows for logs
	totalDbRows += numLogs
	if !filter.IsLogDisabled() {
		storeDbRows += numLogs
	}

	return
}
