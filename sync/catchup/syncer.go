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
	logutil "github.com/Conflux-Chain/go-conflux-util/log"
	viperutil "github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/openweb3/web3go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// Syncer accelerates core space epoch data catch-up using concurrently workers.
// Specifically, each worker will be dispatched as round-robin load balancing.
type Syncer struct {
	// goroutine workers to fetch epoch data concurrently
	workers []*worker
	// rpc clients delegated to get network status
	rpcClients []IRpcClient
	// db store to persist epoch data
	db *mysql.MysqlStore
	// min num of db rows per batch persistence
	minBatchDbRows int
	// max num of db rows collected before persistence
	maxDbRows int
	// benchmark catch-up sync performance
	benchmark bool
	// HA leader/follower election
	elm election.LeaderManager
	// sync monitor
	monitor *monitor.Monitor
	// epoch to start sync
	epochFrom uint64
	// configuration for boost mode
	boostConf boostConfig
}

// functional options for syncer
type SyncOption func(*Syncer)

func WithMinBatchDbRows(dbRows int) SyncOption {
	return func(s *Syncer) {
		s.minBatchDbRows = dbRows
	}
}

func WithMaxDbRows(dbRows int) SyncOption {
	return func(s *Syncer) {
		s.maxDbRows = dbRows
	}
}

func WithWorkers(workers []*worker) SyncOption {
	return func(s *Syncer) {
		s.workers = workers
	}
}

func WithBenchmark(benchmark bool) SyncOption {
	return func(s *Syncer) {
		s.benchmark = benchmark
	}
}

func WithBoostConfig(config boostConfig) SyncOption {
	return func(s *Syncer) {
		s.boostConf = config
	}
}

func MustNewCfxSyncer(
	clients []*sdk.Client,
	dbs *mysql.MysqlStore,
	elm election.LeaderManager,
	monitor *monitor.Monitor,
	epochFrom uint64,
	opts ...SyncOption) *Syncer {

	var conf config
	viperutil.MustUnmarshalKey("sync.catchup", &conf)

	var rpcClients []IRpcClient
	for _, cfx := range clients {
		rpcClients = append(rpcClients, NewCoreRpcClient(cfx))
	}

	var workers []*worker
	for i, nodeUrl := range conf.NodePool.Cfx { // initialize workers
		name := fmt.Sprintf("CUWorker#%v", i)
		worker := mustNewWorker(name, MustNewCoreRpcClient(nodeUrl), conf.WorkerChanSize)
		workers = append(workers, worker)
	}

	syncer, err := newSyncer(conf, rpcClients, workers, dbs, elm, monitor, epochFrom, opts...)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize CFX catch-up syncer")
	}
	return syncer
}

func MustNewEthSyncer(
	clients []*web3go.Client,
	dbs *mysql.MysqlStore,
	elm election.LeaderManager,
	monitor *monitor.Monitor,
	epochFrom uint64,
	opts ...SyncOption) *Syncer {

	var conf config
	viperutil.MustUnmarshalKey("sync.catchup", &conf)

	var rpcClients []IRpcClient
	for _, w3c := range clients {
		rpcClients = append(rpcClients, NewEvmRpcClient(w3c))
	}

	var workers []*worker
	for i, nodeUrl := range conf.NodePool.Eth { // initialize workers
		name := fmt.Sprintf("CUWorker#%v", i)
		worker := mustNewWorker(name, MustNewEvmRpcClient(nodeUrl), conf.WorkerChanSize)
		workers = append(workers, worker)
	}

	syncer, err := newSyncer(conf, rpcClients, workers, dbs, elm, monitor, epochFrom, opts...)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize ETH catch-up syncer")
	}
	return syncer
}

func newSyncer(
	conf config,
	clients []IRpcClient,
	workers []*worker,
	dbs *mysql.MysqlStore,
	elm election.LeaderManager,
	monitor *monitor.Monitor,
	epochFrom uint64,
	opts ...SyncOption) (*Syncer, error) {

	var cOpts []SyncOption
	cOpts = append(cOpts,
		WithMaxDbRows(conf.MaxDbRows),
		WithMinBatchDbRows(conf.DbRowsThreshold),
		WithWorkers(workers),
		WithBenchmark(conf.Benchmark),
		WithBoostConfig(conf.Boost),
	)
	cOpts = append(cOpts, opts...)

	syncer := &Syncer{
		elm:            elm,
		rpcClients:     clients,
		monitor:        monitor,
		epochFrom:      epochFrom,
		minBatchDbRows: 1_500,
	}
	for _, opt := range cOpts {
		opt(syncer)
	}

	// Check boost mode eligibility
	if syncer.UseBoost() {
		// Boost mode is an optimization focused solely on syncing event logs.
		// To achieve this, it requires disabling the syncing of blocks, transactions, and receipts.
		// This is because boost mode skips fetching these data types for faster event log processing.
		disabler := store.StoreConfig()
		if !disabler.IsChainBlockDisabled() || !disabler.IsChainTxnDisabled() || !disabler.IsChainReceiptDisabled() {
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

func (s *Syncer) UseBoost() bool {
	return s.boostConf.Enabled
}

func (s *Syncer) Close() error {
	for _, w := range s.workers {
		w.Close()
	}

	return s.db.Close()
}

func (s *Syncer) Sync(ctx context.Context) {
	if len(s.workers) == 0 { // no workers configured?
		logrus.Debug("Catch-up syncer skipped due to no workers configured")
		return
	}

	logrus.WithField("numWorkers", len(s.workers)).
		Debug("Catch-up syncer starting to catch up latest epoch")

	etLogger := logutil.NewErrorTolerantLogger(logutil.DefaultETConfig)
	for s.elm.Await(ctx) {
		start, end, err := s.nextSyncRange()
		if err != nil {
			etLogger.Log(
				logrus.StandardLogger(), err, "Catch-up syncer failed to get next sync range",
			)
			time.Sleep(1 * time.Second)
			continue
		}

		if start > end {
			break
		}

		s.SyncOnce(ctx, start, end)
	}
}

func (s *Syncer) SyncOnce(ctx context.Context, start, end uint64) {
	var bmarker *benchmarker
	if s.benchmark {
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

func (s *Syncer) doSync(ctx context.Context, bmarker *benchmarker, start, end uint64) {
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

func (s *Syncer) fetchResult(ctx context.Context, start, end uint64, bmarker *benchmarker) error {
	var epochData *store.EpochData
	var state persistState

	for eno := start; eno <= end; {
		for i := 0; i < len(s.workers) && eno <= end; i++ {
			w, startTime := s.workers[i], time.Now()

			select {
			case <-ctx.Done():
				return ctx.Err()
			case epochData = <-w.Data():
				if bmarker != nil {
					bmarker.metricFetchPerEpochDuration(startTime)
				}

				// collect epoch data
				eno++

				s.monitor.Update(eno)
			}

			epochDbRows, storeDbRows := state.update(epochData)

			logrus.WithFields(logrus.Fields{
				"workerName":         w.name,
				"epochNo":            epochData.Number,
				"epochDbRows":        epochDbRows,
				"storeDbRows":        storeDbRows,
				"state.insertDbRows": state.insertDbRows,
				"state.totalDbRows":  state.totalDbRows,
			}).Debug("Catch-up syncer collects new epoch data from worker")

			// Batch insert into db if enough db rows collected, also use total db rows here to
			// restrict memory usage.
			if state.insertDbRows >= s.minBatchDbRows || (s.maxDbRows > 0 && state.totalDbRows >= s.maxDbRows) {
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

type persistState struct {
	totalDbRows  int                // total db rows for collected epochs
	insertDbRows int                // total db rows to be inserted for collected epochs
	epochs       []*store.EpochData // all collected epochs
}

func (s *persistState) reset() {
	s.totalDbRows = 0
	s.insertDbRows = 0
	s.epochs = []*store.EpochData{}
}

func (s *persistState) numEpochs() int {
	return len(s.epochs)
}

func (s *persistState) update(epochData *store.EpochData) (int, int) {
	totalDbRows, storeDbRows := countDbRows(epochData)

	s.epochs = append(s.epochs, epochData)
	s.totalDbRows += totalDbRows
	s.insertDbRows += storeDbRows

	return totalDbRows, storeDbRows
}

func (s *Syncer) persist(ctx context.Context, state *persistState, bmarker *benchmarker) error {
	numEpochs := state.numEpochs()
	if numEpochs == 0 {
		return nil
	}

	start := time.Now()
	err := s.db.PushnWithFinalizer(state.epochs, func(d *gorm.DB) error {
		return s.elm.Extend(ctx)
	})

	if err != nil {
		return errors.WithMessage(err, "failed to push db store")
	}

	if bmarker != nil {
		bmarker.metricPersistDb(start, state)
	}

	return nil
}

// nextSyncRange gets the sync range by loading max epoch number from the database as the start
// and fetching the maximum epoch of the latest finalized or the latest checkpoint epoch as the end
func (s *Syncer) nextSyncRange() (uint64, uint64, error) {
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
func countDbRows(epoch *store.EpochData) (totalDbRows int, storeDbRows int) {
	storeDisabler := store.StoreConfig()

	// db rows for block
	totalDbRows += len(epoch.Blocks)
	if !storeDisabler.IsChainBlockDisabled() {
		storeDbRows += len(epoch.Blocks)
	}

	// db rows for txs
	totalDbRows += len(epoch.Receipts)
	if !storeDisabler.IsChainReceiptDisabled() || !storeDisabler.IsChainTxnDisabled() {
		storeDbRows += len(epoch.Receipts)
	}

	numLogs := 0
	for _, rcpt := range epoch.Receipts {
		numLogs += len(rcpt.Logs)
	}

	// db rows for logs
	totalDbRows += numLogs
	if !storeDisabler.IsChainLogDisabled() {
		storeDbRows += numLogs
	}

	return
}
