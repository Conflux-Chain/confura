package sync

import (
	"context"
	gosync "sync"

	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/sync/tracelog"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/core"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/poll"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/process/db"
	"github.com/Conflux-Chain/go-conflux-util/ctxutil"
	viperutil "github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/sirupsen/logrus"
)

type traceLogSyncConfig struct {
	CatchUp struct {
		Poller    poll.CatchUpOption
		Processor db.BatchOption
	}

	Poller    poll.Option
	Processor db.Option
}

// TraceLogSyncer orchestrates the synchronization of internal contract trace logs.
type TraceLogSyncer struct {
	conf traceLogSyncConfig

	epochFrom uint64
	adapter   poll.Adapter[core.EpochData]
	client    *sdk.Client

	registry           *tracelog.Registry
	epochBlockMapStore *mysql.CfxTraceSyncEpochBlockMapStore
	logStore           *mysql.InternalContractLogStore
}

// MustNewTraceLogSyncer creates a new TraceLogSyncer, panicking on any initialization error.
func MustNewTraceLogSyncer(clients []*sdk.Client, store *mysql.CfxStore) *TraceLogSyncer {
	if len(clients) == 0 {
		logrus.Fatal("No SDK client provided")
	}

	var conf cfxSyncConfig
	viperutil.MustUnmarshalKey("sync.cfx", &conf)

	adapter, err := core.NewAdapterWithConfig(core.AdapterConfig{
		URL: clients[0].GetNodeURL(),
		AdapterOption: core.AdapterOption{
			RequestTimeout: rpcutil.DefaultCfxClientConfig().RequestTimeout,
			IgnoreReceipts: true,
		},
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create RPC adapter")
	}

	registry, err := tracelog.NewRegistry(clients[0])
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create internal contract registry")
	}

	epochBlockMapStore := mysql.NewCfxTraceSyncEpochBlockMapStore(store)
	maxEpoch, ok, err := epochBlockMapStore.MaxEpoch()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to get max epoch")
	}

	var epochFrom uint64
	if !ok { // start from configured start epoch if not found
		epochFrom = conf.FromEpoch
	} else {
		epochFrom = maxEpoch + 1
	}

	return &TraceLogSyncer{
		conf:               conf.TraceLog,
		adapter:            adapter,
		epochFrom:          epochFrom,
		client:             clients[0],
		registry:           registry,
		epochBlockMapStore: epochBlockMapStore,
		logStore:           mysql.NewInternalContractLogStore(store.DB()),
	}
}

// MustSync starts the synchronization process: first catches up, then follows latest.
func (s *TraceLogSyncer) MustSync(ctx context.Context, wg *gosync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	// Phase 1: Catchup
	params := sync.CatchupParamsDB[core.EpochData]{
		Adapter:         s.adapter,
		Poller:          s.conf.CatchUp.Poller,
		Processor:       s.conf.CatchUp.Processor,
		DB:              s.logStore.DB(),
		NextBlockNumber: s.epochFrom,
	}

	bp := tracelog.NewBatchProcessor(s.registry, s.logStore, s.epochBlockMapStore)
	s.epochFrom = sync.CatchUpDB[core.EpochData](ctx, params, bp)

	if ctxutil.IsDone(ctx) {
		logrus.Info("Trace log catchup sync interrupted")
		return
	}

	// Phase 2: Follow latest
	latestFinalized, err := s.client.GetEpochNumber(types.EpochLatestFinalized)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to get latest finalized epoch")
	}

	syncParams := sync.ParamsDB[core.EpochData]{
		Adapter:         s.adapter,
		Poller:          s.conf.Poller,
		Processor:       s.conf.Processor,
		DB:              s.logStore.DB(),
		NextBlockNumber: s.epochFrom,
	}

	finalizedEpoch := latestFinalized.ToInt().Uint64()
	if s.epochFrom > finalizedEpoch {
		pivotHashes, err := s.epochBlockMapStore.LoadPivotHashes(finalizedEpoch, s.epochFrom-1)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to load pivot hashes for reorg window")
		}

		for epoch := finalizedEpoch; epoch < s.epochFrom; epoch++ {
			if _, ok := pivotHashes[epoch]; !ok {
				logrus.WithField("missingEpoch", epoch).Fatal("Missing pivot hash in reorg window")
			}
		}

		syncParams.Reorg = poll.ReorgWindowParams{
			FinalizedBlockNumber: finalizedEpoch,
			FinalizedBlockHash:   pivotHashes[finalizedEpoch],
			LatestBlocks:         pivotHashes,
		}
	}

	proc := tracelog.NewProcessor(s.registry, s.logStore, s.epochBlockMapStore)
	sync.StartLatestDB(ctx, wg, syncParams, proc)
}
