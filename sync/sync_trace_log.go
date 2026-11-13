package sync

import (
	"context"
	"fmt"
	gosync "sync"

	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/sync/tracelog"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	viperutil "github.com/Conflux-Chain/confura/util/viper"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/core"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/poll"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/process/db"
	"github.com/Conflux-Chain/go-conflux-util/ctxutil"
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

	syncParams := sync.LatestParamsDB[core.EpochData]{
		Adapter:   s.adapter,
		Poller:    s.conf.Poller,
		Processor: s.conf.Processor,
		DB:        s.logStore.DB(),
	}

	finalizedEpoch := latestFinalized.ToInt().Uint64()
	syncParams.Reorg, err = loadTraceReorgWindow(s.epochFrom, finalizedEpoch, s.epochBlockMapStore.LoadPivotHashes)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to restore trace reorg window")
	}

	proc := tracelog.NewProcessor(s.registry, s.logStore, s.epochBlockMapStore)
	if err := sync.StartLatestDB(ctx, wg, syncParams, proc); err != nil {
		logrus.WithError(err).Fatal("Failed to start latest trace sync")
	}
}

// loadTraceReorgWindow restores the last committed height. Finality may advance
// after catch-up; in that case the preceding committed epoch is also finalized
// and remains the anchor, so the next epoch is not skipped.
func loadTraceReorgWindow(next, finalized uint64, load func(uint64, uint64) (map[uint64]string, error)) (poll.ReorgWindowParams, error) {
	if next == 0 {
		return poll.ReorgWindowParams{}, nil
	}
	anchor := min(finalized, next-1)
	hashes, err := load(anchor, next-1)
	if err != nil {
		return poll.ReorgWindowParams{}, err
	}
	latest := make(map[uint64]string)
	for epoch := anchor; epoch < next; epoch++ {
		if hashes[epoch] == "" {
			return poll.ReorgWindowParams{}, fmt.Errorf("missing pivot hash for epoch %d", epoch)
		}
		if epoch > anchor {
			latest[epoch] = hashes[epoch]
		}
	}
	return poll.ReorgWindowParams{
		FinalizedBlockNumber: anchor,
		FinalizedBlockHash:   hashes[anchor],
		LatestBlocks:         latest,
	}, nil
}
