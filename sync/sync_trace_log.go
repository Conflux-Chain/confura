package sync

import (
	"context"
	gosync "sync"

	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/sync/tracelog"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/core"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/poll"
	"github.com/sirupsen/logrus"
)

// TraceLogSyncer orchestrates the synchronization of internal contract trace logs.
type TraceLogSyncer struct {
	epochFrom uint64
	adapter   *core.Adapter
	clients   []*sdk.Client

	registry           *tracelog.Registry
	syncStatusStore    *mysql.SyncStatusStore
	epochBlockMapStore *mysql.CfxTraceSyncEpochBlockMapStore
	logStore           *mysql.InternalContractLogStore
}

// MustNewTraceLogSyncer creates a new TraceLogSyncer, panicking on any initialization error.
func MustNewTraceLogSyncer(clients []*sdk.Client, store *mysql.CfxStore) *TraceLogSyncer {
	if len(clients) == 0 {
		logrus.Fatal("No SDK client provided")
	}

	adapter, err := core.NewAdapterWithConfig(core.AdapterConfig{
		URL:           clients[0].GetNodeURL(),
		AdapterOption: core.AdapterOption{IgnoreReceipts: true},
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create RPC adapter")
	}

	registry, err := tracelog.NewRegistry(clients[0])
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create internal contract registry")
	}

	epochBlockMapStore := mysql.NewCfxTraceSyncEpochBlockMapStore(store)
	epochFrom, _, err := epochBlockMapStore.MaxEpoch()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to get max epoch")
	}

	return &TraceLogSyncer{
		adapter:            adapter,
		epochFrom:          epochFrom,
		clients:            clients,
		registry:           registry,
		epochBlockMapStore: epochBlockMapStore,
		syncStatusStore:    mysql.NewSyncStatusStore(store.DB()),
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
		DB:              s.syncStatusStore.DB(),
		NextBlockNumber: s.epochFrom,
	}
	bp := tracelog.NewBatchProcessor(s.registry, s.logStore, s.epochBlockMapStore)
	s.epochFrom = sync.CatchUpDB[core.EpochData](ctx, params, bp)

	// Phase 2: Follow latest
	latestFinalized, err := s.clients[0].GetEpochNumber(types.EpochLatestFinalized)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to get latest finalized epoch")
	}

	syncParams := sync.ParamsDB[core.EpochData]{
		Adapter:         s.adapter,
		DB:              s.syncStatusStore.DB(),
		NextBlockNumber: s.epochFrom,
	}

	finalizedEpoch := latestFinalized.ToInt().Uint64()
	if s.epochFrom >= finalizedEpoch {
		pivotHashes, err := s.epochBlockMapStore.LoadPivotHashes(finalizedEpoch, s.epochFrom)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to load pivot hashes")
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
