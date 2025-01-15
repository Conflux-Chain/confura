package benchmark

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/sync/catchup"
	"github.com/Conflux-Chain/confura/sync/election"
	"github.com/Conflux-Chain/confura/sync/monitor"
	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// CatchUpMode defines the supported catch-up modes.
type CatchUpMode string

const (
	ModeClassic CatchUpMode = "classic"
	ModeBoost   CatchUpMode = "boost"

	defaultMode  = "classic" // default catch-up mode
	defaultCount = 10000     // default number of epochs/blocks to sync
)

type CatchUpCmdConfig struct {
	Mode  string // catch-up mode ("classic" or "boost")
	Start uint64 // start epoch/block number
	Count uint64 // number of epochs/blocks to sync
}

var (
	catchUpConfig CatchUpCmdConfig

	catchUpCmd = &cobra.Command{
		Use:   "catchup",
		Short: "Start catch-up benchmark testing",
		Run:   runCatchUpBenchmark,
	}
)

func init() {
	Cmd.AddCommand(catchUpCmd)
	hookCatchUpCmdFlags(catchUpCmd)
}

// hookCatchUpCmdFlags configures the command-line flags for the catch-up command.
func hookCatchUpCmdFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(
		&catchUpConfig.Mode, "mode", "m", defaultMode, "catch-up mode ('classic' or 'boost')",
	)
	cmd.MarkFlagRequired("mode")

	cmd.Flags().Uint64VarP(
		&catchUpConfig.Start, "start", "s", 0, "start epoch or block number to sync from",
	)
	cmd.Flags().Uint64VarP(
		&catchUpConfig.Count, "count", "c", defaultCount, "number of epochs or blocks to sync",
	)
}

// runCatchUpBenchmark starts the catch-up benchmarking process.
func runCatchUpBenchmark(cmd *cobra.Command, args []string) {
	// Initialize required contexts
	storeCtx, syncCtx, err := initializeContexts()
	if err != nil {
		logrus.WithError(err).Info("Failed to initialize contexts")
		return
	}
	defer storeCtx.Close()
	defer syncCtx.Close()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// Determine sync range and mode
	start := catchUpConfig.Start
	end := catchUpConfig.Start + max(catchUpConfig.Count, 1) - 1
	useBoost := strings.EqualFold(catchUpConfig.Mode, string(ModeBoost))

	// Initialize the catch-up syncer.
	catchUpSyncer := createCatchUpSyncer(syncCtx, storeCtx, start)
	defer catchUpSyncer.Close()

	// Start the sync process in a separate goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		catchUpSyncer.SyncOnce(ctx, start, end, useBoost)
	}()

	// Ensure RPC metrics are reported after sync completes.
	defer reportRpcMetrics(useBoost)

	// Handle graceful shutdown
	util.GracefulShutdown(&wg, cancel)
}

// initializeContexts sets up and returns the required store and sync contexts.
func initializeContexts() (*util.StoreContext, *util.SyncContext, error) {
	storeCtx := util.MustInitStoreContext()
	if storeCtx.CfxDB == nil {
		return nil, nil, errors.New("database is not provided")
	}

	syncCtx := util.MustInitSyncContext(storeCtx)
	return &storeCtx, &syncCtx, nil
}

// createCatchUpSyncer initializes the catch-up syncer with the necessary dependencies.
func createCatchUpSyncer(syncCtx *util.SyncContext, storeCtx *util.StoreContext, start uint64) *catchup.Syncer {
	return catchup.MustNewSyncer(
		syncCtx.SyncCfxs, storeCtx.CfxDB, election.NewNoopLeaderManager(), &monitor.Monitor{}, start,
	)
}

// reportRpcMetrics outputs RPC-related metrics based on the sync mode.
func reportRpcMetrics(useBoostMode bool) {
	if useBoostMode {
		reportBoostMetrics()
	} else {
		reportClassicMetrics()
	}
}

// reportBoostMetrics reports metrics specific to boost mode.
func reportBoostMetrics() {
	boostQueryTimer := metrics.Registry.Sync.BoostQueryEpochData("cfx")
	boostQueryRangeHistogram := metrics.Registry.Sync.BoostQueryEpochRange()
	boostQueryRateGaugue := metrics.Registry.Sync.BoostQueryEpochDataAvailability("cfx")

	fmt.Println("// ------------- boost query tps --------------")
	fmt.Printf("mean tps: %v\n", boostQueryTimer.Snapshot().RateMean())
	fmt.Printf("  m1 tps: %v\n", boostQueryTimer.Snapshot().Rate1())
	fmt.Printf("  m5 tps: %v\n", boostQueryTimer.Snapshot().Rate5())
	fmt.Printf(" m15 tps: %v\n", boostQueryTimer.Snapshot().Rate15())

	fmt.Println("// ---------- boost query duration ------------")
	fmt.Printf(" total queries: %v\n", boostQueryTimer.Snapshot().Count())
	fmt.Printf("  max duration: %.2f(ms)\n", float64(boostQueryTimer.Snapshot().Max())/1e6)
	fmt.Printf("  min duration: %.2f(ms)\n", float64(boostQueryTimer.Snapshot().Min()/1e6))
	fmt.Printf(" mean duration: %.2f(ms)\n", boostQueryTimer.Snapshot().Mean()/1e6)
	fmt.Printf("  p99 duration: %.2f(ms)\n", float64(boostQueryTimer.Snapshot().Percentile(0.99))/1e6)
	fmt.Printf("  p75 duration: %.2f(ms)\n", float64(boostQueryTimer.Snapshot().Percentile(0.75))/1e6)
	fmt.Printf("  p50 duration: %.2f(ms)\n", float64(boostQueryTimer.Snapshot().Percentile(0.50))/1e6)

	fmt.Println("// ---------- boost query epoch range ------------")
	fmt.Printf("     total epochs: %v\n", boostQueryRangeHistogram.Snapshot().Sum())
	fmt.Printf(" max batch epochs: %v\n", boostQueryRangeHistogram.Snapshot().Max())
	fmt.Printf(" min batch epochs: %v\n", boostQueryRangeHistogram.Snapshot().Min())
	fmt.Printf("mean batch epochs: %v\n", boostQueryRangeHistogram.Snapshot().Mean())
	fmt.Printf(" p99 batch epochs: %v\n", boostQueryRangeHistogram.Snapshot().Percentile(0.99))
	fmt.Printf(" p75 batch epochs: %v\n", boostQueryRangeHistogram.Snapshot().Percentile(0.75))
	fmt.Printf(" p50 batch epochs: %v\n", boostQueryRangeHistogram.Snapshot().Percentile(0.50))

	fmt.Println("// ---------- boost query success rate ------------")
	fmt.Printf(" success ratio: %v\n", boostQueryRateGaugue.Snapshot().Value())

	fmt.Println("// ------------------------------------------------")
}

// reportClassicMetrics reports metrics specific to classic mode.
func reportClassicMetrics() {
	queryTimer := metrics.Registry.Sync.QueryEpochData("cfx")
	queryRateGaugue := metrics.Registry.Sync.QueryEpochDataAvailability("cfx")

	fmt.Println("// ------------- epoch query tps --------------")
	fmt.Printf("mean tps: %v\n", queryTimer.Snapshot().RateMean())
	fmt.Printf("  m1 tps: %v\n", queryTimer.Snapshot().Rate1())
	fmt.Printf("  m5 tps: %v\n", queryTimer.Snapshot().Rate5())
	fmt.Printf(" m15 tps: %v\n", queryTimer.Snapshot().Rate15())

	fmt.Println("// ---------- epoch query duration ------------")
	fmt.Printf(" total queries: %v\n", queryTimer.Snapshot().Count())
	fmt.Printf("  max duration: %.2f(ms)\n", float64(queryTimer.Snapshot().Max())/1e6)
	fmt.Printf("  min duration: %.2f(ms)\n", float64(queryTimer.Snapshot().Min()/1e6))
	fmt.Printf(" mean duration: %.2f(ms)\n", queryTimer.Snapshot().Mean()/1e6)
	fmt.Printf("  p99 duration: %.2f(ms)\n", float64(queryTimer.Snapshot().Percentile(0.99))/1e6)
	fmt.Printf("  p75 duration: %.2f(ms)\n", float64(queryTimer.Snapshot().Percentile(0.75))/1e6)
	fmt.Printf("  p50 duration: %.2f(ms)\n", float64(queryTimer.Snapshot().Percentile(0.50))/1e6)

	fmt.Println("// ---------- epoch query success rate ------------")
	fmt.Printf(" success ratio: %v\n", queryRateGaugue.Snapshot().Value())

	fmt.Println("// ------------------------------------------------")
}
