package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	viperutil "github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/conflux-chain/conflux-infura/config"
	"github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/relay"
	"github.com/conflux-chain/conflux-infura/rpc"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/store/mysql"
	"github.com/conflux-chain/conflux-infura/store/redis"
	cisync "github.com/conflux-chain/conflux-infura/sync"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	flagVersion       bool // print version and exit
	nodeServerEnabled bool // node management service
	rpcServerEnabled  bool // Conflux public RPC service
	syncServerEnabled bool // data sync/prune service

	rootCmd = &cobra.Command{
		Use:   "conflux-infura",
		Short: "Conflux infura provides scalable RPC service",
		Run:   start,
	}
)

func init() {
	rootCmd.Flags().BoolVarP(&flagVersion, "version", "v", false, "If true, print version and exit")

	rootCmd.Flags().BoolVar(&nodeServerEnabled, "nm", false, "whether to start node management service")
	rootCmd.Flags().BoolVar(&rpcServerEnabled, "rpc", false, "whether to start Conflux public RPC service")
	rootCmd.Flags().BoolVar(&syncServerEnabled, "sync", false, "whether to start data sync/prune service")

	rootCmd.AddCommand(testCmd)
	rootCmd.AddCommand(wsTestCmd)
	rootCmd.AddCommand(ethTestCmd)
}

func start(cmd *cobra.Command, args []string) {
	if flagVersion {
		config.DumpVersionInfo()
		return
	}

	if !nodeServerEnabled && !rpcServerEnabled && !syncServerEnabled {
		logrus.Fatal("No services started")
		return
	}

	// Context to control child go routines
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	// Initialize database
	var db store.Store
	if config := mysql.MustNewConfigFromViper(); config.Enabled {
		db = config.MustOpenOrCreate(mysql.StoreOption{CalibrateEpochStats: syncServerEnabled})
		defer db.Close()
	}

	var ethdb store.Store
	if ethConfig := mysql.MustNewEthStoreConfigFromViper(); ethConfig.Enabled {
		ethdb = ethConfig.MustOpenOrCreate(mysql.StoreOption{CalibrateEpochStats: syncServerEnabled})
		defer ethdb.Close()
	}

	// Initialize cache store
	cache, ok := redis.MustNewCacheStoreFromViper()
	if ok {
		defer cache.Close()
	}

	if syncServerEnabled {
		if db == nil || ethdb == nil {
			logrus.Fatal("database not enabled")
		}

		if cache == nil {
			logrus.Fatal("cache not enabled")
		}

		// Prepare cfx instance with http protocol for epoch sync purpose
		syncCfx := util.MustNewCfxClient(viper.GetString("cfx.http"))
		defer syncCfx.Close()

		// Start to sync data
		logrus.Info("Starting to sync epoch data into db...")
		syncer := cisync.MustNewDatabaseSyncer(syncCfx, db)
		go syncer.Sync(ctx, wg)

		logrus.Info("Starting to sync epoch data into cache...")
		csyncer := cisync.MustNewKVCacheSyncer(syncCfx, cache)
		go csyncer.Sync(ctx, wg)

		// Prepare cfx instance with ws portocol for pub/sub purpose
		subCfx := util.MustNewCfxClient(viper.GetString("cfx.ws"))
		defer subCfx.Close()

		// Monitor pivot chain switch via pub/sub
		logrus.Info("Starting to pub/sub conflux chain...")
		go cisync.MustSubEpoch(ctx, wg, subCfx, syncer, csyncer)

		// Start database pruner
		logrus.Info("Starting db pruner...")
		pruner := cisync.MustNewDBPruner(db)
		go pruner.Prune(ctx, wg)

		// Start kv cache pruner
		logrus.Info("Starting kv cache pruner...")
		cpruner := cisync.MustNewKVCachePruner(cache)
		go cpruner.Prune(ctx, wg)

		// Prepare ETH client with http protocol for eth sync purpose
		syncEth := util.MustNewEthClientFromViper()

		// Start to sync ETH data
		logrus.Info("Starting to sync eth data into db...")
		ethSyncer := cisync.MustNewEthSyncer(syncEth, ethdb)
		go ethSyncer.Sync(ctx, wg)

		// Start ethdb pruner
		logrus.Info("Starting ethdb pruner...")
		ethdbPruner := cisync.MustNewDBPruner(ethdb)
		go ethdbPruner.Prune(ctx, wg)
	}

	if rpcServerEnabled {
		startNativeSpaceRpcServer(ctx, wg, db, cache)
		startEvmSpaceRpcServer(ctx, wg, ethdb)
		startNativeSpaceBridgeRpcServer(ctx, wg)
	}

	if nodeServerEnabled {
		startNodeServer(ctx, wg)
	}

	gracefulShutdown(wg, cancel)
}

func startEvmSpaceRpcServer(ctx context.Context, wg *sync.WaitGroup, db store.Store) {
	// Add empty store tolerance
	var ethhandler *rpc.EthStoreHandler
	if !util.IsInterfaceValNil(db) {
		ethhandler = rpc.NewEthStoreHandler(db, ethhandler)
	}

	// TODO: add support for ETH client pool from node cluster
	ethNodeURL := viper.GetString("eth.http")
	exposedModules := viper.GetStringSlice("ethrpc.exposedModules")
	server := rpc.MustNewEvmSpaceServer(ethhandler, ethNodeURL, exposedModules)

	httpEndpoint := viper.GetString("ethrpc.endpoint")
	go server.MustServeGraceful(ctx, wg, httpEndpoint, util.RpcProtocolHttp)
}

func startNativeSpaceBridgeRpcServer(ctx context.Context, wg *sync.WaitGroup) {
	var config rpc.CfxBridgeServerConfig
	viperutil.MustUnmarshalKey("rpc.cfxBridge", &config)

	logrus.WithField("config", config).Info("Start to run cfx bridge rpc server")

	server := rpc.MustNewNativeSpaceBridgeServer(&config)
	go server.MustServeGraceful(ctx, wg, config.Endpoint, util.RpcProtocolHttp)
}

func startNativeSpaceRpcServer(ctx context.Context, wg *sync.WaitGroup, db, cache store.Store) {
	router := node.MustNewRouterFromViper()

	// Add empty store tolerance
	var cfxHandler *rpc.CfxStoreHandler
	for _, s := range []store.Store{db, cache} {
		if !util.IsInterfaceValNil(s) {
			cfxHandler = rpc.NewCfxStoreHandler(s, cfxHandler)
		}
	}

	gasHandler := rpc.NewGasStationHandler(db, cache)
	exposedModules := viper.GetStringSlice("rpc.exposedModules")
	txRelayer := relay.MustNewTxnRelayerFromViper()

	server := rpc.MustNewNativeSpaceServer(
		router, cfxHandler, gasHandler, exposedModules, txRelayer,
	)

	httpEndpoint := viper.GetString("rpc.endpoint")
	go server.MustServeGraceful(ctx, wg, httpEndpoint, util.RpcProtocolHttp)

	if wsEndpoint := viper.GetString("rpc.wsEndpoint"); len(wsEndpoint) > 0 {
		go server.MustServeGraceful(ctx, wg, wsEndpoint, util.RpcProtocolWS)
	}
}

func startNodeServer(ctx context.Context, wg *sync.WaitGroup) {
	server := node.NewServer()
	endpoint := node.Config().Endpoint
	go server.MustServeGraceful(ctx, wg, endpoint, util.RpcProtocolHttp)
}

func gracefulShutdown(wg *sync.WaitGroup, cancel context.CancelFunc) {
	// Handle sigterm and await termChan signal
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGTERM, syscall.SIGINT)

	// Wait for SIGTERM to be captured
	<-termChan
	logrus.Info("SIGTERM/SIGINT received, shutdown process initiated")

	// Cancel to notify active goroutines to clean up.
	cancel()

	logrus.Info("Waiting for shutdown...")
	wg.Wait()

	logrus.Info("Shutdown gracefully")
}

// Execute is the command line entrypoint.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
