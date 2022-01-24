package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

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
	if config, ok := mysql.NewConfigFromViper(); ok {
		db = config.MustOpenOrCreate(mysql.StoreOption{CalibrateEpochStats: syncServerEnabled})
		defer db.Close()
	}

	// Initialize cache store
	var cache store.CacheStore
	if viper.GetBool("store.redis.enabled") {
		cache = redis.MustNewCacheStore()
		defer cache.Close()
	}

	if syncServerEnabled {
		if db == nil {
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
	}

	var rpcServers []*util.RpcServer

	if rpcServerEnabled {
		router := node.MustNewRouterFromViper()

		nsServer := startNativeSpaceRpcServer(router, db, cache)
		rpcServers = append(rpcServers, nsServer)

		evmServer := startEvmSpaceRpcServer()
		rpcServers = append(rpcServers, evmServer)

		cfxBridgeServer := startNativeSpaceBridgeRpcServer()
		rpcServers = append(rpcServers, cfxBridgeServer)
	}

	if nodeServerEnabled {
		server := startNodeServer()
		rpcServers = append(rpcServers, server)
	}

	gracefulShutdown(ctx, func() error {
		// Shutdown the RPC server gracefully
		for _, server := range rpcServers {
			if err := server.Shutdown(3 * time.Second); err != nil {
				logrus.WithError(err).WithField("name", server.String()).Error("RPC server shutdown failed")
			} else {
				logrus.WithField("name", server.String()).Info("RPC server shutdown ok")
			}
		}
		return nil
	}, wg, cancel)
}

func startEvmSpaceRpcServer() *util.RpcServer {
	// Start RPC server
	logrus.Info("Start to run EVM space rpc server...")

	// TODO: add support for ETH client pool from node cluster
	ethNodeURL := viper.GetString("eth.http")
	exposedModules := viper.GetStringSlice("ethrpc.exposedModules")
	server := rpc.MustNewEvmSpaceServer(ethNodeURL, exposedModules)

	httpEndpoint := viper.GetString("ethrpc.endpoint")
	go server.MustServe(httpEndpoint)

	return server
}

func startNativeSpaceBridgeRpcServer() *util.RpcServer {
	// Start RPC server
	logrus.Info("Start to run native space bridge rpc server...")

	// TODO configure cluster for CFX bridge?
	ethNodeURL := viper.GetString("cfxBridge.ethNode")
	cfxNodeURL := viper.GetString("cfxBridge.cfxNode")
	exposedModules := viper.GetStringSlice("cfxBridge.exposedModules")
	server := rpc.MustNewNativeSpaceBridgeServer(ethNodeURL, cfxNodeURL, exposedModules)

	httpEndpoint := viper.GetString("cfxBridge.endpoint")
	go server.MustServe(httpEndpoint)

	return server
}

func startNativeSpaceRpcServer(router node.Router, db, cache store.Store) *util.RpcServer {
	// Start RPC server
	logrus.Info("Start to run native space rpc server...")

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
	wsEndpoint := viper.GetString("rpc.wsEndpoint")
	go server.MustServe(httpEndpoint, wsEndpoint)

	return server
}

func startNodeServer() *util.RpcServer {
	logrus.Info("Start to run node management rpc server")
	server := node.NewServer()
	go server.MustServe(viper.GetString("node.endpoint"))
	return server
}

func gracefulShutdown(ctx context.Context, destroy func() error, wg *sync.WaitGroup, cancel context.CancelFunc) {
	// Handle sigterm and await termChan signal
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGTERM, syscall.SIGINT)

	// Wait for SIGTERM to be captured
	<-termChan
	logrus.Info("SIGTERM/SIGINT received, shutdown process initiated")

	if destroy != nil {
		destroy()
	}

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
