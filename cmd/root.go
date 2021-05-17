package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/rpc"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/store/mysql"
	cisync "github.com/conflux-chain/conflux-infura/sync"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
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
	rootCmd.Flags().BoolVar(&nodeServerEnabled, "nm", false, "Whether to start node management service")
	rootCmd.Flags().BoolVar(&rpcServerEnabled, "rpc", false, "Whether to start Conflux public RPC service")
	rootCmd.Flags().BoolVar(&syncServerEnabled, "sync", false, "Whether to start data sync/prune service")
}

func start(cmd *cobra.Command, args []string) {
	if !nodeServerEnabled && !rpcServerEnabled && !syncServerEnabled {
		logrus.Info("No services started")
		return
	}

	// Context to control child go routines
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	// Initialize database
	var db store.Store
	if config, ok := mysql.NewConfigFromViper(); ok {
		db = config.MustOpenOrCreate()
		defer db.Close()
	}

	if syncServerEnabled {
		// Prepare cfx instance with http protocol for epoch sync purpose
		syncCfx := util.MustNewCfxClient(viper.GetString("cfx.http"))
		defer syncCfx.Close()

		// Start to sync data
		logrus.Info("Starting to sync epoch data...")
		syncer := cisync.NewDatabaseSyncer(syncCfx, db)
		go syncer.Sync(ctx, wg)

		// Prepare cfx instance with ws portocol for pub/sub purpose
		subCfx := util.MustNewCfxClient(viper.GetString("cfx.ws"))
		defer subCfx.Close()

		// Monitor pivot chain switch via pub/sub
		logrus.Info("Starting to pub/sub conflux chain...")
		go cisync.MustSubEpoch(ctx, wg, subCfx, syncer)

		// Start database pruner
		logrus.Info("Starting db pruner...")
		pruner := cisync.NewDBPruner(db)
		go pruner.Prune(ctx, wg)
	}

	var rpcServers []*util.RpcServer

	if rpcServerEnabled {
		server := startRpcServer(db)
		rpcServers = append(rpcServers, server)
	}

	if nodeServerEnabled {
		server := startNodeServer()
		rpcServers = append(rpcServers, server)
	}

	gracefulShutdown(ctx, rpcServers, wg, cancel)
}

func startRpcServer(db store.Store) *util.RpcServer {
	// Initialize node manager to route RPC requests
	// TODO use node management RPC for distributed deployment
	nm := node.NewMananger()

	// Start RPC server
	logrus.Info("Start to run public rpc server...")
	server := rpc.NewServer(nm, db)
	go server.MustServe(viper.GetString("endpoint"))
	return server
}

func startNodeServer() *util.RpcServer {
	logrus.Info("Start to run node management rpc server")
	server := node.NewServer()
	go server.MustServe(viper.GetString("node.endpoint"))
	return server
}

func gracefulShutdown(ctx context.Context, rpcServers []*util.RpcServer, wg *sync.WaitGroup, cancel context.CancelFunc) {
	// Handle sigterm and await termChan signal
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGTERM, syscall.SIGINT)

	// Wait for SIGTERM to be captured
	<-termChan
	logrus.Info("SIGTERM/SIGINT received, shutdown process initiated")

	// Shutdown the RPC server gracefully
	for _, server := range rpcServers {
		if err := server.Shutdown(3 * time.Second); err != nil {
			logrus.WithError(err).WithField("name", server.String()).Error("RPC server shutdown failed")
		} else {
			logrus.WithField("name", server.String()).Info("RPC server shutdown ok")
		}
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
