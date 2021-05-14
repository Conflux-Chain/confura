package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	// ensure viper based configuration initialized at the very beginning
	_ "github.com/conflux-chain/conflux-infura/config"

	// init metrics reporter
	_ "github.com/conflux-chain/conflux-infura/metrics"

	"github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/rpc"
	"github.com/conflux-chain/conflux-infura/store/mysql"
	cisync "github.com/conflux-chain/conflux-infura/sync"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func main() {
	// Context to control child go routines
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	// Initialize database for sync
	config := mysql.NewConfigFromViper()
	db := config.MustOpenOrCreate()
	defer db.Close()

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

	// Initialize node manager to route RPC requests
	nm := node.NewMananger()

	// Start RPC server
	logrus.Info("Starting to run rpc server...")
	rpcServer := rpc.NewServer(nm, db)
	go rpcServer.MustServe(viper.GetString("endpoint"))

	gracefulShutdown(ctx, rpcServer, wg, cancel)
}

func gracefulShutdown(ctx context.Context, rpcServer *util.RpcServer, wg *sync.WaitGroup, cancel context.CancelFunc) {
	// Handle sigterm and await termChan signal
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGTERM, syscall.SIGINT)

	// Wait for SIGTERM to be captured
	<-termChan
	logrus.Info("SIGTERM/SIGINT received, shutdown process initiated")

	// Shutdown the RPC server gracefully
	if err := rpcServer.Shutdown(3 * time.Second); err != nil {
		logrus.WithError(err).Error("RPC server shutdown failed")
	} else {
		logrus.Info("RPC server shutdown ok")
	}

	// Cancel to notify active goroutines to clean up.
	cancel()

	logrus.Info("Waiting for shutdown...")
	wg.Wait()

	logrus.Info("Shutdown gracefully")
}
