package main

import (
	// ensure viper based configuration initialized at the very beginning
	_ "github.com/conflux-chain/conflux-infura/config"

	// init metrics reporter
	_ "github.com/conflux-chain/conflux-infura/metrics"

	"github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/rpc"
	"github.com/conflux-chain/conflux-infura/store/mysql"
	"github.com/conflux-chain/conflux-infura/sync"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func main() {
	// Initialize database for sync
	config := mysql.NewConfigFromViper()
	db := config.MustOpenOrCreate()
	defer db.Close()

	// Prepare cfx instance with http protocol for epoch sync purpose
	syncCfx := util.MustNewCfxClient(viper.GetString("cfx.http"))
	defer syncCfx.Close()

	// Start to sync data
	logrus.Info("Starting to sync epoch data...")
	syncer := sync.NewDatabaseSyncer(syncCfx, db)
	go syncer.Sync()

	// Prepare cfx instance with ws portocol for pub/sub purpose
	subCfx := util.MustNewCfxClient(viper.GetString("cfx.ws"))
	defer subCfx.Close()

	// Monitor pivot chain switch via pub/sub
	logrus.Info("Starting to pub/sub conflux chain...")
	go sync.MustSubEpoch(subCfx, syncer)

	// Start database pruner
	logrus.Info("Starting db pruner...")
	pruner := sync.NewDBPruner(db)
	go pruner.Prune()

	// Initialize node manager to route RPC requests
	nm := node.NewMananger()

	// Start RPC server
	logrus.Info("Starting to run rpc server...")
	go rpc.Serve(viper.GetString("endpoint"), nm, db)

	select {}
}
