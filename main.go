package main

import (
	// ensure viper based configuration initialized at the very beginning
	_ "github.com/conflux-chain/conflux-infura/config"
	"github.com/sirupsen/logrus"

	// init metrics reporter
	_ "github.com/conflux-chain/conflux-infura/metrics"

	"github.com/conflux-chain/conflux-infura/rpc"
	"github.com/conflux-chain/conflux-infura/store/mysql"
	"github.com/conflux-chain/conflux-infura/sync"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/spf13/viper"
)

func main() {
	// Prepare cfx instance with ws portocol for pub/sub purpose
	cfx := util.MustNewCfxClient(viper.GetString("cfx.ws"))
	defer cfx.Close()

	// Initialize database for sync
	config := mysql.NewConfigFromViper()
	db := config.MustOpenOrCreate()
	defer db.Close()

	// Start to sync data
	logrus.Info("Starting to sync epoch data...")
	syncer := sync.NewDatabaseSyncer(cfx, db)
	go syncer.Sync()

	// Monitor pivot chain switch via pub/sub
	logrus.Info("Starting to pub/sub conflux chain...")
	go sync.MustSubEpoch(cfx, syncer)

	// Start database pruner
	logrus.Info("Starting db pruner...")
	pruner := sync.NewDBPruner(db)
	go pruner.Prune()

	// Start RPC server
	logrus.Info("Starting to run rpc server...")
	go rpc.Serve(viper.GetString("endpoint"), cfx, db)

	select {}
}
