package main

import (
	// ensure viper based configuration initialized at the very beginning
	_ "github.com/conflux-chain/conflux-infura/config"

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
	syncer := sync.NewDatabaseSyncer(cfx, db)
	go syncer.Sync()

	// Monitor pivot chain switch via pub/sub
	go sync.MustSubEpoch(cfx, syncer)

	// Start RPC server
	go rpc.Serve(viper.GetString("endpoint"), cfx)

	select {}
}
