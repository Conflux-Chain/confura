package main

import (
	// ensure viper based configuration initialized at the very beginning
	_ "github.com/conflux-chain/conflux-infura/config"

	// init metrics reporter
	_ "github.com/conflux-chain/conflux-infura/metrics"

	"github.com/conflux-chain/conflux-infura/rpc"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/store/mysql"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/spf13/viper"
)

func main() {
	cfx := util.MustNewCfxClient(viper.GetString("cfx.ws"))
	defer cfx.Close()

	// executedEpochs := sync.NewEpochExecWindow()
	// go sync.MustSubEpoch(cfx, sync.NewConsoleEpochSubscriber(cfx))
	// go sync.MustSubEpoch(cfx, executedEpochs)
	// go sync.MustSubEpoch(cfx, sync.NewConsoleEpochSubscriber(cfx), executedEpochs)
	// go nearhead.Start(cfx, executedEpochs.Executed())
	// go executedEpochs.Diff(cfx)

	config := mysql.NewConfigFromViper()
	db := config.MustOpenOrCreate()
	defer db.Close()
	go store.SyncEpochData(cfx, db)

	go rpc.Serve(viper.GetString("endpoint"), cfx)

	// TODO monitor ctrlc for cleanup before termination?
	select {}
}
