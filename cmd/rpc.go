package cmd

import (
	"context"
	"sync"

	viperutil "github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/relay"
	"github.com/conflux-chain/conflux-infura/rpc"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/store/mysql"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type rpcOption struct {
	cfxEnabled       bool
	ethEnabled       bool
	cfxBridgeEnabled bool
}

var (
	rpcOpt rpcOption

	rpcCmd = &cobra.Command{
		Use:   "rpc",
		Short: "Start RPC service, including CFX, ETH and CfxBridge RPC servers",
		Run:   startRpcService,
	}
)

func init() {
	rpcCmd.Flags().BoolVar(&rpcOpt.cfxEnabled, "cfx", false, "Start CFX space RPC server")
	rpcCmd.Flags().BoolVar(&rpcOpt.ethEnabled, "eth", false, "Start ETH space RPC server")
	rpcCmd.Flags().BoolVar(&rpcOpt.cfxBridgeEnabled, "cfxBridge", false, "Start CFX bridge RPC server")

	rootCmd.AddCommand(rpcCmd)
}

func startRpcService(*cobra.Command, []string) {
	if !rpcOpt.cfxEnabled && !rpcOpt.ethEnabled && !rpcOpt.cfxBridgeEnabled {
		logrus.Fatal("No RPC server specified")
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	storeCtx := mustInitStoreContext(false)
	defer storeCtx.Close()

	if rpcOpt.cfxEnabled {
		startNativeSpaceRpcServer(ctx, &wg, storeCtx)
	}

	if rpcOpt.ethEnabled {
		startEvmSpaceRpcServer(ctx, &wg, storeCtx)
	}

	if rpcOpt.cfxBridgeEnabled {
		startNativeSpaceBridgeRpcServer(ctx, &wg)
	}

	util.GracefulShutdown(&wg, cancel)
}

func startNativeSpaceRpcServer(ctx context.Context, wg *sync.WaitGroup, storeCtx storeContext) {
	router := node.Factory().CreateRouter()

	// Add empty store tolerance
	var cfxHandler *rpc.CfxStoreHandler
	for _, s := range []store.Store{storeCtx.cfxDB, storeCtx.cfxCache} {
		if !util.IsInterfaceValNil(s) {
			cfxHandler = rpc.NewCfxStoreHandler(s, cfxHandler)
		}
	}

	gasHandler := rpc.NewGasStationHandler(storeCtx.cfxDB, storeCtx.cfxCache)
	exposedModules := viper.GetStringSlice("rpc.exposedModules")

	var logApi *rpc.CfxLogApi
	if storeCtx.cfxDB != nil {
		if redisUrl := viper.GetString("rpc.throttling.redisUrl"); len(redisUrl) > 0 {
			logApi = rpc.NewCfxLogApi(
				node.NewCfxClientProvider(router),
				mysql.MustConvert(storeCtx.cfxDB).UserStore,
				util.MustNewRedisClient(redisUrl),
			)
		}
	}

	option := rpc.CfxAPIOption{
		Handler: cfxHandler,
		Relayer: relay.MustNewTxnRelayerFromViper(),
		LogApi:  logApi,
	}

	server := rpc.MustNewNativeSpaceServer(router, gasHandler, exposedModules, option)

	httpEndpoint := viper.GetString("rpc.endpoint")
	go server.MustServeGraceful(ctx, wg, httpEndpoint, util.RpcProtocolHttp)

	if wsEndpoint := viper.GetString("rpc.wsEndpoint"); len(wsEndpoint) > 0 {
		go server.MustServeGraceful(ctx, wg, wsEndpoint, util.RpcProtocolWS)
	}
}

func startEvmSpaceRpcServer(ctx context.Context, wg *sync.WaitGroup, storeCtx storeContext) {
	// Add empty store tolerance
	var ethhandler *rpc.EthStoreHandler
	if !util.IsInterfaceValNil(storeCtx.ethDB) {
		ethhandler = rpc.NewEthStoreHandler(storeCtx.ethDB, ethhandler)
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
