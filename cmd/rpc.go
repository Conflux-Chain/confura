package cmd

import (
	"context"
	"sync"
	"time"

	viperutil "github.com/Conflux-Chain/go-conflux-util/viper"
	cmdutil "github.com/conflux-chain/conflux-infura/cmd/util"
	"github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/rpc"
	"github.com/conflux-chain/conflux-infura/rpc/handler"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/store/redis"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/conflux-chain/conflux-infura/util/rate"
	"github.com/conflux-chain/conflux-infura/util/relay"
	rpcutil "github.com/conflux-chain/conflux-infura/util/rpc"
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

	storeCtx := mustInitStoreContext()
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

	cmdutil.GracefulShutdown(&wg, cancel)
}

func startNativeSpaceRpcServer(ctx context.Context, wg *sync.WaitGroup, storeCtx storeContext) {
	router := node.Factory().CreateRouter()

	// Add empty store tolerance
	var storeHandler *handler.CfxStoreHandler
	storeNames := []string{"db", "cache"}

	for i, s := range []store.Readable{storeCtx.cfxDB, storeCtx.cfxCache} {
		if !util.IsInterfaceValNil(s) {
			storeHandler = handler.NewCfxCommonStoreHandler(storeNames[i], s, storeHandler)
		}
	}

	gasHandler := handler.NewGasStationHandler(storeCtx.cfxDB, storeCtx.cfxCache)
	exposedModules := viper.GetStringSlice("rpc.exposedModules")

	var logsApiHandler *handler.CfxLogsApiHandlerV2
	if storeCtx.cfxDB != nil {
		var prunedHandler *handler.CfxPrunedLogsHandler
		if redisUrl := viper.GetString("rpc.throttling.redisUrl"); len(redisUrl) > 0 {
			prunedHandler = handler.NewCfxPrunedLogsHandler(
				node.NewCfxClientProvider(router),
				storeCtx.cfxDB.UserStore,
				redis.MustNewRedisClient(redisUrl),
			)
		}

		logsApiHandler = handler.NewCfxLogsApiHandlerV2(storeCtx.ethDB, prunedHandler)

		go rate.DefaultRegistryCfx.AutoReload(10*time.Second, storeCtx.cfxDB.LoadRateLimitConfigs)
	}

	option := rpc.CfxAPIOption{
		StoreHandler:  storeHandler,
		LogApiHandler: logsApiHandler,
		Relayer:       relay.MustNewTxnRelayerFromViper(),
	}

	server := rpc.MustNewNativeSpaceServer(router, gasHandler, exposedModules, option)

	httpEndpoint := viper.GetString("rpc.endpoint")
	go server.MustServeGraceful(ctx, wg, httpEndpoint, rpcutil.ProtocolHttp)

	if wsEndpoint := viper.GetString("rpc.wsEndpoint"); len(wsEndpoint) > 0 {
		go server.MustServeGraceful(ctx, wg, wsEndpoint, rpcutil.ProtocolWS)
	}
}

func startEvmSpaceRpcServer(ctx context.Context, wg *sync.WaitGroup, storeCtx storeContext) {
	var option rpc.EthAPIOption
	router := node.EthFactory().CreateRouter()

	// Add empty store tolerance
	if !util.IsInterfaceValNil(storeCtx.ethDB) {
		option.StoreHandler = handler.NewEthStoreHandler(storeCtx.ethDB, nil)
		option.LogApiHandler = handler.NewEthLogsApiHandlerV2(storeCtx.ethDB)

		go rate.DefaultRegistryEth.AutoReload(10*time.Second, storeCtx.ethDB.LoadRateLimitConfigs)
	}

	exposedModules := viper.GetStringSlice("ethrpc.exposedModules")
	server := rpc.MustNewEvmSpaceServer(router, exposedModules, option)

	httpEndpoint := viper.GetString("ethrpc.endpoint")
	go server.MustServeGraceful(ctx, wg, httpEndpoint, rpcutil.ProtocolHttp)

	if wsEndpoint := viper.GetString("ethrpc.wsEndpoint"); len(wsEndpoint) > 0 {
		go server.MustServeGraceful(ctx, wg, wsEndpoint, rpcutil.ProtocolWS)
	}
}

func startNativeSpaceBridgeRpcServer(ctx context.Context, wg *sync.WaitGroup) {
	var config rpc.CfxBridgeServerConfig
	viperutil.MustUnmarshalKey("rpc.cfxBridge", &config)

	logrus.WithField("config", config).Info("Start to run cfx bridge rpc server")

	server := rpc.MustNewNativeSpaceBridgeServer(&config)
	go server.MustServeGraceful(ctx, wg, config.Endpoint, rpcutil.ProtocolHttp)
}
