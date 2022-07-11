package cmd

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	viperutil "github.com/Conflux-Chain/go-conflux-util/viper"
	cmdutil "github.com/conflux-chain/conflux-infura/cmd/util"
	"github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/rpc"
	"github.com/conflux-chain/conflux-infura/rpc/handler"
	"github.com/conflux-chain/conflux-infura/store/redis"
	"github.com/conflux-chain/conflux-infura/util/rate"
	"github.com/conflux-chain/conflux-infura/util/relay"
	rpcutil "github.com/conflux-chain/conflux-infura/util/rpc"
)

var (
	// RPC boot options
	rpcOpt struct {
		cfxEnabled       bool
		ethEnabled       bool
		cfxBridgeEnabled bool
	}

	rpcCmd = &cobra.Command{
		Use:   "rpc",
		Short: "Start RPC service, including core space, evm space and CfxBridge RPC servers",
		Run:   startRpcService,
	}
)

func init() {
	// boot flag for core space
	rpcCmd.Flags().BoolVar(
		&rpcOpt.cfxEnabled, "cfx", false, "start core space RPC server",
	)

	// boot flag for evm space
	rpcCmd.Flags().BoolVar(
		&rpcOpt.ethEnabled, "eth", false, "start evm space RPC server",
	)

	// boot flag for core space bridge
	rpcCmd.Flags().BoolVar(
		&rpcOpt.cfxBridgeEnabled, "cfxBridge", false, "start core space bridge RPC server",
	)

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

	if rpcOpt.cfxEnabled { // start core space RPC
		startNativeSpaceRpcServer(ctx, &wg, storeCtx)
	}

	if rpcOpt.ethEnabled { // start evm space RPC
		startEvmSpaceRpcServer(ctx, &wg, storeCtx)
	}

	if rpcOpt.cfxBridgeEnabled { // start core space bridge RPC
		startNativeSpaceBridgeRpcServer(ctx, &wg)
	}

	cmdutil.GracefulShutdown(&wg, cancel)
}

// startNativeSpaceRpcServer starts core space RPC server
func startNativeSpaceRpcServer(ctx context.Context, wg *sync.WaitGroup, storeCtx storeContext) {
	router := node.Factory().CreateRouter()
	option := rpc.CfxAPIOption{
		Relayer: relay.MustNewTxnRelayerFromViper(),
	}

	// initialize store handler
	if storeCtx.cfxDB != nil {
		option.StoreHandler = handler.NewCfxCommonStoreHandler("db", storeCtx.cfxDB, option.StoreHandler)
	}

	if storeCtx.cfxCache != nil {
		option.StoreHandler = handler.NewCfxCommonStoreHandler("cache", storeCtx.cfxCache, option.StoreHandler)
	}

	// initialize gas station handler
	gasHandler := handler.NewGasStationHandler(storeCtx.cfxDB, storeCtx.cfxCache)

	if storeCtx.cfxDB != nil {
		// initialize pruned logs handler
		var prunedHandler *handler.CfxPrunedLogsHandler

		if redisUrl := viper.GetString("rpc.throttling.redisUrl"); len(redisUrl) > 0 {
			prunedHandler = handler.NewCfxPrunedLogsHandler(
				node.NewCfxClientProvider(router),
				storeCtx.cfxDB.UserStore,
				redis.MustNewRedisClient(redisUrl),
			)
		}

		// initialize logs api handler
		option.LogApiHandler = handler.NewCfxLogsApiHandler(storeCtx.cfxDB, prunedHandler)

		// periodically reload rate limit settings from db
		go rate.DefaultRegistryCfx.AutoReload(15*time.Second, storeCtx.cfxDB.LoadRateLimitConfigs)
	}

	// initialize RPC server
	exposedModules := viper.GetStringSlice("rpc.exposedModules")
	server := rpc.MustNewNativeSpaceServer(router, gasHandler, exposedModules, option)

	// serve HTTP endpoint
	httpEndpoint := viper.GetString("rpc.endpoint")
	go server.MustServeGraceful(ctx, wg, httpEndpoint, rpcutil.ProtocolHttp)

	// server Websocket endpoint
	if wsEndpoint := viper.GetString("rpc.wsEndpoint"); len(wsEndpoint) > 0 {
		go server.MustServeGraceful(ctx, wg, wsEndpoint, rpcutil.ProtocolWS)
	}
}

// startEvmSpaceRpcServer starts evm space RPC server
func startEvmSpaceRpcServer(ctx context.Context, wg *sync.WaitGroup, storeCtx storeContext) {
	var option rpc.EthAPIOption
	router := node.EthFactory().CreateRouter()

	if storeCtx.ethDB != nil {
		// initialize store handler
		option.StoreHandler = handler.NewEthStoreHandler(storeCtx.ethDB, nil)
		// initialize logs api handler
		option.LogApiHandler = handler.NewEthLogsApiHandler(storeCtx.ethDB)

		// periodically reload rate limit settings from db
		go rate.DefaultRegistryEth.AutoReload(10*time.Second, storeCtx.ethDB.LoadRateLimitConfigs)
	}

	// initialize RPC server
	exposedModules := viper.GetStringSlice("ethrpc.exposedModules")
	server := rpc.MustNewEvmSpaceServer(router, exposedModules, option)

	// serve HTTP endpoint
	httpEndpoint := viper.GetString("ethrpc.endpoint")
	go server.MustServeGraceful(ctx, wg, httpEndpoint, rpcutil.ProtocolHttp)

	// serve Websocket endpoint
	if wsEndpoint := viper.GetString("ethrpc.wsEndpoint"); len(wsEndpoint) > 0 {
		go server.MustServeGraceful(ctx, wg, wsEndpoint, rpcutil.ProtocolWS)
	}
}

// startNativeSpaceBridgeRpcServer starts core space bridge RPC server
func startNativeSpaceBridgeRpcServer(ctx context.Context, wg *sync.WaitGroup) {
	var config rpc.CfxBridgeServerConfig

	viperutil.MustUnmarshalKey("rpc.cfxBridge", &config)
	logrus.WithField("config", config).Info("Start to run cfx bridge rpc server")

	server := rpc.MustNewNativeSpaceBridgeServer(&config)
	go server.MustServeGraceful(ctx, wg, config.Endpoint, rpcutil.ProtocolHttp)
}
