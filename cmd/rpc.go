package cmd

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/rpc"
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/store/redis"
	"github.com/Conflux-Chain/confura/util/rate"
	"github.com/Conflux-Chain/confura/util/relay"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	vfclient "github.com/Conflux-Chain/confura/virtualfilter/client"
	viperutil "github.com/Conflux-Chain/go-conflux-util/viper"
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

	storeCtx := util.MustInitStoreContext()
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

	util.GracefulShutdown(&wg, cancel)
}

// startNativeSpaceRpcServer starts core space RPC server
func startNativeSpaceRpcServer(ctx context.Context, wg *sync.WaitGroup, storeCtx util.StoreContext) {
	var rateReg *rate.Registry

	router := node.Factory().CreateRouter()
	clientProvider := node.NewCfxClientProvider(storeCtx.CfxDB, router)
	relayer := relay.MustNewTxnRelayerFromViper()

	option := rpc.CfxAPIOption{
		TxnHandler: handler.MustNewCfxTxnHandler(relayer),
	}

	// initialize store handler
	if storeCtx.CfxDB != nil {
		option.StoreHandler = handler.NewCfxCommonStoreHandler("db", storeCtx.CfxDB, option.StoreHandler)

		rateKeyLoader := rate.NewKeyLoader(storeCtx.CfxDB.LoadRateLimitKeyset)
		rateReg = rate.NewRegistry(rateKeyLoader)

		// periodically reload rate limit settings from db
		go rateReg.AutoReload(15*time.Second, storeCtx.CfxDB.LoadRateLimitConfigs)
	}

	if storeCtx.CfxCache != nil {
		option.StoreHandler = handler.NewCfxCommonStoreHandler("cache", storeCtx.CfxCache, option.StoreHandler)
	}

	// initialize gas station handler
	gasHandler := handler.NewGasStationHandler(storeCtx.CfxDB, storeCtx.CfxCache)

	if storeCtx.CfxDB != nil {
		// initialize pruned logs handler
		var prunedHandler *handler.CfxPrunedLogsHandler

		if redisUrl := viper.GetString("rpc.throttling.redisUrl"); len(redisUrl) > 0 {
			prunedHandler = handler.NewCfxPrunedLogsHandler(
				clientProvider,
				storeCtx.CfxDB.UserStore,
				redis.MustNewRedisClient(redisUrl),
			)
		}

		// initialize logs api handler
		option.LogApiHandler = handler.NewCfxLogsApiHandler(storeCtx.CfxDB, prunedHandler)
	}

	// initialize RPC server
	exposedModules := viper.GetStringSlice("rpc.exposedModules")
	server := rpc.MustNewNativeSpaceServer(rateReg, clientProvider, gasHandler, exposedModules, option)

	// serve HTTP endpoint
	httpEndpoint := viper.GetString("rpc.endpoint")
	go server.MustServeGraceful(ctx, wg, httpEndpoint, rpcutil.ProtocolHttp)

	// serve Websocket endpoint
	if wsEndpoint := viper.GetString("rpc.wsEndpoint"); len(wsEndpoint) > 0 {
		go server.MustServeGraceful(ctx, wg, wsEndpoint, rpcutil.ProtocolWS)
	}

	// serve debug endpoint
	if debugEndpoint := viper.GetString("rpc.debugEndpoint"); len(debugEndpoint) > 0 {
		server := rpc.MustNewDebugServer()
		go server.MustServeGraceful(ctx, wg, debugEndpoint, rpcutil.ProtocolHttp)
	}
}

// startEvmSpaceRpcServer starts evm space RPC server
func startEvmSpaceRpcServer(ctx context.Context, wg *sync.WaitGroup, storeCtx util.StoreContext) {
	var rateReg *rate.Registry

	router := node.EthFactory().CreateRouter()
	clientProvider := node.NewEthClientProvider(storeCtx.EthDB, router)
	relayer := relay.MustNewEthTxnRelayerFromViper()

	option := rpc.EthAPIOption{
		TxnHandler:          handler.MustNewEthTxnHandler(relayer),
		VirtualFilterClient: vfclient.MustNewClientFromViper(),
	}

	if storeCtx.EthDB != nil {
		// initialize store handler
		option.StoreHandler = handler.NewEthStoreHandler(storeCtx.EthDB, nil)
		// initialize logs api handler
		option.LogApiHandler = handler.NewEthLogsApiHandler(storeCtx.EthDB)

		rateKeyLoader := rate.NewKeyLoader(storeCtx.EthDB.LoadRateLimitKeyset)
		rateReg = rate.NewRegistry(rateKeyLoader)

		// periodically reload rate limit settings from db
		go rateReg.AutoReload(15*time.Second, storeCtx.EthDB.LoadRateLimitConfigs)
	}

	// initialize RPC server
	exposedModules := viper.GetStringSlice("ethrpc.exposedModules")
	server := rpc.MustNewEvmSpaceServer(rateReg, clientProvider, exposedModules, option)

	// serve HTTP endpoint
	httpEndpoint := viper.GetString("ethrpc.endpoint")
	go server.MustServeGraceful(ctx, wg, httpEndpoint, rpcutil.ProtocolHttp)

	// serve Websocket endpoint
	if wsEndpoint := viper.GetString("ethrpc.wsEndpoint"); len(wsEndpoint) > 0 {
		go server.MustServeGraceful(ctx, wg, wsEndpoint, rpcutil.ProtocolWS)
	}

	// serve debug endpoint
	if debugEndpoint := viper.GetString("ethrpc.debugEndpoint"); len(debugEndpoint) > 0 {
		server := rpc.MustNewDebugServer()
		go server.MustServeGraceful(ctx, wg, debugEndpoint, rpcutil.ProtocolHttp)
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
