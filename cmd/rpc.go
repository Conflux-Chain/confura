package cmd

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/rpc"
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/store/redis"
	"github.com/Conflux-Chain/confura/util/acl"
	"github.com/Conflux-Chain/confura/util/rate"
	"github.com/Conflux-Chain/confura/util/relay"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	viperutil "github.com/Conflux-Chain/confura/util/viper"
	vfclient "github.com/Conflux-Chain/confura/virtualfilter/client"
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
		RunE:  startRpcService,
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

func startRpcService(*cobra.Command, []string) error {
	if !rpcOpt.cfxEnabled && !rpcOpt.ethEnabled && !rpcOpt.cfxBridgeEnabled {
		return fmt.Errorf("no RPC server specified")
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	storeCtx := util.MustInitStoreContext()
	defer func() { cancel(); wg.Wait(); storeCtx.Close() }()

	if rpcOpt.cfxEnabled { // start core space RPC
		if err := startNativeSpaceRpcServer(ctx, &wg, storeCtx); err != nil {
			return err
		}
	}

	if rpcOpt.ethEnabled { // start evm space RPC
		if err := startEvmSpaceRpcServer(ctx, &wg, storeCtx); err != nil {
			return err
		}
	}

	if rpcOpt.cfxBridgeEnabled { // start core space bridge RPC
		if err := startNativeSpaceBridgeRpcServer(ctx, &wg, storeCtx); err != nil {
			return err
		}
	}

	util.GracefulShutdown(&wg, cancel)
	return nil
}

// startNativeSpaceRpcServer starts core space RPC server
func startNativeSpaceRpcServer(
	ctx context.Context,
	wg *sync.WaitGroup,
	storeCtx util.StoreContext,
) error {
	var rateReg *rate.Registry

	router := node.Factory().CreateRouter()
	clientProvider := node.NewCfxClientProvider(storeCtx.GetCfxCommonStore(), router)
	relayer := relay.MustNewTxnRelayerFromViper()

	option := rpc.CfxAPIOption{
		TxnHandler: handler.MustNewCfxTxnHandler(relayer),
	}

	if vfc, ok := vfclient.MustNewCfxClientFromViper(); ok {
		option.VirtualFilterClient = vfc
		logrus.Info("Virtual filter client enabled")
	}

	// initialize store handler
	if storeCtx.CfxDB != nil {
		option.StoreHandler = handler.NewCfxStoreHandler("db", storeCtx.CfxDB)

		var err error
		rateReg, err = startSecurityRegistry(ctx, wg, storeCtx.CfxDB.CommonStores, acl.NewCfxValidator)
		if err != nil {
			return err
		}
	}

	// initialize gas station handler
	gasHandler := handler.MustNewCfxGasStationHandlerFromViper(clientProvider)

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
		maxAttempts := viper.GetInt("requestControl.maxGetLogsSuggestionAttempts")
		option.LogApiHandler = handler.NewCfxLogsApiHandler(storeCtx.CfxDB, prunedHandler, maxAttempts)

		// initialize internal contract logs api handler
		option.InternalContractLogApiHandler = handler.MustNewCfxInternalContractLogsApiHandler(storeCtx.CfxDB, clientProvider)
	}

	// initialize RPC server
	exposedModules := viper.GetStringSlice("rpc.exposedModules")
	server := rpc.MustNewNativeSpaceServer(
		rateReg,
		clientProvider,
		gasHandler,
		exposedModules,
		rpcutil.NewPubSubLimiterFromViper(),
		option,
	)

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
	return nil
}

// startEvmSpaceRpcServer starts evm space RPC server
func startEvmSpaceRpcServer(
	ctx context.Context,
	wg *sync.WaitGroup,
	storeCtx util.StoreContext,
) error {
	var rateReg *rate.Registry

	router := node.EthFactory().CreateRouter()
	dataCache := rpcutil.MustNewEthDataCacheClientFromViper()
	clientProvider := node.NewEthClientProvider(dataCache, storeCtx.GetEthCommonStore(), router)
	relayer := relay.MustNewEthTxnRelayerFromViper()

	option := rpc.EthAPIOption{
		TxnHandler: handler.MustNewEthTxnHandler(relayer),
	}

	if vfc, ok := vfclient.MustNewEthClientFromViper(); ok {
		option.VirtualFilterClient = vfc
		logrus.Info("Virtual filter client enabled")
	}

	// initialize gas station handler
	gasHandler := handler.MustNewEthGasStationHandlerFromViper(clientProvider)

	if storeCtx.EthDB != nil {
		// initialize store handler
		option.StoreHandler = handler.NewEthStoreHandler(storeCtx.EthDB)
		// initialize logs api handler
		maxAttempts := viper.GetInt("requestControl.maxGetLogsSuggestionAttempts")
		option.LogApiHandler = handler.NewEthLogsApiHandler(storeCtx.EthDB, maxAttempts)

		var err error
		rateReg, err = startSecurityRegistry(ctx, wg, storeCtx.EthDB.CommonStores, acl.NewEthValidator)
		if err != nil {
			return err
		}
	}

	// initialize RPC server
	exposedModules := viper.GetStringSlice("ethrpc.exposedModules")
	server := rpc.MustNewEvmSpaceServer(
		rateReg,
		clientProvider,
		gasHandler,
		exposedModules,
		rpcutil.NewPubSubLimiterFromViper(),
		option,
	)

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
	return nil
}

// startNativeSpaceBridgeRpcServer starts core space bridge RPC server
func startNativeSpaceBridgeRpcServer(ctx context.Context, wg *sync.WaitGroup, storeCtx util.StoreContext) error {
	// Initialize ratelimit registry
	var rateReg *rate.Registry
	if storeCtx.CfxDB != nil {
		var err error
		rateReg, err = startSecurityRegistry(ctx, wg, storeCtx.CfxDB.CommonStores, acl.NewCfxValidator)
		if err != nil {
			return err
		}
	}

	var config rpc.CfxBridgeServerConfig

	viperutil.MustUnmarshalKey("rpc.cfxBridge", &config)
	logrus.WithField("config", config).Info("Start to run cfx bridge rpc server")

	server := rpc.MustNewNativeSpaceBridgeServer(rateReg, &config)
	go server.MustServeGraceful(ctx, wg, config.Endpoint, rpcutil.ProtocolHttp)
	return nil
}

func startSecurityRegistry(ctx context.Context, wg *sync.WaitGroup, db *mysql.CommonStores, factory acl.ValidatorFactory) (*rate.Registry, error) {
	timeout := 10 * time.Second
	if viper.IsSet("rpc.securityConfigTimeout") {
		timeout = viper.GetDuration("rpc.securityConfigTimeout")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("rpc.securityConfigTimeout must be positive")
	}
	keys := rate.NewKeyLoader(func(filter *rate.KeysetFilter) ([]*rate.KeyInfo, error) {
		queryCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return db.LoadRateLimitKeyInfosContext(queryCtx, filter)
	})
	registry := rate.NewRegistry(keys, factory)
	if err := registry.StartReload(ctx, wg, 15*time.Second, timeout, db.LoadRateLimitConfigsContext); err != nil {
		return nil, err
	}
	return registry, nil
}
