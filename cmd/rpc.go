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

func startRpcService(cmd *cobra.Command, args []string) error {
	if !rpcOpt.cfxEnabled && !rpcOpt.ethEnabled && !rpcOpt.cfxBridgeEnabled {
		return fmt.Errorf("no RPC server specified")
	}

	life, err := util.NewLifecycle(cmd.Context())
	if err != nil {
		return err
	}
	defer life.Close()
	ctx, wg := life.Ctx, &life.Wg
	storeCtx, err := util.InitStoreContext("cfx", "eth")
	if err != nil {
		return err
	}
	defer func() { life.Shutdown(); storeCtx.Close() }()

	if rpcOpt.cfxEnabled { // start core space RPC
		if err := startNativeSpaceRpcServer(ctx, wg, storeCtx); err != nil {
			return err
		}
	}

	if rpcOpt.ethEnabled { // start evm space RPC
		if err := startEvmSpaceRpcServer(ctx, wg, storeCtx); err != nil {
			return err
		}
	}

	if rpcOpt.cfxBridgeEnabled { // start core space bridge RPC
		if err := startNativeSpaceBridgeRpcServer(ctx, wg, storeCtx); err != nil {
			return err
		}
	}

	life.Wait()
	return life.Err()
}

// startNativeSpaceRpcServer starts core space RPC server
func startNativeSpaceRpcServer(
	ctx context.Context,
	wg *sync.WaitGroup,
	storeCtx util.StoreContext,
) error {
	var cleanup []func()
	transferred := false
	defer func() {
		if !transferred {
			for i := len(cleanup) - 1; i >= 0; i-- {
				cleanup[i]()
			}
		}
	}()

	var rateReg *rate.Registry

	router, err := node.Factory().NewRouter()
	if err != nil {
		return err
	}
	clientProvider := node.NewCfxClientProvider(storeCtx.GetCfxCommonStore(), router)
	cleanup = append(cleanup, clientProvider.Close)
	relayer, err := relay.NewTxnRelayerFromViper(false)
	if err != nil {
		return err
	}
	cleanup = append(cleanup, relayer.Close)
	txnHandler, err := handler.NewCfxTxnHandler(relayer)
	if err != nil {
		return err
	}
	cleanup = append(cleanup, txnHandler.Close)
	option := rpc.CfxAPIOption{TxnHandler: txnHandler}

	if vfc, ok, err := vfclient.NewCfxClientFromViper(); err != nil {
		return err
	} else if ok {
		cleanup = append(cleanup, vfc.Close)
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
	gasHandler, err := handler.NewCfxGasStationHandlerFromViper(clientProvider)
	if err != nil {
		return err
	}
	if gasHandler != nil {
		cleanup = append(cleanup, gasHandler.Close)
	}

	if storeCtx.CfxDB != nil {
		// initialize pruned logs handler
		var prunedHandler *handler.CfxPrunedLogsHandler

		if redisUrl := viper.GetString("rpc.throttling.redisUrl"); len(redisUrl) > 0 {
			redisClient, err := redis.NewRedisClient(ctx, redisUrl)
			if err != nil {
				return err
			}
			cleanup = append(cleanup, func() { _ = redisClient.Close() })
			prunedHandler = handler.NewCfxPrunedLogsHandler(
				clientProvider,
				storeCtx.CfxDB.UserStore,
				redisClient,
			)
		}

		// initialize logs api handler
		maxAttempts := viper.GetInt("requestControl.maxGetLogsSuggestionAttempts")
		option.LogApiHandler = handler.NewCfxLogsApiHandler(storeCtx.CfxDB, prunedHandler, maxAttempts)

		// initialize internal contract logs api handler
		option.InternalContractLogApiHandler, err = handler.NewCfxInternalContractLogsHandlerFromStore(storeCtx.CfxDB, clientProvider)
		if err != nil {
			return err
		}
	}

	// initialize RPC server
	exposedModules := viper.GetStringSlice("rpc.exposedModules")
	server, err := rpc.NewNativeSpaceServer(
		rateReg,
		clientProvider,
		gasHandler,
		exposedModules,
		rpcutil.NewPubSubLimiterFromViper(),
		option,
	)

	if err != nil {
		return err
	}
	server.OnClose(cleanup...)
	transferred = true

	// serve HTTP endpoint
	httpEndpoint := viper.GetString("rpc.endpoint")
	if err := server.Start(ctx, wg, httpEndpoint, rpcutil.ProtocolHttp); err != nil {
		server.Close()
		return err
	}

	// serve Websocket endpoint
	if wsEndpoint := viper.GetString("rpc.wsEndpoint"); len(wsEndpoint) > 0 {
		if err := server.Start(ctx, wg, wsEndpoint, rpcutil.ProtocolWS); err != nil {
			// The registered HTTP task owns cleanup after the caller cancels startup.
			return err
		}
	}

	// serve debug endpoint
	if debugEndpoint := viper.GetString("rpc.debugEndpoint"); len(debugEndpoint) > 0 {
		server, err := rpc.NewDebugServer()
		if err != nil {
			return err
		}
		if err := server.Start(ctx, wg, debugEndpoint, rpcutil.ProtocolHttp); err != nil {
			server.Close()
			return err
		}
	}
	return nil
}

// startEvmSpaceRpcServer starts evm space RPC server
func startEvmSpaceRpcServer(
	ctx context.Context,
	wg *sync.WaitGroup,
	storeCtx util.StoreContext,
) error {
	var cleanup []func()
	transferred := false
	defer func() {
		if !transferred {
			for i := len(cleanup) - 1; i >= 0; i-- {
				cleanup[i]()
			}
		}
	}()

	var rateReg *rate.Registry

	router, err := node.EthFactory().NewRouter()
	if err != nil {
		return err
	}
	dataCache, err := rpcutil.NewEthDataCacheClientFromViper()
	if err != nil {
		if closer, ok := router.(interface{ Close() }); ok {
			closer.Close()
		}
		return err
	}
	clientProvider := node.NewEthClientProvider(dataCache, storeCtx.GetEthCommonStore(), router)
	cleanup = append(cleanup, clientProvider.Close)
	relayer, err := relay.NewTxnRelayerFromViper(true)
	if err != nil {
		return err
	}
	cleanup = append(cleanup, relayer.Close)
	txnHandler, err := handler.NewEthTxnHandler(relayer)
	if err != nil {
		return err
	}
	cleanup = append(cleanup, txnHandler.Close)
	option := rpc.EthAPIOption{TxnHandler: txnHandler}

	if vfc, ok, err := vfclient.NewEthClientFromViper(); err != nil {
		return err
	} else if ok {
		cleanup = append(cleanup, vfc.Close)
		option.VirtualFilterClient = vfc
		logrus.Info("Virtual filter client enabled")
	}

	// initialize gas station handler
	gasHandler, err := handler.NewEthGasStationHandlerFromViper(clientProvider)
	if err != nil {
		return err
	}
	if gasHandler != nil {
		cleanup = append(cleanup, gasHandler.Close)
	}

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
	server, err := rpc.NewEvmSpaceServer(
		rateReg,
		clientProvider,
		gasHandler,
		exposedModules,
		rpcutil.NewPubSubLimiterFromViper(),
		option,
	)

	if err != nil {
		return err
	}
	server.OnClose(cleanup...)
	transferred = true

	// serve HTTP endpoint
	httpEndpoint := viper.GetString("ethrpc.endpoint")
	if err := server.Start(ctx, wg, httpEndpoint, rpcutil.ProtocolHttp); err != nil {
		server.Close()
		return err
	}

	// serve Websocket endpoint
	if wsEndpoint := viper.GetString("ethrpc.wsEndpoint"); len(wsEndpoint) > 0 {
		if err := server.Start(ctx, wg, wsEndpoint, rpcutil.ProtocolWS); err != nil {
			// The registered HTTP task owns cleanup after the caller cancels startup.
			return err
		}
	}

	// serve debug endpoint
	if debugEndpoint := viper.GetString("ethrpc.debugEndpoint"); len(debugEndpoint) > 0 {
		server, err := rpc.NewDebugServer()
		if err != nil {
			return err
		}
		if err := server.Start(ctx, wg, debugEndpoint, rpcutil.ProtocolHttp); err != nil {
			server.Close()
			return err
		}
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

	if err := viperutil.UnmarshalKey("rpc.cfxBridge", &config); err != nil {
		return err
	}
	logrus.WithField("config", config).Info("Start to run cfx bridge rpc server")

	server, err := rpc.NewNativeSpaceBridgeServer(rateReg, &config)
	if err != nil {
		return err
	}
	if err := server.Start(ctx, wg, config.Endpoint, rpcutil.ProtocolHttp); err != nil {
		server.Close()
		return err
	}
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
		registry.Close()
		return nil, err
	}
	wg.Add(1)
	go func() { defer wg.Done(); <-ctx.Done(); registry.Close() }()
	return registry, nil
}
