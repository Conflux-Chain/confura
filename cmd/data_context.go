package cmd

import (
	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/store/redis"
	"github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/openweb3/web3go"
)

// storeContext context to hold store instances
type storeContext struct {
	cfxDB    *mysql.MysqlStore
	ethDB    *mysql.MysqlStore
	cfxCache *redis.RedisStore
}

func mustInitStoreContext() storeContext {
	var ctx storeContext

	// prepare core space db store
	if config := mysql.MustNewConfigFromViper(); config.Enabled {
		ctx.cfxDB = config.MustOpenOrCreate(mysql.StoreOption{
			Disabler: store.StoreConfig(),
		})
	}

	// prepare evm space db store
	if ethConfig := mysql.MustNewEthStoreConfigFromViper(); ethConfig.Enabled {
		ctx.ethDB = ethConfig.MustOpenOrCreate(mysql.StoreOption{
			Disabler: store.EthStoreConfig(),
		})
	}

	// prepare redis store
	if redis, ok := redis.MustNewRedisStoreFromViper(store.StoreConfig()); ok {
		ctx.cfxCache = redis
	}

	return ctx
}

func (ctx *storeContext) Close() {
	if ctx.cfxDB != nil {
		ctx.cfxDB.Close()
	}

	if ctx.ethDB != nil {
		ctx.ethDB.Close()
	}

	if ctx.cfxCache != nil {
		ctx.cfxCache.Close()
	}
}

// syncContext context to hold sdk clients for blockchain interoperation.
type syncContext struct {
	storeContext

	syncCfx *sdk.Client
	subCfx  *sdk.Client
	syncEth *web3go.Client
}

func mustInitSyncContext(storeCtx storeContext) syncContext {
	sc := syncContext{storeContext: storeCtx}

	if storeCtx.cfxDB != nil || storeCtx.cfxCache != nil {
		sc.syncCfx = rpc.MustNewCfxClientFromViper(rpc.WithClientHookMetrics(true))
		sc.subCfx = rpc.MustNewCfxWsClientFromViper()
	}

	if storeCtx.ethDB != nil {
		sc.syncEth = rpc.MustNewEthClientFromViper(rpc.WithClientHookMetrics(true))
	}

	return sc
}

func (ctx *syncContext) Close() {
	// Usually, storeContext will be defer closed by itself
	// ctx.storeContext.Close()
	if ctx.syncCfx != nil {
		ctx.syncCfx.Close()
	}

	if ctx.subCfx != nil {
		ctx.subCfx.Close()
	}

	// not provided yet!
	// ctx.syncEth.Close()
}
