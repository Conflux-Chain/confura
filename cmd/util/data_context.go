package util

import (
	"errors"
	"strings"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/store/redis"
	"github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/openweb3/web3go"
)

// StoreContext context to hold store instances
type StoreContext struct {
	CfxDB    *mysql.MysqlStore
	EthDB    *mysql.MysqlStore
	CfxCache *redis.RedisStore
}

func MustInitStoreContext() StoreContext {
	var ctx StoreContext

	// prepare core space db store
	if config := mysql.MustNewConfigFromViper(); config.Enabled {
		ctx.CfxDB = config.MustOpenOrCreate(mysql.StoreOption{
			Disabler: store.StoreConfig(),
		})
	}

	// prepare evm space db store
	if ethConfig := mysql.MustNewEthStoreConfigFromViper(); ethConfig.Enabled {
		ctx.EthDB = ethConfig.MustOpenOrCreate(mysql.StoreOption{
			Disabler: store.EthStoreConfig(),
		})
	}

	// prepare redis store
	if redis, ok := redis.MustNewRedisStoreFromViper(store.StoreConfig()); ok {
		ctx.CfxCache = redis
	}

	return ctx
}

func (ctx *StoreContext) Close() {
	if ctx.CfxDB != nil {
		ctx.CfxDB.Close()
	}

	if ctx.EthDB != nil {
		ctx.EthDB.Close()
	}

	if ctx.CfxCache != nil {
		ctx.CfxCache.Close()
	}
}

// GetMysqlStore returns MySQL store by network space
func (ctx *StoreContext) GetMysqlStore(network string) (store *mysql.MysqlStore, err error) {
	switch {
	case strings.EqualFold(network, "eth"):
		return ctx.EthDB, nil
	case strings.EqualFold(network, "cfx"):
		return ctx.CfxDB, nil
	default:
		return nil, errors.New("invalid network space (only `cfx` and `eth` acceptable)")
	}
}

// SyncContext context to hold sdk clients for blockchain interoperation.
type SyncContext struct {
	StoreContext

	SyncCfxs []*sdk.Client
	SyncEths []*web3go.Client
}

func MustInitSyncContext(storeCtx StoreContext) SyncContext {
	sc := SyncContext{StoreContext: storeCtx}

	if storeCtx.CfxDB != nil || storeCtx.CfxCache != nil {
		sc.SyncCfxs = rpc.MustNewCfxClientsFromViper(rpc.WithClientHookMetrics(true))
	}

	if storeCtx.EthDB != nil {
		sc.SyncEths = rpc.MustNewEthClientsFromViper(rpc.WithClientHookMetrics(true))
	}

	return sc
}

func (ctx *SyncContext) Close() {
	// Usually, storeContext will be defer closed by itself
	// ctx.storeContext.Close()

	for _, client := range ctx.SyncCfxs {
		client.Close()
	}

	for _, client := range ctx.SyncEths {
		client.Close()
	}
}
