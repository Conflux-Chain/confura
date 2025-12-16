package util

import (
	"strings"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/openweb3/web3go"
	"github.com/pkg/errors"
)

// StoreContext context to hold store instances
type StoreContext struct {
	CfxDB *mysql.CfxStore
	EthDB *mysql.EthStore
}

func MustInitStoreContext() StoreContext {
	var ctx StoreContext

	// prepare core space db store
	if config := mysql.MustNewConfigFromViper(); config.Enabled {
		db := config.MustOpenOrCreate()
		ctx.CfxDB = mysql.NewCfxStore(db, config, store.StoreConfig())
	}

	// prepare evm space db store
	if ethConfig := mysql.MustNewEthStoreConfigFromViper(); ethConfig.Enabled {
		db := ethConfig.MustOpenOrCreate()
		ctx.EthDB = mysql.NewEthStore(db, ethConfig, store.EthStoreConfig())
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
}

func (ctx *StoreContext) GetCommonStore(network string) (*mysql.CommonStores, error) {
	switch {
	case strings.EqualFold(network, "eth"):
		if ctx.EthDB != nil {
			return ctx.EthDB.CommonStores, nil
		}
	case strings.EqualFold(network, "cfx"):
		if ctx.CfxDB != nil {
			return ctx.CfxDB.CommonStores, nil
		}
	default:
		return nil, errors.Errorf("invalid network space %s", network)
	}

	return nil, errors.New("mysql store is unavailable")
}

// SyncContext context to hold sdk clients for blockchain interoperation.
type SyncContext struct {
	StoreContext

	SyncCfxs []*sdk.Client
	SyncEths []*web3go.Client
}

func MustInitSyncContext(storeCtx StoreContext) SyncContext {
	sc := SyncContext{StoreContext: storeCtx}

	if storeCtx.CfxDB != nil {
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
