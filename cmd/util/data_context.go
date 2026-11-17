package util

import (
	"github.com/sirupsen/logrus"
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
	ctx, err := InitStoreContext("cfx", "eth")
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize stores")
	}
	return ctx
}

func InitStoreContext(networks ...string) (ctx StoreContext, err error) {
	defer func() {
		if err != nil {
			ctx.Close()
		}
	}()
	for _, network := range networks {
		config, e := mysql.NewConfigFromViper(network)
		if e != nil {
			return ctx, e
		}
		if !config.Enabled {
			continue
		}
		db, e := config.OpenOrCreate()
		if e != nil {
			return ctx, e
		}
		if strings.EqualFold(network, "cfx") {
			ctx.CfxDB = mysql.NewCfxStore(db, config, store.StoreConfig())
		} else {
			ctx.EthDB = mysql.NewEthStore(db, config, store.EthStoreConfig())
		}
	}
	return ctx, nil
}

func (ctx *StoreContext) Close() {
	if ctx.CfxDB != nil {
		ctx.CfxDB.Close()
	}

	if ctx.EthDB != nil {
		ctx.EthDB.Close()
	}
}

func (ctx *StoreContext) GetEthCommonStore() *mysql.CommonStores {
	store, _ := ctx.GetCommonStore("eth")
	return store
}

func (ctx *StoreContext) GetCfxCommonStore() *mysql.CommonStores {
	store, _ := ctx.GetCommonStore("cfx")
	return store
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
	return nil, nil
}

// SyncContext context to hold sdk clients for blockchain interoperation.
type SyncContext struct {
	StoreContext

	SyncCfxs []*sdk.Client
	SyncEths []*web3go.Client
}

func MustInitSyncContext(storeCtx StoreContext) SyncContext {
	sc, err := InitSyncContext(storeCtx)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize sync clients")
	}
	return sc
}

func InitSyncContext(storeCtx StoreContext) (sc SyncContext, err error) {
	sc.StoreContext = storeCtx
	defer func() {
		if err != nil {
			sc.Close()
		}
	}()
	if storeCtx.CfxDB != nil {
		sc.SyncCfxs, err = rpc.NewCfxClientsFromViper(rpc.WithClientHookMetrics(true))
		if err != nil {
			return sc, err
		}
	}
	if storeCtx.EthDB != nil {
		sc.SyncEths, err = rpc.NewEthClientsFromViper(rpc.WithClientHookMetrics(true))
		if err != nil {
			return sc, err
		}
	}
	return sc, nil
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

// OpenManagementStore opens only the selected space and owns no background tasks.
func OpenManagementStore(network string) (*mysql.CommonStores, error) {
	config, err := mysql.NewConfigFromViper(network)
	if err != nil {
		return nil, err
	}
	if !config.Enabled {
		return nil, errors.Errorf("%s database is unavailable", network)
	}
	db, err := config.Open()
	if err != nil {
		return nil, err
	}
	return mysql.NewCommonStores(db), nil
}
