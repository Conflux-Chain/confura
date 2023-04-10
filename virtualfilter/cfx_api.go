package virtualfilter

import (
	"context"

	"github.com/Conflux-Chain/confura/rpc"
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	w3rpc "github.com/openweb3/go-rpc-provider"
	"github.com/sirupsen/logrus"
)

// core space filter API

type cfxFilterApi struct {
	fs         *cfxFilterSystem           // filter system
	logHandler *handler.CfxLogsApiHandler // handler to get filter logs
	fnClients  util.ConcurrentMap         // full node clients: node name => sdk client
}

func newCfxFilterApi(sys *cfxFilterSystem, handler *handler.CfxLogsApiHandler) *cfxFilterApi {
	return &cfxFilterApi{fs: sys, logHandler: handler}
}

func (api *cfxFilterApi) NewBlockFilter(nodeUrl string) (w3rpc.ID, error) {
	client, err := api.loadOrGetFnClient(nodeUrl)
	if err != nil {
		return nilRpcId, err
	}

	return api.fs.newBlockFilter(client)
}

func (api *cfxFilterApi) NewPendingTransactionFilter(nodeUrl string) (w3rpc.ID, error) {
	client, err := api.loadOrGetFnClient(nodeUrl)
	if err != nil {
		return nilRpcId, err
	}

	return api.fs.newPendingTransactionFilter(client)
}

func (api *cfxFilterApi) UninstallFilter(id w3rpc.ID) (bool, error) {
	return api.fs.uninstallFilter(id)
}

func (api *cfxFilterApi) NewFilter(nodeUrl string, crit types.LogFilter) (w3rpc.ID, error) {
	client, err := api.loadOrGetFnClient(nodeUrl)
	if err != nil {
		return nilRpcId, err
	}

	return api.fs.newFilter(client, crit)
}

func (api *cfxFilterApi) GetFilterLogs(id w3rpc.ID) ([]types.Log, error) {
	vf, ok := api.fs.getFilter(id)
	if !ok || vf.ftype() != filterTypeLog {
		return nil, errFilterNotFound
	}

	lf := vf.(*cfxLogFilter)
	cfx, crit := lf.client, lf.crit

	flag, ok := rpc.ParseLogFilterType(&crit)
	if !ok {
		return nil, rpc.ErrInvalidLogFilter
	}

	if err := rpc.NormalizeLogFilter(cfx, flag, &crit); err != nil {
		return nil, err
	}

	if err := rpc.ValidateLogFilter(flag, &crit); err != nil {
		return nil, err
	}

	logs, hitStore, err := api.logHandler.GetLogs(context.Background(), cfx, &crit, "cfx_getFilterLogs")
	metrics.Registry.RPC.StoreHit("cfx_getFilterLogs", "store").Mark(hitStore)

	return logs, err
}

func (api *cfxFilterApi) GetFilterChanges(id w3rpc.ID) (*types.CfxFilterChanges, error) {
	return api.fs.getFilterChanges(id)
}

func (api *cfxFilterApi) loadOrGetFnClient(nodeUrl string) (*sdk.Client, error) {
	nodeName := rpcutil.Url2NodeName(nodeUrl)
	client, _, err := api.fnClients.LoadOrStoreFnErr(nodeName, func(interface{}) (interface{}, error) {
		client, err := rpcutil.NewCfxClient(nodeUrl, rpcutil.WithClientHookMetrics(true))
		if err != nil {
			logrus.WithField("fnNodeUrl", nodeUrl).
				WithError(err).
				Error("Failed to new cfx client for virtual filter")
			return nil, err
		}

		return client, nil
	})

	if err != nil {
		return nil, err
	}

	return client.(*sdk.Client), nil
}
