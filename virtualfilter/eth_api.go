package virtualfilter

import (
	"context"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/rpc"
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	w3rpc "github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	nilRpcId = w3rpc.ID("0x0")
)

// EVM space filter API

type ethFilterApi struct {
	fs         *ethFilterSystem           // filter system
	logHandler *handler.EthLogsApiHandler // handler to get filter logs
	fnClients  util.ConcurrentMap         // full node clients: node name => sdk client
}

func newEthFilterApi(sys *ethFilterSystem, handler *handler.EthLogsApiHandler) *ethFilterApi {
	return &ethFilterApi{fs: sys, logHandler: handler}
}

func (api *ethFilterApi) NewBlockFilter(nodeUrl string) (w3rpc.ID, error) {
	client, err := api.loadOrGetFnClient(nodeUrl)
	if err != nil {
		return nilRpcId, err
	}

	return api.fs.newBlockFilter(client)
}

func (api *ethFilterApi) NewPendingTransactionFilter(nodeUrl string) (w3rpc.ID, error) {
	client, err := api.loadOrGetFnClient(nodeUrl)
	if err != nil {
		return nilRpcId, err
	}

	return api.fs.newPendingTransactionFilter(client)
}

func (api *ethFilterApi) UninstallFilter(id w3rpc.ID) (bool, error) {
	return api.fs.uninstallFilter(id)
}

func (api *ethFilterApi) NewFilter(nodeUrl string, crit types.FilterQuery) (w3rpc.ID, error) {
	client, err := api.loadOrGetFnClient(nodeUrl)
	if err != nil {
		return nilRpcId, err
	}

	return api.fs.newFilter(client, crit)
}

func (api *ethFilterApi) GetFilterLogs(id w3rpc.ID) ([]types.Log, error) {
	vf, ok := api.fs.getFilter(id)
	if !ok || vf.ftype() != filterTypeLog {
		return nil, errFilterNotFound
	}

	ethf := vf.(*ethLogFilter)
	crit, w3c := ethf.crit, ethf.client

	flag, ok := rpc.ParseEthLogFilterType(&crit)
	if !ok {
		return nil, rpc.ErrInvalidEthLogFilter
	}

	chainId, err := api.logHandler.GetNetworkId(w3c.Eth)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get chain ID")
	}

	hardforkBlockNum := util.GetEthHardforkBlockNumber(uint64(chainId))

	if err := rpc.NormalizeEthLogFilter(w3c.Client, flag, &crit, hardforkBlockNum); err != nil {
		return nil, err
	}

	if err := rpc.ValidateEthLogFilter(flag, &crit); err != nil {
		return nil, err
	}

	// return empty directly if filter block range before eSpace hardfork
	if crit.ToBlock != nil && *crit.ToBlock <= hardforkBlockNum {
		return nil, nil
	}

	logs, hitStore, err := api.logHandler.GetLogs(context.Background(), w3c.Eth, &crit, "eth_getFilterLogs")
	metrics.Registry.RPC.StoreHit("eth_getFilterLogs", "store").Mark(hitStore)

	return logs, err
}

func (api *ethFilterApi) GetFilterChanges(id w3rpc.ID) (*types.FilterChanges, error) {
	return api.fs.getFilterChanges(id)
}

func (api *ethFilterApi) loadOrGetFnClient(nodeUrl string) (*node.Web3goClient, error) {
	nodeName := rpcutil.Url2NodeName(nodeUrl)
	client, _, err := api.fnClients.LoadOrStoreFnErr(nodeName, func(interface{}) (interface{}, error) {
		client, err := rpcutil.NewEthClient(nodeUrl, rpcutil.WithClientHookMetrics(true))
		if err != nil {
			logrus.WithField("fnNodeUrl", nodeUrl).
				WithError(err).
				Error("Failed to new eth client for virtual filter")
			return nil, err
		}

		return &node.Web3goClient{Client: client, URL: nodeUrl}, nil
	})

	if err != nil {
		return nil, err
	}

	return client.(*node.Web3goClient), nil
}
