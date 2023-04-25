package virtualfilter

import (
	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/util"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	w3rpc "github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go/types"
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

func (api *ethFilterApi) GetLogFilter(fid w3rpc.ID) (*types.FilterQuery, error) {
	vf, ok := api.fs.getFilter(fid)
	if !ok || vf.ftype() != filterTypeLog {
		return nil, errFilterNotFound
	}

	ethf := vf.(*ethLogFilter)
	return &ethf.crit, nil
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
