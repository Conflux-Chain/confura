package virtualfilter

import (
	"errors"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go/types"
	web3Types "github.com/openweb3/web3go/types"
)

var (
	errFilterProxyError = errors.New("filter proxy error")
)

// FilterSystem creates proxy log filter to fullnode, and instantly polls event logs from
// the full node to persist data in db or cache for high performance and stable log filter
// data retrieval service.
type FilterSystem struct {
	cfg         *Config
	logsHandler *handler.EthLogsApiHandler
}

func NewFilterSystem(lhandler *handler.EthLogsApiHandler, conf *Config) *FilterSystem {
	return &FilterSystem{
		cfg: conf, logsHandler: lhandler,
	}
}

// delegateNewFilter delegated to create a new filter from full node.
func (fs *FilterSystem) delegateNewFilter(client *node.Web3goClient) (rpc.ID, error) {
	// TODO: create a shared delegate log filter instance to full node etc.,
	return rpc.NewID(), errors.New("not supported yet")
}

// delegateUninstallFilter delegated to uninstall a proxy filter from full node.
func (fs *FilterSystem) delegateUninstallFilter(id rpc.ID) (bool, error) {
	// TODO: delete delegate filer info from db/cache etc.,
	return false, errors.New("not supported yet")
}

// Logs returns the matching log entries from the blockchain node or db/cache store.
func (fs *FilterSystem) GetLogs(w3c *node.Web3goClient, crit *web3Types.FilterQuery) ([]types.Log, error) {
	// TODO: use `handler.EthLogsApiHandler` to get logs from db or full node seperately.
	return nil, errors.New("not supported yet")
}

// GetFilterLogs returns the matching log entries since last polling, and updates the filter cursor accordingly.
func (fs *FilterSystem) GetFilterLogs(w3c *node.Web3goClient, crit *web3Types.FilterQuery) ([]types.Log, error) {
	// TODO: get matching logs from db/cache
	return nil, errors.New("not supported yet")
}
