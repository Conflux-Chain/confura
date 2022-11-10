package virtualfilter

import (
	"context"

	"github.com/Conflux-Chain/confura/node"
	rpc "github.com/Conflux-Chain/confura/rpc"
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	web3rpc "github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go/types"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

var (
	errFilterProxyError = errors.New("filter proxy error")
)

// FilterSystem creates proxy log filter to fullnode, and instantly polls event logs from
// the full node to persist data in db or cache for high performance and stable log filter
// data retrieval service.
type FilterSystem struct {
	cfg      *Config
	lhandler *handler.EthLogsApiHandler
}

func NewFilterSystem(lhandler *handler.EthLogsApiHandler, conf *Config) *FilterSystem {
	return &FilterSystem{
		cfg: conf, lhandler: lhandler,
	}
}

// delegateNewFilter delegated to create a new filter from full node.
func (fs *FilterSystem) delegateNewFilter(client *node.Web3goClient) (*web3rpc.ID, error) {
	// TODO: create a shared delegate log filter instance to full node etc.,
	fid := web3rpc.NewID()
	return &fid, errors.New("not supported yet")
}

// delegateUninstallFilter delegated to uninstall a proxy filter from full node.
func (fs *FilterSystem) delegateUninstallFilter(id web3rpc.ID) (bool, error) {
	// TODO: delete delegate filer info from db/cache etc.,
	return false, errors.New("not supported yet")
}

// Logs returns the matching log entries from the blockchain node or db/cache store.
func (fs *FilterSystem) GetFilterLogs(w3c *node.Web3goClient, crit *web3Types.FilterQuery) ([]types.Log, error) {
	flag, ok := rpc.ParseEthLogFilterType(crit)
	if !ok {
		return ethEmptyLogs, rpc.ErrInvalidEthLogFilter
	}

	chainId, err := fs.lhandler.GetNetworkId(w3c.Eth)
	if err != nil {
		return ethEmptyLogs, errors.WithMessage(err, "failed to get chain ID")
	}

	hardforkBlockNum := util.GetEthHardforkBlockNumber(uint64(chainId))

	if err := rpc.NormalizeEthLogFilter(w3c.Client, flag, crit, hardforkBlockNum); err != nil {
		return ethEmptyLogs, err
	}

	if err := rpc.ValidateEthLogFilter(flag, crit); err != nil {
		return ethEmptyLogs, err
	}

	// return empty directly if filter block range before eSpace hardfork
	if crit.ToBlock != nil && *crit.ToBlock <= hardforkBlockNum {
		return ethEmptyLogs, nil
	}

	logs, hitStore, err := fs.lhandler.GetLogs(context.Background(), w3c.Client.Eth, crit)
	metrics.Registry.RPC.StoreHit("eth_getFilterLogs", "store").Mark(hitStore)

	if logs == nil { // uniform empty logs
		logs = ethEmptyLogs
	}

	return logs, err
}

// GetFilterChanges returns the matching log entries since last polling, and updates the filter cursor accordingly.
func (fs *FilterSystem) GetFilterChanges(w3c *node.Web3goClient, crit *web3Types.FilterQuery) ([]types.Log, error) {
	// TODO: get matching logs from db/cache
	return nil, errors.New("not supported yet")
}
