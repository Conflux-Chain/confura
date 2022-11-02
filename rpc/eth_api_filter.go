package rpc

import (
	"context"
	"errors"
	"fmt"
	"strings"

	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	lru "github.com/hashicorp/golang-lru"
	"github.com/openweb3/go-rpc-provider"
	web3Types "github.com/openweb3/web3go/types"
)

const (
	defaultFilterQueryCacheSize = 5000
)

var (
	// (fullnode name + filter ID) => *web3Types.FilterQuery
	ethFilterQueryCache, _ = lru.New(defaultFilterQueryCacheSize)

	errFilterNotFound = errors.New("filter not found")
)

// NewFilter creates a new filter and returns the filter id. It can be
// used to retrieve logs when the state changes. This method cannot be
// used to fetch logs that are already stored in the state.
//
// Default criteria for the from and to block are "latest".
// Using "latest" as block number will return logs for mined blocks.
// Using "pending" as block number is not supported.
// In case logs are removed (chain reorg) previously returned logs are returned
// again but with the removed property set to true.
//
// In case "fromBlock" > "toBlock" an error is returned.
func (api *ethAPI) NewFilter(ctx context.Context, fq web3Types.FilterQuery) (rpc.ID, error) {
	w3c := GetEthClientFromContext(ctx)

	fid, err := w3c.Filter.NewLogFilter(&fq)
	if err != nil {
		return "", err
	}

	cachekey := ethFilterQueryCacheKey(w3c.URL, *fid)
	ethFilterQueryCache.Add(cachekey, &fq)

	return *fid, nil
}

// NewBlockFilter creates a filter that fetches blocks that are imported into the chain.
// It is part of the filter package since polling goes with eth_getFilterChanges.
func (api *ethAPI) NewBlockFilter(ctx context.Context) (rpc.ID, error) {
	w3c := GetEthClientFromContext(ctx)

	fid, err := w3c.Filter.NewBlockFilter()
	if err != nil {
		return "", err
	}

	return *fid, nil
}

// NewPendingTransactionFilter creates a filter that fetches pending transaction hashes
// as transactions enter the pending state.
//
// It is part of the filter package because this filter can be used through the
// `eth_getFilterChanges` polling method that is also used for log filters.
func (api *ethAPI) NewPendingTransactionFilter(ctx context.Context) (rpc.ID, error) {
	w3c := GetEthClientFromContext(ctx)

	fid, err := w3c.Filter.NewPendingTransactionFilter()
	if err != nil {
		return "", err
	}

	return *fid, nil
}

// UninstallFilter removes the filter with the given filter id.
func (api *ethAPI) UninstallFilter(ctx context.Context, fid rpc.ID) (bool, error) {
	w3c := GetEthClientFromContext(ctx)

	// remove from the LRU cache if existed
	cachekey := ethFilterQueryCacheKey(w3c.URL, fid)
	ethFilterQueryCache.Remove(cachekey)

	return w3c.Filter.UninstallFilter(fid)
}

// GetFilterChanges returns the logs for the filter with the given id since
// last time it was called. This can be used for polling.
//
// For pending transaction and block filters the result is []common.Hash.
// (pending)Log filters return []Log.
func (api *ethAPI) GetFilterChanges(ctx context.Context, fid rpc.ID) (interface{}, error) {
	w3c := GetEthClientFromContext(ctx)

	// refresh the LRU cache
	cachekey := ethFilterQueryCacheKey(w3c.URL, fid)
	_, ok := ethFilterQueryCache.Get(cachekey)

	res, err := w3c.Filter.GetFilterChanges(fid)
	if detectFilterNotFoundError(err) && ok {
		// remove cache if filter not found on full node any more
		ethFilterQueryCache.Remove(cachekey)
	}

	return res, err
}

// GetFilterLogs returns the logs for the filter with the given id.
// If the filter could not be found an empty array of logs is returned.
func (api *ethAPI) GetFilterLogs(ctx context.Context, fid rpc.ID) ([]web3Types.Log, error) {
	w3c := GetEthClientFromContext(ctx)

	cachekey := ethFilterQueryCacheKey(w3c.URL, fid)
	v, ok := ethFilterQueryCache.Get(cachekey)
	if !ok {
		return ethEmptyLogs, errFilterNotFound
	}

	fq := v.(*web3Types.FilterQuery)
	logs, delegated, err := api.getLogs(ctx, fq, true)

	logger := api.filterLogger(fq).WithField("fnDelegated", delegated)

	if err != nil {
		logger.WithError(err).Debug("Failed to handle `eth_getFilterLogs` RPC request")
		return logs, err
	}

	logger.Debug("`eth_getFilterLogs` RPC request handled")
	return logs, nil
}

// ethFilterQueryCacheKey assembles filter query cache key for specified filter ID and fullnode
func ethFilterQueryCacheKey(nodeUrl string, fid rpc.ID) string {
	fn := rpcutil.Url2NodeName(string(nodeUrl))
	return fmt.Sprintf("%s/%s", fn, fid)
}

// detectFilterNotFoundError detects `filter not found` error according to error content
func detectFilterNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "filter not found")
}
