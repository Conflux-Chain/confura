package rpc

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"

	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	lru "github.com/hashicorp/golang-lru"
	"github.com/openweb3/go-rpc-provider"
	web3Types "github.com/openweb3/web3go/types"
)

const delegateFilterCacheSize = 10000
const (
	// Filter type
	FilterTypeUnknown = iota
	FilterTypeLog
	FilterTypeBlock
	FilterTypePendingTxn
	FilterTypeLastIndex
)

var (
	// proxy filter ID => *ethDelegateFilter
	ethDelegateFilterCache, _ = lru.New(delegateFilterCacheSize)

	errFilterNotFound = errors.New("filter not found")
)

type ethDelegateFilter struct {
	ftype   int                    // filter type
	fid     rpc.ID                 // filter ID
	fq      *web3Types.FilterQuery // log filter query
	nodeUrl string                 // delegate fullnode URL
}

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

	pfid := genProxyFilterId(*fid, FilterTypeLog, w3c.URL)
	ethDelegateFilterCache.Add(pfid, &ethDelegateFilter{
		ftype: FilterTypeLog, fid: *fid, fq: &fq, nodeUrl: w3c.URL,
	})

	return pfid, nil
}

// NewBlockFilter creates a filter that fetches blocks that are imported into the chain.
// It is part of the filter package since polling goes with eth_getFilterChanges.
func (api *ethAPI) NewBlockFilter(ctx context.Context) (rpc.ID, error) {
	w3c := GetEthClientFromContext(ctx)

	fid, err := w3c.Filter.NewBlockFilter()
	if err != nil {
		return "", err
	}

	pfid := genProxyFilterId(*fid, FilterTypeBlock, w3c.URL)
	ethDelegateFilterCache.Add(pfid, &ethDelegateFilter{
		ftype: FilterTypeBlock, fid: *fid, nodeUrl: w3c.URL,
	})

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

	pfid := genProxyFilterId(*fid, FilterTypePendingTxn, w3c.URL)
	ethDelegateFilterCache.Add(pfid, &ethDelegateFilter{
		ftype: FilterTypePendingTxn, fid: *fid, nodeUrl: w3c.URL,
	})

	return *fid, nil
}

// UninstallFilter removes the filter with the given filter id.
func (api *ethAPI) UninstallFilter(ctx context.Context, fid rpc.ID) (bool, error) {
	w3c := GetEthClientFromContext(ctx)

	if v, ok := ethDelegateFilterCache.Get(fid); ok {
		ethDelegateFilterCache.Remove(fid)

		efilter := v.(*ethDelegateFilter)
		if strings.EqualFold(efilter.nodeUrl, w3c.URL) {
			return w3c.Filter.UninstallFilter(efilter.fid)
		}

		// if delegate fullnode already switched, the old delegate may be eliminated
		// due to unhealthy status. In this way, we don't wanna uninstall the filter
		// from the new allocated fullnode neither.
		return true, nil
	}

	return false, nil
}

// GetFilterChanges returns the logs for the filter with the given id since
// last time it was called. This can be used for polling.
//
// For pending transaction and block filters the result is []common.Hash.
// (pending)Log filters return []Log.
func (api *ethAPI) GetFilterChanges(ctx context.Context, fid rpc.ID) (interface{}, error) {
	w3c := GetEthClientFromContext(ctx)

	cv, ok := ethDelegateFilterCache.Get(fid)
	if !ok {
		return nil, errFilterNotFound
	}

	efilter := cv.(*ethDelegateFilter)
	if !strings.EqualFold(efilter.nodeUrl, w3c.URL) { // delegate fullnode switched?
		ethDelegateFilterCache.Remove(fid)
		return nil, errFilterNotFound
	}

	result, err := w3c.Filter.GetFilterChanges(efilter.fid)
	if detectFilterNotFoundError(err) {
		// lazy remove deprecated delegate filter
		ethDelegateFilterCache.Remove(fid)
	}

	return result, err
}

// GetFilterLogs returns the logs for the filter with the given id.
// If the filter could not be found an empty array of logs is returned.
func (api *ethAPI) GetFilterLogs(ctx context.Context, fid rpc.ID) ([]web3Types.Log, error) {
	w3c := GetEthClientFromContext(ctx)

	cv, ok := ethDelegateFilterCache.Get(fid)
	if !ok {
		return nil, errFilterNotFound
	}

	efilter := cv.(*ethDelegateFilter)
	if !strings.EqualFold(efilter.nodeUrl, w3c.URL) { // delegate fullnode switched?
		ethDelegateFilterCache.Remove(fid)
		return nil, errFilterNotFound
	}

	if efilter.ftype != FilterTypeLog { // wrong filter type
		return nil, errFilterNotFound
	}

	logs, delegated, err := api.getLogs(ctx, efilter.fq, false)

	logger := api.filterLogger(efilter.fq).WithField("fnDelegated", delegated)
	if err == nil {
		logger.Debug("`eth_getFilterLogs` RPC request handled")
		return logs, nil
	}

	if delegated && detectFilterNotFoundError(err) {
		// lazy remove deprecated delegate filter
		ethDelegateFilterCache.Remove(fid)
	} else {
		logger.WithError(err).Debug("Failed to handle `eth_getFilterLogs` RPC request")
	}

	return logs, err
}

// detectFilterNotFoundError detects filter not found error according to error content
func detectFilterNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "filter not found")
}

func genProxyFilterId(filterId rpc.ID, filterType int, nodeUrl string) rpc.ID {
	nodeName := rpcutil.Url2NodeName(nodeUrl)
	data := fmt.Sprintf("node:%s;fid:%s;type:%d", nodeName, filterId, filterType)

	// proxy filter ID = hash(node name + filter ID + filter type)

	h := fnv.New128()
	h.Write([]byte(data))
	b := h.Sum(nil)

	id := hex.EncodeToString(b)
	id = strings.TrimLeft(id, "0")
	if id == "" {
		id = "0" // ID's are RPC quantities, no leading zero's and 0 is 0x0.
	}

	return rpc.ID("0x" + id)
}
