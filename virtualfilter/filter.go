package virtualfilter

import (
	"errors"
	"strings"
	"time"

	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	"github.com/openweb3/go-rpc-provider"
	web3Types "github.com/openweb3/web3go/types"
)

type FilterType byte

const (
	// log filter, block filter and pending txn filter
	FilterTypeUnknown FilterType = iota
	FilterTypeLog
	FilterTypeBlock
	FilterTypePendingTxn
	FilterTypeLastIndex
)

var (
	errFilterNotFound = errors.New("filter not found")
)

// Filter is a helper struct that holds meta information over the filter type,
// log filter criterion and proxy full node delegation.
type Filter struct {
	typ      FilterType             // filter type
	deadline *time.Timer            // filter is inactive when deadline triggers
	crit     *web3Types.FilterQuery // log filter query
	del      *fnDelegateInfo        // full node delegate info
}

type fnDelegateInfo struct {
	fid      rpc.ID // filter ID by full node
	nodeName string // node name
}

// IsDelegateFullNode checks if the filter uses the full node with specified URL as
// the delegate full node
func (f *Filter) IsDelegateFullNode(nodeUrl string) bool {
	nodeName := rpcutil.Url2NodeName(nodeUrl)
	return strings.EqualFold(f.del.nodeName, nodeName)
}

// IsFilterNotFoundError check if error content contains `filter not found`
func IsFilterNotFoundError(err error) bool {
	if err != nil {
		errStr := strings.ToLower(err.Error())
		return strings.Contains(errStr, errFilterNotFound.Error())
	}

	return false
}
