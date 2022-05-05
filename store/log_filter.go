package store

import (
	"math"
	"math/bits"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/rpc/cfxbridge"
	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util"
	common "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/openweb3/web3go"
	web3Types "github.com/openweb3/web3go/types"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	// Log filter constants
	MaxLogBlockHashesSize int    = 128
	MaxLogEpochRange      uint64 = 1000
	MaxLogBlockRange      uint64 = 1000
	MaxLogLimit           uint64 = 5000 // TODO adjust max log limit accordingly

	// Log filter types
	LogFilterTypeBlockHash  LogFilterType = 1 << iota // 0001
	LogFilterTypeEpochRange                           // 0010
	LogFilterTypeBlockRange                           // 0100
)

var (
	TimeoutGetLogs = 3 * time.Second

	ErrGetLogsQuerySetTooLarge  = errors.New("query set is too large, please narrow down your filter condition")
	ErrGetLogsResultSetTooLarge = errors.Errorf(
		"result set to be queried is too large with more than %v logs, %v",
		MaxLogLimit, "please narrow down your filter condition",
	)
	ErrGetLogsTimeout = errors.Errorf(
		"query timeout with duration exceeds %v(s)", TimeoutGetLogs,
	)
)

type LogFilterType int

func ParseLogFilterType(filter *types.LogFilter) (LogFilterType, bool) {
	// filter type set flag bitwise
	var flag LogFilterType

	// check if epoch range provided
	if filter.FromEpoch != nil || filter.ToEpoch != nil {
		flag |= LogFilterTypeEpochRange
	}

	// check if block range provided
	if filter.FromBlock != nil || filter.ToBlock != nil {
		flag |= LogFilterTypeBlockRange
	}

	// check if block hashes provided
	if len(filter.BlockHashes) != 0 {
		flag |= LogFilterTypeBlockHash
	}

	// different types of log filters are mutual exclusion
	if bits.OnesCount(uint(flag)) > 1 {
		return flag, false
	}

	// if no explicit filter type detected, use epoch range filter type as default
	if flag == 0 {
		flag |= LogFilterTypeEpochRange
	}

	return flag, true
}

func ParseEthLogFilterType(filter *web3Types.FilterQuery) (LogFilterType, bool) {
	// filter type set flag bitwise
	var flag LogFilterType

	// check if block range provided
	if filter.FromBlock != nil || filter.ToBlock != nil {
		flag |= LogFilterTypeBlockRange
	}

	// check if block hash provided
	if filter.BlockHash != nil {
		flag |= LogFilterTypeBlockHash
	}

	// different types of log filters are mutual exclusion
	if bits.OnesCount(uint(flag)) > 1 {
		return flag, false
	}

	// if no explicit filter type detected, use block range filter type as default
	if flag == 0 {
		flag |= LogFilterTypeBlockRange
	}

	return flag, true
}

type logFilterRangeFetcher interface {
	// fetchs epoch from fullnode by block hash.
	FetchEpochByBlockHash(blockHash string) (uint64, error)
	// fetchs epoch range from fullnode by from and to block number.
	FetchEpochRangeByBlockNumber(filter *LogFilter) (citypes.RangeUint64, error)
}

type cfxRangeFetcher struct {
	cfx sdk.ClientOperator
}

func newCfxRangeFetcher(cfx sdk.ClientOperator) *cfxRangeFetcher {
	return &cfxRangeFetcher{cfx: cfx}
}

// implements logFilterRangeFetcher

func (fetcher *cfxRangeFetcher) FetchEpochByBlockHash(blockHash string) (uint64, error) {
	block, err := fetcher.cfx.GetBlockSummaryByHash(types.Hash(blockHash))
	if err != nil {
		return 0, err
	}

	if block == nil {
		return 0, errors.New("unknown block")
	}

	return block.EpochNumber.ToInt().Uint64(), nil
}

func (fetcher *cfxRangeFetcher) FetchEpochRangeByBlockNumber(filter *LogFilter) (citypes.RangeUint64, error) {
	res := citypes.RangeUint64{From: math.MaxUint64, To: 0}

	if filter.BlockRange == nil {
		return res, nil
	}

	blockNumbers := make([]hexutil.Uint64, 0, 2)
	blockNumbers = append(blockNumbers, hexutil.Uint64(filter.BlockRange.From))
	if filter.BlockRange.From != filter.BlockRange.To {
		blockNumbers = append(blockNumbers, hexutil.Uint64(filter.BlockRange.To))
	}

	blockSummarys, err := fetcher.cfx.BatchGetBlockSummarysByNumber(blockNumbers)
	if len(blockSummarys) < len(blockNumbers) {
		err = errors.Errorf("missing block summary(s) returned, expect count %v got %v", len(blockNumbers), len(blockSummarys))
	}

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"blockNumbers": blockNumbers, "blockSummarys": blockSummarys,
		}).Error("Failed to batch get block summarys by from/to block number")
		return res, errors.WithMessage(err, "failed to batch get block summarys by from && to block number")
	}

	for _, bs := range blockSummarys {
		epochNo := bs.EpochNumber.ToInt().Uint64()
		res.From = util.MinUint64(res.From, epochNo)
		res.To = util.MaxUint64(res.To, epochNo)
	}

	logrus.WithFields(logrus.Fields{
		"blockSummarys": blockSummarys, "blockNumbers": blockNumbers, "epochRange": res,
	}).Debug("Fetched epoch range from fullnode by log filter")

	return res, nil
}

type ethRangeFetcher struct {
	w3c *web3go.Client
}

func newEthRangeFetcher(w3c *web3go.Client) *ethRangeFetcher {
	return &ethRangeFetcher{w3c: w3c}
}

// implements logFilterRangeFetcher

func (fetcher *ethRangeFetcher) FetchEpochByBlockHash(blockHash string) (uint64, error) {
	ethBlockHash := common.HexToHash(blockHash)
	block, err := fetcher.w3c.Eth.BlockByHash(ethBlockHash, false)
	if err != nil {
		return 0, err
	}

	if block == nil {
		return 0, errors.New("unknown block")
	}

	return block.Number.Uint64(), nil
}

func (fetcher *ethRangeFetcher) FetchEpochRangeByBlockNumber(filter *LogFilter) (citypes.RangeUint64, error) {
	res := citypes.RangeUint64{From: math.MaxUint64, To: 0}

	if filter.BlockRange == nil {
		return res, nil
	}

	return *filter.BlockRange, nil
}

// LogFilter is used to filter logs when query in any store.
type LogFilter struct {
	rangeFetcher logFilterRangeFetcher
	Type         LogFilterType
	EpochRange   *citypes.RangeUint64
	BlockRange   *citypes.RangeUint64
	Contracts    VariadicValue
	BlockHashId  uint64
	BlockHash    string
	Topics       []VariadicValue // event hash and indexed data 1, 2, 3
	OffSet       uint64
	Limit        uint64
}

// ParseLogFilter creates an instance of LogFilter with specified RPC log filter.
func ParseLogFilter(cfx sdk.ClientOperator, filter *types.LogFilter) (result *LogFilter, ok bool) {
	switch {
	case filter.FromEpoch != nil && filter.ToEpoch != nil:
		for _, epoch := range [2]*types.Epoch{filter.FromEpoch, filter.ToEpoch} {
			if _, ok := epoch.ToInt(); !ok {
				return nil, false
			}
		}

		result, ok = NewLogFilter(LogFilterTypeEpochRange, filter), true
	case filter.FromBlock != nil && filter.ToBlock != nil:
		result, ok = NewLogFilter(LogFilterTypeBlockRange, filter), true
	case len(filter.BlockHashes) == 1:
		result, ok = NewLogFilter(LogFilterTypeBlockHash, filter), true
	}

	if ok {
		result.rangeFetcher = newCfxRangeFetcher(cfx)
	}

	return result, ok
}

// ParseEthLogFilter creates an instance of LogFilter with specified ETH RPC log filter.
func ParseEthLogFilter(w3c *web3go.Client, ethNetworkId uint32, filter *web3Types.FilterQuery) (result *LogFilter, ok bool) {
	switch {
	case filter.BlockHash != nil:
		result, ok = NewEthLogFilter(LogFilterTypeBlockHash, ethNetworkId, filter), true
	case filter.FromBlock != nil && filter.ToBlock != nil:
		result, ok = NewEthLogFilter(LogFilterTypeBlockRange, ethNetworkId, filter), true
	}

	if ok {
		result.rangeFetcher = newEthRangeFetcher(w3c)
	}

	return result, ok
}

// NewLogFilter creates an instance of LogFilter with specified RPC log filter.
func NewLogFilter(filterType LogFilterType, filter *types.LogFilter) *LogFilter {
	result := &LogFilter{
		Type:      filterType,
		Contracts: newVariadicValueByAddress(filter.Address),
		OffSet:    0,
		Limit:     MaxLogLimit,
	}

	switch filterType {
	case LogFilterTypeEpochRange:
		fromEpoch, ok1 := filter.FromEpoch.ToInt()
		toEpoch, ok2 := filter.ToEpoch.ToInt()

		if ok1 && ok2 {
			result.EpochRange = &citypes.RangeUint64{
				From: fromEpoch.Uint64(),
				To:   toEpoch.Uint64(),
			}
		}
	case LogFilterTypeBlockRange:
		result.BlockRange = &citypes.RangeUint64{
			From: filter.FromBlock.ToInt().Uint64(),
			To:   filter.ToBlock.ToInt().Uint64(),
		}
	case LogFilterTypeBlockHash:
		result.BlockHash = filter.BlockHashes[0].String()
		result.BlockHashId = util.GetShortIdOfHash(result.BlockHash)
	}

	// init topics filter
	for _, v := range filter.Topics {
		result.Topics = append(result.Topics, newVariadicValueByHashes(v))
	}

	// remove empty topic filter at tail
	for len(result.Topics) > 0 && result.Topics[len(result.Topics)-1].IsNull() {
		result.Topics = result.Topics[:len(result.Topics)-1]
	}

	// init offset filter
	if filter.Offset != nil {
		result.OffSet = uint64(*filter.Offset)
	}

	// init limit filter
	if filter.Limit != nil && uint64(*filter.Limit) < MaxLogLimit {
		result.Limit = uint64(*filter.Limit)
	}

	return result
}

// NewEthLogFilter creates an instance of LogFilter with specified ETH RPC log filter.
func NewEthLogFilter(filterType LogFilterType, ethNetworkId uint32, filter *web3Types.FilterQuery) *LogFilter {
	cfxLogFilter := cfxbridge.ConvertLogFilter(filter, ethNetworkId)
	return NewLogFilter(filterType, cfxLogFilter)
}

// FetchEpochByBlockHash fetchs epoch from fullnode by blockhash.
func (filter *LogFilter) FetchEpochByBlockHash() (uint64, error) {
	return filter.rangeFetcher.FetchEpochByBlockHash(filter.BlockHash)
}

// FetchEpochRangeByBlockNumber fetchs epoch range from fullnode by from and to block number.
func (filter *LogFilter) FetchEpochRangeByBlockNumber() (citypes.RangeUint64, error) {
	return filter.rangeFetcher.FetchEpochRangeByBlockNumber(filter)
}
