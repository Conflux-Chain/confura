package rpc

import (
	"math/bits"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

type LogFilterType int

const (
	// Log filter types
	LogFilterTypeBlockHash  LogFilterType = 1 << iota // 0001
	LogFilterTypeEpochRange                           // 0010
	LogFilterTypeBlockRange                           // 0100
)

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

	// if no explicit filter type detected, use block range filter type as default
	if flag == 0 {
		flag |= LogFilterTypeBlockRange
	}

	// different filter types are mutual exclusive
	if bits.OnesCount(uint(flag)) > 1 {
		return flag, false
	}

	return flag, true
}

func NormalizeEthLogFilter(
	resolver util.EthBlockNumberResolver, flag LogFilterType,
	filter *web3Types.FilterQuery, hardforkBlockNum web3Types.BlockNumber,
) error {
	if flag&LogFilterTypeBlockRange == 0 { // not a blockrange log filter
		return nil
	}

	// set default block range if not set and normalize block number if necessary
	defaultBlockNo := web3Types.LatestBlockNumber

	// if no from block provided, set latest block number as default
	if filter.FromBlock == nil {
		filter.FromBlock = &defaultBlockNo
	}

	// if no to block provided, set latest block number as default
	if filter.ToBlock == nil {
		filter.ToBlock = &defaultBlockNo
	}

	var blocks [2]*web3Types.BlockNumber
	for i, b := range []*web3Types.BlockNumber{filter.FromBlock, filter.ToBlock} {
		block, err := util.NormalizeEthBlockNumber(resolver, b, hardforkBlockNum)
		if err != nil {
			return errors.WithMessage(err, "failed to normalize block number")
		}

		blocks[i] = block
	}

	filter.FromBlock, filter.ToBlock = blocks[0], blocks[1]

	// deduplicate log filter if necessary
	dedupEthLogFilter(filter)
	return nil
}

func ValidateEthLogFilter(flag LogFilterType, filter *web3Types.FilterQuery) error {
	// different types of log filters are mutual exclusive
	if bits.OnesCount(uint(flag)) > 1 {
		return ErrInvalidEthLogFilter
	}

	if flag&LogFilterTypeBlockRange != 0 && *filter.FromBlock > *filter.ToBlock {
		return ErrInvalidLogFilterBlockRange
	}

	if len(filter.Addresses) > store.MaxLogFilterAddrCount {
		// num of filter address bounded
		return ErrExceedLogFilterAddrLimit(len(filter.Addresses))
	}

	if len(filter.Topics) > 4 {
		// num of filter topic dimensions bounded
		return ErrExceedLogFilterTopicDimension(len(filter.Topics))
	}

	for i := range filter.Topics {
		if len(filter.Topics[i]) > store.MaxLogFilterTopicCount {
			// num of filter topics per dimension bounded
			return ErrExceedLogFilterTopicLimit(len(filter.Topics[i]))
		}
	}

	return nil
}

func NormalizeLogFilter(cfx sdk.ClientOperator, flag LogFilterType, filter *types.LogFilter) error {
	// set default epoch range if not set and convert to numbered epoch if necessary
	if flag&LogFilterTypeEpochRange != 0 {
		// if no from epoch provided, set latest state epoch as default
		if filter.FromEpoch == nil {
			filter.FromEpoch = types.EpochLatestState
		}

		// if no to epoch provided, set latest state epoch as default
		if filter.ToEpoch == nil {
			filter.ToEpoch = types.EpochLatestState
		}

		var epochs [2]*types.Epoch
		for i, e := range []*types.Epoch{filter.FromEpoch, filter.ToEpoch} {
			epoch, err := util.ConvertToNumberedEpoch(cfx, e)
			if err != nil {
				return errors.WithMessagef(err, "failed to convert numbered epoch for %v", e)
			}

			epochs[i] = epoch
		}

		filter.FromEpoch, filter.ToEpoch = epochs[0], epochs[1]
	}

	// deduplicate log filter if necessary
	dedupLogFilter(filter)
	return nil
}

func ValidateLogFilter(flag LogFilterType, filter *types.LogFilter) error {
	switch {
	case flag&LogFilterTypeBlockHash != 0: // validate block hash log filter
		if len(filter.BlockHashes) > store.MaxLogBlockHashesSize {
			return ErrExceedLogFilterBlockHashLimit(len(filter.BlockHashes))
		}
	case flag&LogFilterTypeBlockRange != 0: // validate block range log filter
		// both fromBlock and toBlock must be provided
		if filter.FromBlock == nil || filter.ToBlock == nil {
			return ErrInvalidLogFilter
		}

		fromBlock := filter.FromBlock.ToInt().Uint64()
		toBlock := filter.ToBlock.ToInt().Uint64()

		if fromBlock > toBlock {
			return ErrInvalidLogFilterBlockRange
		}

	case flag&LogFilterTypeEpochRange != 0: // validate epoch range log filter
		epochFrom, _ := filter.FromEpoch.ToInt()
		epochTo, _ := filter.ToEpoch.ToInt()

		ef := epochFrom.Uint64()
		et := epochTo.Uint64()

		if ef > et {
			return ErrInvalidLogFilterEpochRange
		}
	}

	if len(filter.Address) > store.MaxLogFilterAddrCount {
		// num of filter address bounded
		return ErrExceedLogFilterAddrLimit(len(filter.Address))
	}

	if len(filter.Topics) > 4 {
		// num of filter topic dimensions bounded
		return ErrExceedLogFilterTopicDimension(len(filter.Topics))
	}

	for i := range filter.Topics {
		if len(filter.Topics[i]) > store.MaxLogFilterTopicCount {
			// num of filter topics per dimension bounded
			return ErrExceedLogFilterTopicLimit(len(filter.Topics[i]))
		}
	}

	return nil
}

// dedupLogFilter deduplicate log filter such as block hashes, contract addresses and topics.
func dedupLogFilter(filter *types.LogFilter) {
	// dedup block hashes
	var hashes []types.Hash

	dupset := make(map[string]bool)
	for _, hash := range filter.BlockHashes {
		if !dupset[string(hash)] {
			hashes = append(hashes, hash)
		}

		dupset[string(hash)] = true
	}

	if len(hashes) != len(filter.BlockHashes) {
		filter.BlockHashes = hashes
	}

	// depup contract addresses
	var addrs []types.Address

	dupset = make(map[string]bool)
	for _, addr := range filter.Address {
		if !dupset[addr.String()] {
			addrs = append(addrs, addr)
		}

		dupset[addr.String()] = true
	}

	if len(addrs) != len(filter.Address) {
		filter.Address = addrs
	}

	// dedup topics
	for i, topics := range filter.Topics {
		var dtopics []types.Hash

		dupset = make(map[string]bool)
		for _, topic := range topics {
			if !dupset[topic.String()] {
				dtopics = append(dtopics, topic)
			}

			dupset[topic.String()] = true
		}

		if len(dtopics) != len(topics) {
			filter.Topics[i] = dtopics
		}
	}
}

// dedupEthLogFilter deduplicate log filter such as contract addresses and topics.
func dedupEthLogFilter(filter *web3Types.FilterQuery) {
	// depup contract addresses
	var addrs []common.Address

	dupset := make(map[string]bool)
	for _, addr := range filter.Addresses {
		if !dupset[addr.String()] {
			addrs = append(addrs, addr)
		}

		dupset[addr.String()] = true
	}

	if len(addrs) != len(filter.Addresses) {
		filter.Addresses = addrs
	}

	// dedup topics
	for i, topics := range filter.Topics {
		var dtopics []common.Hash

		dupset = make(map[string]bool)
		for _, topic := range topics {
			if !dupset[topic.String()] {
				dtopics = append(dtopics, topic)
			}

			dupset[topic.String()] = true
		}

		if len(dtopics) != len(topics) {
			filter.Topics[i] = dtopics
		}
	}
}
