package rpc

import (
	"math/bits"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/openweb3/web3go"
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
	w3c *web3go.Client, flag LogFilterType,
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
		block, err := util.NormalizeEthBlockNumber(w3c, b, hardforkBlockNum)
		if err != nil {
			return errors.WithMessage(err, "failed to normalize block number")
		}

		blocks[i] = block
	}

	filter.FromBlock, filter.ToBlock = blocks[0], blocks[1]
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

	return nil
}
