package rpc

import (
	"fmt"

	cfxTypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethTypes "github.com/openweb3/web3go/types"
)

const (
	maxTraceFilterRange       uint64 = 1000
	maxTraceFilterBlockHashes        = 64
)

type traceEpochResolver interface {
	GetEpochNumber(...*cfxTypes.Epoch) (*hexutil.Big, error)
}

type traceBlockResolver interface {
	Resolve(ethTypes.BlockNumber) (uint64, error)
}

func validateCfxTraceFilter(resolver traceEpochResolver, filter cfxTypes.TraceFilter) error {
	// A non-null blockHashes array takes precedence over the epoch range, even when empty.
	if filter.BlockHashes != nil {
		if len(filter.BlockHashes) > maxTraceFilterBlockHashes {
			return fmt.Errorf("trace_filter blockHashes exceeds the maximum of %d hashes (%d provided)",
				maxTraceFilterBlockHashes, len(filter.BlockHashes))
		}
		return nil
	}
	if filter.FromEpoch == nil && filter.ToEpoch == nil {
		return nil
	}

	from, to := filter.FromEpoch, filter.ToEpoch
	if from == nil {
		from = cfxTypes.EpochLatestState
	}
	if to == nil {
		to = cfxTypes.EpochLatestState
	}

	resolve := func(epoch *cfxTypes.Epoch) (uint64, error) {
		if number, ok := epoch.ToInt(); ok {
			if number == nil || !number.IsUint64() {
				return 0, fmt.Errorf("trace_filter epoch %s is out of range", epoch)
			}
			return number.Uint64(), nil
		}
		value, err := resolver.GetEpochNumber(epoch)
		if err != nil {
			return 0, fmt.Errorf("trace_filter failed to resolve epoch %s: %w", epoch, err)
		}
		if value == nil {
			return 0, fmt.Errorf("trace_filter resolved epoch %s is out of range", epoch)
		}
		resolvedNumber := value.ToInt()
		if resolvedNumber == nil || !resolvedNumber.IsUint64() {
			return 0, fmt.Errorf("trace_filter resolved epoch %s is out of range", epoch)
		}
		number := resolvedNumber.Uint64()
		return number, nil
	}

	fromNumber, err := resolve(from)
	if err != nil {
		return err
	}
	toNumber := fromNumber
	if !from.Equals(to) {
		toNumber, err = resolve(to)
		if err != nil {
			return err
		}
	}
	return validateTraceFilterRange("epoch", fromNumber, toNumber)
}

func validateEthTraceFilter(resolver traceBlockResolver, filter ethTypes.TraceFilter) error {
	if filter.FromBlock == nil && filter.ToBlock == nil {
		return nil
	}
	from, to := filter.FromBlock, filter.ToBlock
	latest := ethTypes.LatestBlockNumber
	if from == nil {
		from = &latest
	}
	if to == nil {
		to = &latest
	}

	resolve := func(block ethTypes.BlockNumber) (uint64, error) {
		if block >= 0 {
			return uint64(block), nil
		}
		number, err := resolver.Resolve(block)
		if err != nil {
			return 0, fmt.Errorf("trace_filter failed to resolve block %s: %w", block, err)
		}
		return number, nil
	}

	fromNumber, err := resolve(*from)
	if err != nil {
		return err
	}
	toNumber := fromNumber
	if *from != *to {
		toNumber, err = resolve(*to)
		if err != nil {
			return err
		}
	}
	return validateTraceFilterRange("block", fromNumber, toNumber)
}

func validateTraceFilterRange(kind string, from, to uint64) error {
	if from > to {
		return fmt.Errorf("trace_filter invalid %s range: from %d exceeds to %d", kind, from, to)
	}
	// Subtract after checking the order to avoid overflow at MaxUint64.
	if to-from >= maxTraceFilterRange {
		return fmt.Errorf("trace_filter %s range exceeds the maximum of %d %ss", kind, maxTraceFilterRange, kind)
	}
	return nil
}
