package rpc

import (
	"github.com/Conflux-Chain/confura/store"
	"github.com/pkg/errors"
)

// rpc errors conform to fullnode

var (
	ErrInvalidLogFilter = errors.Errorf(
		"Filter must provide one of the following: %v, %v, %v",
		"(1) an epoch range through `fromEpoch` and `toEpoch`",
		"(2) a block number range through `fromBlock` and `toBlock`",
		"(3) a set of block hashes through `blockHashes`",
	)

	ErrInvalidLogFilterBlockRange = errors.New(
		"invalid block range (from block larger than to block)",
	)

	ErrInvalidLogFilterEpochRange = errors.New(
		"invalid epoch range (from epoch larger than to epoch)",
	)

	ErrInvalidEthLogFilter = errors.Errorf(
		"Filter must provide one of the following: %v, %v",
		"(1) a block number range through `fromBlock` and `toBlock`",
		"(2) a set of block hashes through `blockHash`",
	)
)

func ErrExceedLogFilterBlockHashLimit(size int) error {
	return errors.Errorf(
		"filter.block_hashes can contain up to %v hashes; %v were provided.",
		store.MaxLogBlockHashesSize, size,
	)
}

func ErrExceedLogFilterAddrLimit(size int) error {
	return errors.Errorf(
		"filter.address can contain up to %v addresses; %v were provided.",
		store.MaxLogFilterAddrCount, size,
	)
}

func ErrExceedLogFilterTopicDimension(size int) error {
	return errors.Errorf("filter.topics must be no more than 4-dimensional array; %v were provided.", size)
}

func ErrExceedLogFilterTopicLimit(size int) error {
	return errors.Errorf(
		"filter.topics can contain up to  %v topics per dimension; %v were provided.",
		store.MaxLogFilterTopicCount, size,
	)
}
