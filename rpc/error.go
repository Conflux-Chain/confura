package rpc

import (
	"github.com/pkg/errors"
	"github.com/scroll-tech/rpc-gateway/store"
)

// rpc errors conform to fullnode

var (
	errInvalidLogFilter = errors.Errorf(
		"Filter must provide one of the following: %v, %v, %v",
		"(1) an epoch range through `fromEpoch` and `toEpoch`",
		"(2) a block number range through `fromBlock` and `toBlock`",
		"(3) a set of block hashes through `blockHashes`",
	)

	errInvalidLogFilterBlockRange = errors.New(
		"invalid block range (from block larger than to block)",
	)

	errInvalidLogFilterEpochRange = errors.New(
		"invalid epoch range (from epoch larger than to epoch)",
	)

	errInvalidEthLogFilter = errors.Errorf(
		"Filter must provide one of the following: %v, %v",
		"(1) a block number range through `fromBlock` and `toBlock`",
		"(2) a set of block hashes through `blockHash`",
	)
)

func errExceedLogFilterBlockHashLimit(size int) error {
	return errors.Errorf(
		"filter.block_hashes can contain up to %v hashes; %v were provided.",
		store.MaxLogBlockHashesSize, size,
	)
}
