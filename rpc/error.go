package rpc

import (
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/pkg/errors"
)

// rpc errors conform to fullnode

var errInvalidLogFilter = errors.Errorf(
	"Filter must provide one of the following: %v, %v, %v",
	"(1) an epoch range through `fromEpoch` and `toEpoch`",
	"(2) a block number range through `fromBlock` and `toBlock`",
	"(3) a set of block hashes through `blockHashes`",
)

func errExceedLogFilterBlockHashLimit(size int) error {
	return errors.Errorf(
		"filter.block_hashes can contain up to %v hashes; %v were provided.",
		store.MaxLogBlockHashesSize, size,
	)
}

var errInvalidLogFilterBlockRange = errors.New(
	"invalid block range (from block larger than to block)",
)

var errExceedLogFilterBlockRangeLimit = errExceedLogFilterBlockRangeSize(store.MaxLogBlockRange)

func errExceedLogFilterBlockRangeSize(size uint64) error {
	return errors.Errorf("block range exceeds maximum value %v", size)
}

var errInvalidLogFilterEpochRange = errors.New(
	"invalid epoch range (from epoch larger than to epoch)",
)

var errExceedLogFilterEpochRangeLimit = errors.Errorf(
	"epoch range exceeds maximum value %v", store.MaxLogEpochRange,
)

var errInvalidEthLogFilter = errors.Errorf(
	"Filter must provide one of the following: %v, %v",
	"(1) a block number range through `fromBlock` and `toBlock`",
	"(2) a set of block hashes through `blockHash`",
)
