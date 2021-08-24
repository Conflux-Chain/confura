package rpc

import (
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/pkg/errors"
)

// rpc errors conform to fullnode

var (
	errInvalidLogFilter = errors.New("Filter must provide one of the following: (1) an epoch range through `fromEpoch` and `toEpoch`, (2) a block number range through `fromBlock` and `toBlock`, (3) a set of block hashes through `blockHashes`")
)

func errExceedLogFilterBlockHashLimit(size int) error {
	return errors.Errorf("filter.block_hashes can contain up to %v hashes; %v were provided.", store.MaxLogBlockHashesSize, size)
}
