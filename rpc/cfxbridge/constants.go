package cfxbridge

import (
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/pkg/errors"
)

var (
	// ETH space only accept latest_state as block number. Other epoch tags, e.g. latest_checkpoint,
	// latest_confirmed and latest_mined are not supported.
	ErrEpochUnsupported = errors.New("epoch not supported")

	// ETH space only have pivot blocks as a list, and do not require to query non-pivot blocks.
	// To compatible with CFX space RPC, bridge service will check the assumption against pivot block.
	ErrInvalidBlockAssumption = errors.New("invalid block assumption")
)

var (
	HexBig0 = types.NewBigInt(0)
)

var (
	emptyDepositList       = []types.DepositInfo{}
	emptyVoteList          = []types.VoteStakeInfo{}
	emptyStorageChangeList = []types.StorageChange{}
)
