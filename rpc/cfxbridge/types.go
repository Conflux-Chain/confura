package cfxbridge

import (
	"encoding/json"
	"strings"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	ethTypes "github.com/openweb3/web3go/types"
)

// EthBlockNumber accepts number and epoch tag latest_state, other values are invalid, e.g. latest_confirmed.
type EthBlockNumber struct {
	value rpc.BlockNumber
}

func (ebn *EthBlockNumber) ValueOrNil() *rpc.BlockNumber {
	if ebn == nil {
		return nil
	}

	return &ebn.value
}

func (ebn *EthBlockNumber) Value() rpc.BlockNumber {
	if ebn == nil {
		return rpc.LatestBlockNumber
	}

	return ebn.value
}

func (ebn *EthBlockNumber) ToArg() *rpc.BlockNumberOrHash {
	if ebn == nil {
		return nil
	}

	v := rpc.BlockNumberOrHashWithNumber(ebn.value)
	return &v
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (ebn *EthBlockNumber) UnmarshalJSON(data []byte) error {
	// Unmarshal as an epoch
	var epoch types.Epoch
	if err := epoch.UnmarshalJSON(data); err != nil {
		return err
	}

	// Supports hex, latest_state and earliest
	if num, ok := epoch.ToInt(); ok {
		ebn.value = rpc.BlockNumber(num.Int64())
	} else if types.EpochLatestState.Equals(&epoch) {
		ebn.value = rpc.LatestBlockNumber
	} else if types.EpochEarliest.Equals(&epoch) {
		ebn.value = rpc.EarliestBlockNumber
	} else {
		// Other values are all invalid
		return ErrEpochUnsupported
	}

	return nil
}

// EthBlockNumberOrHash accepts hex number, hash and epoch tag latest_state, other values are invalid, e.g. latest_confirmed.
type EthBlockNumberOrHash struct {
	number rpc.BlockNumber
	hash   *common.Hash
}

func (ebnh *EthBlockNumberOrHash) ToArg() *ethTypes.BlockNumberOrHash {
	if ebnh.hash == nil {
		v := rpc.BlockNumberOrHashWithNumber(ebnh.number)
		return &v
	}

	v := rpc.BlockNumberOrHashWithHash(*ebnh.hash, true)
	return &v
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (ebnh *EthBlockNumberOrHash) UnmarshalJSON(data []byte) error {
	// Unmarshal as an epoch
	var epoch types.Epoch
	if err := epoch.UnmarshalJSON(data); err != nil {
		return err
	}

	// Supports hex number
	if num, ok := epoch.ToInt(); ok {
		ebnh.number = rpc.BlockNumber(num.Int64())
		return nil
	}

	// Supports particular tags (latest_state and earliest) and hash
	switch {
	case types.EpochEarliest.Equals(&epoch):
		ebnh.number = rpc.EarliestBlockNumber
	case types.EpochLatestState.Equals(&epoch):
		ebnh.number = rpc.LatestBlockNumber
	case len(epoch.String()) == 66:
		blockHash := common.HexToHash(epoch.String())
		ebnh.hash = &blockHash
	default:
		return ErrEpochUnsupported
	}

	return nil
}

func (ebnh *EthBlockNumberOrHash) MarshalText() ([]byte, error) {
	if ebnh.hash == nil {
		return ebnh.number.MarshalText()
	}

	return ebnh.hash.MarshalText()
}

// EthAddress accepts both hex40 and base32 format addresses.
type EthAddress struct {
	value common.Address
}

func (ea *EthAddress) ValueOrNil() *common.Address {
	if ea == nil {
		return nil
	}

	return &ea.value
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (ea *EthAddress) UnmarshalJSON(data []byte) error {
	var addr string
	if err := json.Unmarshal(data, &addr); err != nil {
		return err
	}

	// If prefixed with 0x, decode in hex format.
	if strings.HasPrefix(addr, "0x") {
		ea.value = common.HexToAddress(addr)
		return nil
	}

	// Otherwise, decode in base32 format.
	cfxAddr, err := cfxaddress.NewFromBase32(addr)
	if err != nil {
		return err
	}

	if ea.value, _, err = cfxAddr.ToCommon(); err != nil {
		return err
	}

	return nil
}

// EthCallRequest is compatible with CFX CallRequest and accepts hex40 format address.
// Note, StorageLimit field is simply ignored.
type EthCallRequest struct {
	From     *EthAddress
	To       *EthAddress
	GasPrice *hexutil.Big
	Gas      *hexutil.Uint64
	Value    *hexutil.Big
	Nonce    *hexutil.Uint64
	Data     *string
}

func (req *EthCallRequest) ToCallMsg() ethTypes.CallRequest {
	msg := ethTypes.CallRequest{
		From: req.From.ValueOrNil(),
		To:   req.To.ValueOrNil(),
	}

	if req.GasPrice != nil {
		msg.GasPrice = req.GasPrice.ToInt()
	}

	if req.Gas != nil {
		msg.Gas = (*uint64)(req.Gas)
	}

	if req.Value != nil {
		msg.Value = req.Value.ToInt()
	}

	if req.Nonce != nil {
		msg.Nonce = (*uint64)(req.Nonce)
	}

	if req.Data != nil {
		msg.Data = hexutil.MustDecode(*req.Data)
	}

	return msg
}

// EthLogFilter is compatible with CFX LogFilter and accepts hex40 format address.
// Note, some fields are simply ignored, e.g. from/to block, offset/limit.
type EthLogFilter struct {
	FromEpoch   *EthBlockNumber
	ToEpoch     *EthBlockNumber
	BlockHashes *common.Hash // eth space only accept a single block hash as filter
	Address     []EthAddress
	Topics      [][]common.Hash
	Limit       *hexutil.Uint64
}

func (filter *EthLogFilter) ToFilterQuery() ethTypes.FilterQuery {
	query := ethTypes.FilterQuery{
		BlockHash: filter.BlockHashes,
		FromBlock: filter.FromEpoch.ValueOrNil(),
		ToBlock:   filter.ToEpoch.ValueOrNil(),
		Topics:    filter.Topics,
	}

	for i := range filter.Address {
		query.Addresses = append(query.Addresses, filter.Address[i].value)
	}

	if filter.Limit != nil {
		limit := uint(*filter.Limit)
		query.Limit = &limit
	}

	return query
}
