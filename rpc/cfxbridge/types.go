package cfxbridge

import (
	"encoding/json"
	"math/big"
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

func (ebnh *EthBlockNumberOrHash) Number() (rpc.BlockNumber, bool) {
	if ebnh.hash == nil {
		return ebnh.number, true
	}

	return 0, false
}

func (ebnh *EthBlockNumberOrHash) Hash() (common.Hash, bool) {
	if ebnh.hash == nil {
		return common.Hash{}, false
	}

	return *ebnh.hash, true
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

func (req *EthCallRequest) ToCallMsg() ethTypes.TransactionArgs {
	var msg ethTypes.TransactionArgs

	msg.From = req.From.ValueOrNil()
	msg.To = req.To.ValueOrNil()
	msg.GasPrice = req.GasPrice
	msg.Gas = req.Gas
	msg.Value = req.Value
	msg.Nonce = req.Nonce

	if req.Data != nil {
		data := hexutil.Bytes(hexutil.MustDecode(*req.Data))
		msg.Data = &data
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
}

func (filter *EthLogFilter) ToFilterQuery() ethTypes.EthRpcLogFilter {
	var query ethTypes.EthRpcLogFilter

	if filter.FromEpoch != nil {
		query.FromBlock = big.NewInt(filter.FromEpoch.value.Int64())
	}

	if filter.ToEpoch != nil {
		query.ToBlock = big.NewInt(filter.ToEpoch.value.Int64())
	}

	query.BlockHash = filter.BlockHashes

	for i := range filter.Address {
		query.Addresses = append(query.Addresses, filter.Address[i].value)
	}

	query.Topics = filter.Topics

	return query
}
