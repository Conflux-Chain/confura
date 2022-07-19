package cfxbridge

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
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

func (ebn *EthBlockNumber) ToArg() *ethTypes.BlockNumberOrHash {
	if ebn == nil {
		return nil
	}

	v := ethTypes.BlockNumberOrHashWithNumber(ebn.value)
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
		v := ethTypes.BlockNumberOrHashWithNumber(ebnh.number)
		return &v
	}

	v := ethTypes.BlockNumberOrHashWithHash(*ebnh.hash, true)
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

// UnmarshalJSON implements the json.Unmarshaler interface.
func (l *EthLogFilter) UnmarshalJSON(data []byte) error {
	type tmpLogFilter struct {
		FromEpoch   *EthBlockNumber `json:"fromEpoch,omitempty"`
		ToEpoch     *EthBlockNumber `json:"toEpoch,omitempty"`
		BlockHashes *common.Hash    `json:"blockHashes,omitempty"`
		Address     interface{}     `json:"address,omitempty"`
		Topics      []interface{}   `json:"topics,omitempty"`
	}

	t := tmpLogFilter{}
	if err := json.Unmarshal(data, &t); err != nil {
		return err
	}

	var err error
	l.FromEpoch = t.FromEpoch
	l.ToEpoch = t.ToEpoch
	l.BlockHashes = t.BlockHashes
	if l.Address, err = resolveToAddresses(t.Address); err != nil {
		return err
	}
	if l.Topics, err = resolveToTopicsList(t.Topics); err != nil {
		return err
	}

	return nil
}

func resolveToAddresses(val interface{}) ([]EthAddress, error) {
	// if val is nil, return
	if val == nil {
		return nil, nil
	}

	// if val is string, new address and return
	if addrStr, ok := val.(string); ok {
		var addr EthAddress
		if err := json.Unmarshal([]byte(fmt.Sprintf(`"%v"`, addrStr)), &addr); err != nil {
			return nil, errors.Wrapf(err, "failed to create address by %v", addrStr)
		}

		return []EthAddress{addr}, nil
	}

	// if val is string slice, new every item to cfxaddress
	if addrStrList, ok := val.([]interface{}); ok {
		addrList := make([]EthAddress, 0)
		for _, v := range addrStrList {
			vStr, ok := v.(string)
			if !ok {
				return nil, errors.Errorf("could not conver type %v to address", reflect.TypeOf(v))
			}

			var addr EthAddress
			if err := json.Unmarshal([]byte(fmt.Sprintf(`"%v"`, vStr)), &addr); err != nil {
				return nil, errors.Wrapf(err, "failed to create address by %v", v)
			}

			addrList = append(addrList, addr)
		}

		return addrList, nil
	}

	return nil, errors.Errorf("failed to unmarshal %#v to address or address list", val)
}

func resolveToTopicsList(val []interface{}) ([][]common.Hash, error) {
	// if val is nil, return
	if val == nil {
		return nil, nil
	}

	// otherwise, convert every item to topics
	topicsList := make([][]common.Hash, 0)

	for _, v := range val {
		hashes, err := resolveToHashes(v)
		if err != nil {
			return nil, err
		}
		topicsList = append(topicsList, hashes)
	}
	return topicsList, nil
}

func resolveToHashes(val interface{}) ([]common.Hash, error) {
	// if val is nil, return
	if val == nil {
		return nil, nil
	}

	// if val is string, return
	if hashStr, ok := val.(string); ok {
		return []common.Hash{common.HexToHash(hashStr)}, nil
	}

	// if val is string slice, append every item
	if addrStrList, ok := val.([]interface{}); ok {
		addrList := make([]common.Hash, 0)
		for _, v := range addrStrList {
			vStr, ok := v.(string)
			if !ok {
				return nil, errors.Errorf("could not conver type %v to hash", reflect.TypeOf(v))
			}

			addrList = append(addrList, common.HexToHash(vStr))
		}
		return addrList, nil
	}

	return nil, errors.Errorf("failed to convert %v to hash or hashes", val)
}
