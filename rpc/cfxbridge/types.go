package cfxbridge

import (
	"encoding/json"
	"math/big"
	"strings"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// EthBlockNumber accepts number and epoch tag latest_state, other values are invalid, e.g. latest_confirmed.
type EthBlockNumber struct {
	value *big.Int
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (ebn *EthBlockNumber) UnmarshalJSON(data []byte) error {
	// Unmarshal as an epoch
	var epoch *types.Epoch
	if err := epoch.UnmarshalJSON(data); err != nil {
		return err
	}

	// Treat epoch number as block number
	if num, ok := epoch.ToInt(); ok {
		ebn.value = num
		return nil
	}

	// For latest_state epoch, use nil as block number according to ethclient
	if epoch.Equals(types.EpochLatestState) {
		return nil
	}

	// Other values are all invalid
	return ErrEpochUnsupported
}

// EthAddress accepts both hex40 and base32 format addresses.
type EthAddress struct {
	value common.Address
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
// Note, Nonce and StorageLimit fields are simply ignored.
type EthCallRequest struct {
	From     *EthAddress
	To       *EthAddress
	GasPrice *hexutil.Big
	Gas      *hexutil.Big
	Value    *hexutil.Big
	Data     *string
}

func (req *EthCallRequest) ToCallMsg() ethereum.CallMsg {
	var msg ethereum.CallMsg

	if req.From != nil {
		msg.From = req.From.value
	}

	if req.To != nil {
		msg.To = &req.To.value
	}

	if req.GasPrice != nil {
		msg.GasPrice = req.GasPrice.ToInt()
	}

	if req.Gas != nil {
		msg.Gas = req.Gas.ToInt().Uint64()
	}

	if req.Value != nil {
		msg.Value = req.Value.ToInt()
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
	BlockHashes *common.Hash
	Address     []EthAddress
	Topics      [][]common.Hash
}

func (filter *EthLogFilter) ToFilterQuery() (*ethereum.FilterQuery, error) {
	var query ethereum.FilterQuery

	if filter.FromEpoch != nil {
		query.FromBlock = filter.FromEpoch.value
	}

	if filter.ToEpoch != nil {
		query.ToBlock = filter.ToEpoch.value
	}

	query.BlockHash = filter.BlockHashes

	for i := range filter.Address {
		query.Addresses = append(query.Addresses, filter.Address[i].value)
	}

	query.Topics = filter.Topics

	return &query, nil
}
