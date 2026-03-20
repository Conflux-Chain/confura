package tracelog

import (
	"encoding/hex"
	"math/big"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
	"golang.org/x/crypto/sha3"
)

var (
	StakingEventDefs = []*EventDef{
		{
			MethodSignature:  "deposit(uint256)",
			EventSignature:   "Deposit(address,uint256)",
			Topics:           []TopicDef{{Source: TopicFromCaller}},
			UseRawArgsAsData: true,
			eventIndex:       EventStakingDeposit,
		},
		{
			MethodSignature:  "withdraw(uint256)",
			EventSignature:   "Withdraw(address,uint256)",
			Topics:           []TopicDef{{Source: TopicFromCaller}},
			UseRawArgsAsData: true,
			eventIndex:       EventStakingWithdraw,
		},
		{
			MethodSignature:  "voteLock(uint256,uint256)",
			EventSignature:   "VoteLocked(address,uint256,uint256)",
			Topics:           []TopicDef{{Source: TopicFromCaller}},
			UseRawArgsAsData: true,
			eventIndex:       EventStakingVoteLock,
		},
	}

	SponsorEventDefs = []*EventDef{
		{
			MethodSignature: "setSponsorForGas(address,uint256)",
			EventSignature:  "SponsorGas(address,address,uint256)",
			Topics: []TopicDef{
				{Source: TopicFromCaller},
				{Source: TopicFromArg, ArgIndex: 0},
			},
			NonIndexedArgs: []NonIndexedArg{
				{ArgIndex: 1, ABIType: "uint256"},
			},
			eventIndex: EventSponsorGas,
		},
		{
			MethodSignature: "setSponsorForCollateral(address)",
			EventSignature:  "SponsorCollateral(address,address)",
			Topics: []TopicDef{
				{Source: TopicFromCaller},
				{Source: TopicFromArg, ArgIndex: 0},
			},
			eventIndex: EventSponsorCollateral,
		},
		{
			MethodSignature: "addPrivilegeByAdmin(address,address[])",
			EventSignature:  "WhitelistAddedByAdmin(address,address,address[])",
			Topics: []TopicDef{
				{Source: TopicFromCaller},
				{Source: TopicFromArg, ArgIndex: 0},
			},
			NonIndexedArgs: []NonIndexedArg{
				{ArgIndex: 1, ABIType: "address[]"},
			},
			eventIndex: EventWhitelistAddedByAdmin,
		},
		{
			MethodSignature: "removePrivilegeByAdmin(address,address[])",
			EventSignature:  "WhitelistRemovedByAdmin(address,address,address[])",
			Topics: []TopicDef{
				{Source: TopicFromCaller},
				{Source: TopicFromArg, ArgIndex: 0},
			},
			NonIndexedArgs: []NonIndexedArg{
				{ArgIndex: 1, ABIType: "address[]"},
			},
			eventIndex: EventWhitelistRemovedByAdmin,
		},
		{
			MethodSignature: "addPrivilege(address[])",
			EventSignature:  "WhitelistAdded(address,address[])",
			Topics: []TopicDef{
				{Source: TopicFromCaller},
			},
			NonIndexedArgs: []NonIndexedArg{
				{ArgIndex: 0, ABIType: "address[]"},
			},
			eventIndex: EventWhitelistAdded,
		},
		{
			MethodSignature: "removePrivilege(address[])",
			EventSignature:  "WhitelistRemoved(address,address[])",
			Topics: []TopicDef{
				{Source: TopicFromCaller},
			},
			NonIndexedArgs: []NonIndexedArg{
				{ArgIndex: 0, ABIType: "address[]"},
			},
			eventIndex: EventWhitelistRemoved,
		},
	}

	AdminEventDefs = []*EventDef{
		{
			MethodSignature: "setAdmin(address,address)",
			EventSignature:  "AdminChanged(address,address,address)",
			Topics: []TopicDef{
				{Source: TopicFromCaller},
				{Source: TopicFromArg, ArgIndex: 0},
			},
			NonIndexedArgs: []NonIndexedArg{
				{ArgIndex: 1, ABIType: "address"},
			},
			eventIndex: EventAdminChanged,
		},
		{
			MethodSignature: "destroy(address)",
			EventSignature:  "ContractDestroyed(address,address)",
			Topics: []TopicDef{
				{Source: TopicFromCaller},
				{Source: TopicFromArg, ArgIndex: 0},
			},
			eventIndex: EventContractDestroyed,
		},
	}
)

// MethodID computes the 4-byte method selector from a Solidity function signature.
func MethodID(signature string) string {
	h := sha3.NewLegacyKeccak256()
	h.Write([]byte(signature))
	return hex.EncodeToString(h.Sum(nil)[:4])
}

// Keccak256 computes the full keccak256 hash of the input string.
func Keccak256(signature string) common.Hash {
	h := sha3.NewLegacyKeccak256()
	h.Write([]byte(signature))
	return common.BytesToHash(h.Sum(nil))
}

// TopicSource defines how a topic value is derived from the call context.
type TopicSource int

const (
	// TopicFromCaller uses msg.sender (CallAction.From) as the topic value.
	TopicFromCaller TopicSource = iota
	// TopicFromArg uses the argument at a given index as an address topic.
	TopicFromArg
)

// TopicDef describes how to build a single indexed topic.
type TopicDef struct {
	Source   TopicSource
	ArgIndex int // only used when Source == TopicFromArg
}

// NonIndexedArg describes a non-indexed argument to be ABI-encoded into log data.
type NonIndexedArg struct {
	ArgIndex int
	ABIType  string // e.g. "uint256", "address", "address[]"
}

// EventDef fully describes how to construct a virtual log from a method call.
type EventDef struct {
	// MethodSignature is the Solidity function signature, e.g. "deposit(uint256)".
	MethodSignature string
	// EventSignature is the Solidity event signature, e.g. "Deposit(address,uint256)".
	EventSignature string

	// Topics defines how to build topic1, topic2, topic3.
	// topic0 is always derived from EventSignature.
	Topics []TopicDef

	// NonIndexedArgs defines which method arguments become the log's data field.
	// If nil, the raw method arguments (input[4:]) are used as data directly.
	NonIndexedArgs []NonIndexedArg

	// UseRawArgsAsData indicates that input[4:] should be used verbatim as log data.
	UseRawArgsAsData bool

	// EventIndex is a compact numeric identifier to reference event definitions.
	eventIndex uint8

	// Precomputed values (populated by Register)
	methodID [4]byte
	topic0   common.Hash
}

// MethodIDHex returns the hex-encoded method selector with "0x" prefix.
func (e *EventDef) MethodIDHex() string {
	return "0x" + hex.EncodeToString(e.methodID[:])
}

// Topic0 returns the precomputed event topic0.
func (e *EventDef) Topic0() common.Hash {
	return e.topic0
}

func (e *EventDef) init() {
	// Compute method ID
	h := sha3.NewLegacyKeccak256()
	h.Write([]byte(e.MethodSignature))
	copy(e.methodID[:], h.Sum(nil)[:4])

	// Compute topic0
	e.topic0 = Keccak256(e.EventSignature)
}

// BuildLog constructs a virtual log from the event definition, call frame, and unpacked arguments.
func (e *EventDef) BuildLog(callTrace *CallTraceData, values []any, rawArgs []byte) (*types.Log, error) {
	var topics []common.Hash
	topics = append(topics, e.topic0)

	// Build indexed topics
	for _, td := range e.Topics {
		switch td.Source {
		case TopicFromCaller:
			topics = append(topics, common.HexToHash(callTrace.Action.From.GetHexAddress()))
		case TopicFromArg:
			if td.ArgIndex >= len(values) {
				return nil, errors.Errorf("arg index %d out of range (have %d args)", td.ArgIndex, len(values))
			}
			addr, ok := values[td.ArgIndex].(types.Address)
			if !ok {
				return nil, errors.Errorf("arg %d is not an address type", td.ArgIndex)
			}
			topics = append(topics, common.HexToHash(addr.GetHexAddress()))
		default:
			return nil, errors.Errorf("unknown topic source: %d", td.Source)
		}
	}

	// Build data
	var data hexutil.Bytes
	var err error

	if e.UseRawArgsAsData {
		data = rawArgs
	} else if len(e.NonIndexedArgs) > 0 {
		data, err = e.packNonIndexedArgs(values)
		if err != nil {
			return nil, err
		}
	}

	// Convert to Conflux types
	cfxTopics := make([]types.Hash, len(topics))
	for i, t := range topics {
		cfxTopics[i] = types.Hash(t.String())
	}

	space := types.SPACE_NATIVE
	return &types.Log{
		Space:            &space,
		Data:             data,
		Address:          callTrace.Action.To,
		Topics:           cfxTopics,
		BlockHash:        &callTrace.BlockHash,
		EpochNumber:      &callTrace.EpochNumber,
		TransactionHash:  &callTrace.TransactionHash,
		TransactionIndex: (*hexutil.Big)(new(big.Int).SetUint64(uint64(callTrace.TransactionPosition))),
	}, nil
}

func (e *EventDef) packNonIndexedArgs(values []any) ([]byte, error) {
	args := make(abi.Arguments, 0, len(e.NonIndexedArgs))
	vals := make([]any, 0, len(e.NonIndexedArgs))

	for _, na := range e.NonIndexedArgs {
		if na.ArgIndex >= len(values) {
			return nil, errors.Errorf("arg index %d out of range", na.ArgIndex)
		}
		abiType, err := abi.NewType(na.ABIType, "", nil)
		if err != nil {
			return nil, errors.WithMessagef(err, "invalid ABI type %q", na.ABIType)
		}
		args = append(args, abi.Argument{Type: abiType})
		vals = append(vals, values[na.ArgIndex])
	}

	return args.Pack(vals...)
}
