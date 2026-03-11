package tracelog

import (
	"encoding/hex"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	sdkcontract "github.com/Conflux-Chain/go-conflux-sdk/contract_meta/internal_contract"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/pkg/errors"
)

var StakingEventDefs = []*EventDef{
	{
		MethodSignature:  "deposit(uint256)",
		EventSignature:   "Deposit(address,uint256)",
		Topics:           []TopicDef{{Source: TopicFromCaller}},
		UseRawArgsAsData: true,
	},
	{
		MethodSignature:  "withdraw(uint256)",
		EventSignature:   "Withdraw(address,uint256)",
		Topics:           []TopicDef{{Source: TopicFromCaller}},
		UseRawArgsAsData: true,
	},
	{
		MethodSignature:  "voteLock(uint256,uint256)",
		EventSignature:   "VoteLocked(address,uint256,uint256)",
		Topics:           []TopicDef{{Source: TopicFromCaller}},
		UseRawArgsAsData: true,
	},
}

var SponsorEventDefs = []*EventDef{
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
	},
	{
		MethodSignature: "setSponsorForCollateral(address)",
		EventSignature:  "SponsorCollateral(address,address)",
		Topics: []TopicDef{
			{Source: TopicFromCaller},
			{Source: TopicFromArg, ArgIndex: 0},
		},
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
	},
	{
		MethodSignature: "addPrivilege(address[])",
		EventSignature:  "WhitelistAdded(address,address[])",
		Topics: []TopicDef{
			{Source: TopicFromCaller},
		},
		NonIndexedArgs: []NonIndexedArg{
			{ArgIndex: 1, ABIType: "address[]"},
		},
	},
	{
		MethodSignature: "removePrivilege(address[])",
		EventSignature:  "WhitelistRemoved(address,address[])",
		Topics: []TopicDef{
			{Source: TopicFromCaller},
		},
		NonIndexedArgs: []NonIndexedArg{
			{ArgIndex: 1, ABIType: "address[]"},
		},
	},
}

var AdminEventDefs = []*EventDef{
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
	},
	{
		MethodSignature: "destroy(address)",
		EventSignature:  "ContractDestroyed(address,address)",
		Topics: []TopicDef{
			{Source: TopicFromCaller},
			{Source: TopicFromArg, ArgIndex: 0},
		},
	},
}

// ContractEntry holds the SDK contract and its associated event definitions.
type ContractEntry struct {
	Address  types.Address
	Contract sdk.Contract

	// eventsByMethodID maps 4-byte method ID (hex without 0x) to EventDef.
	eventsByMethodID map[string]*EventDef
}

// LookupEvent finds the EventDef for a given 4-byte method selector.
func (e *ContractEntry) LookupEvent(methodID []byte) (*EventDef, bool) {
	key := hex.EncodeToString(methodID)
	def, ok := e.eventsByMethodID[key]
	return def, ok
}

// Registry manages all known internal contracts and their event mappings.
type Registry struct {
	entries   []*ContractEntry
	byAddress map[string]*ContractEntry // keyed by address
}

// NewRegistry creates a Registry from the SDK client, registering all
// known internal contracts and their event definitions.
func NewRegistry(client sdk.ClientOperator) (*Registry, error) {
	r := &Registry{
		byAddress: make(map[string]*ContractEntry),
	}

	// Register Staking
	staking, err := sdkcontract.NewStaking(client)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create staking contract")
	}
	r.register(*staking.Address, staking.Contract, StakingEventDefs)

	// Register SponsorWhitelistControl
	sponsor, err := sdkcontract.NewSponsor(client)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create sponsor contract")
	}
	r.register(*sponsor.Address, sponsor.Contract, SponsorEventDefs)

	// Register AdminControl
	admin, err := sdkcontract.NewAdminControl(client)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create admin control contract")
	}
	r.register(*admin.Address, admin.Contract, AdminEventDefs)

	return r, nil
}

func (r *Registry) register(addr types.Address, contract sdk.Contract, defs []*EventDef) {
	entry := &ContractEntry{
		Address:          addr,
		Contract:         contract,
		eventsByMethodID: make(map[string]*EventDef, len(defs)),
	}
	for _, def := range defs {
		def.init()
		key := hex.EncodeToString(def.methodID[:])
		entry.eventsByMethodID[key] = def
	}
	r.entries = append(r.entries, entry)
	r.byAddress[addr.GetHexAddress()] = entry
}

// Lookup finds the ContractEntry for a given address.
func (r *Registry) Lookup(addr types.Address) (*ContractEntry, bool) {
	entry, ok := r.byAddress[addr.GetHexAddress()]
	return entry, ok
}
