package tracelog

import (
	"encoding/hex"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	sdkcontract "github.com/Conflux-Chain/go-conflux-sdk/contract_meta/internal_contract"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/pkg/errors"
)

// ContractEntry holds the SDK contract and its associated event definitions.
type ContractEntry struct {
	Address     types.Address
	Contract    sdk.Contract
	ContractIdx uint8

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
	byTopic0  map[string]*EventDef      // keyed by topic0
}

// NewRegistry creates a Registry from the SDK client, registering all
// known internal contracts and their event definitions.
func NewRegistry(client sdk.ClientOperator) (*Registry, error) {
	r := &Registry{
		byAddress: make(map[string]*ContractEntry),
		byTopic0:  make(map[string]*EventDef),
	}

	// Register Staking
	staking, err := sdkcontract.NewStaking(client)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create staking contract")
	}
	r.register(*staking.Address, staking.Contract, ContractStaking, StakingEventDefs)

	// Register SponsorWhitelistControl
	sponsor, err := sdkcontract.NewSponsor(client)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create sponsor contract")
	}
	r.register(*sponsor.Address, sponsor.Contract, ContractSponsor, SponsorEventDefs)

	// Register AdminControl
	admin, err := sdkcontract.NewAdminControl(client)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create admin control contract")
	}
	r.register(*admin.Address, admin.Contract, ContractAdmin, AdminEventDefs)

	return r, nil
}

func (r *Registry) register(addr types.Address, contract sdk.Contract, ci uint8, defs []*EventDef) {
	entry := &ContractEntry{
		Address:          addr,
		Contract:         contract,
		ContractIdx:      ci,
		eventsByMethodID: make(map[string]*EventDef, len(defs)),
	}
	for _, def := range defs {
		def.init()
		key := hex.EncodeToString(def.methodID[:])
		entry.eventsByMethodID[key] = def

		r.byTopic0[def.topic0.String()] = def
	}

	r.entries = append(r.entries, entry)
	r.byAddress[addr.GetHexAddress()] = entry
}

// Lookup finds the ContractEntry for a given address.
func (r *Registry) Lookup(addr types.Address) (*ContractEntry, bool) {
	entry, ok := r.byAddress[addr.GetHexAddress()]
	return entry, ok
}

// ContractIndexOf returns the index for a given contract address.
func (r *Registry) ContractIndexOf(addr types.Address) (uint8, bool) {
	if entry, ok := r.byAddress[addr.GetHexAddress()]; ok {
		return entry.ContractIdx, true
	}
	return ContractUnknown, false
}

// EventIndexOf returns the index for a given topic string.
func (r *Registry) EventIndexOf(topic0 types.Hash) (uint8, bool) {
	if def, ok := r.byTopic0[topic0.String()]; ok {
		return def.eventIndex, true
	}
	return EventUnknown, false
}
