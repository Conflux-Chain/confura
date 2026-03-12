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
	ContractIdx ContractIndex

	// eventsByMethodID maps 4-byte method ID (hex without 0x) to EventDef.
	eventsByMethodID map[string]*EventDef
}

// LookupEvent finds the EventDef for a given 4-byte method selector.
func (e *ContractEntry) LookupEvent(methodID []byte) (*EventDef, bool) {
	key := hex.EncodeToString(methodID)
	def, ok := e.eventsByMethodID[key]
	return def, ok
}

// EventIndexOf returns the EventIndex for a given 4-byte method selector.
func (e *ContractEntry) EventIndexOf(methodID []byte) (EventIndex, bool) {
	if def, ok := e.LookupEvent(methodID); ok {
		return def.eventIndex, true
	}
	return 0, false
}

// Registry manages all known internal contracts and their event mappings.
type Registry struct {
	entries         []*ContractEntry
	byAddress       map[string]*ContractEntry // keyed by address
	contractIndices map[string]ContractIndex  // hex address -> ContractIndex
}

// NewRegistry creates a Registry from the SDK client, registering all
// known internal contracts and their event definitions.
func NewRegistry(client sdk.ClientOperator) (*Registry, error) {
	r := &Registry{
		byAddress:       make(map[string]*ContractEntry),
		contractIndices: make(map[string]ContractIndex),
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

func (r *Registry) register(addr types.Address, contract sdk.Contract, ci ContractIndex, defs []*EventDef) {
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
	}

	r.entries = append(r.entries, entry)
	hexAddr := addr.GetHexAddress()
	r.byAddress[hexAddr] = entry
	r.contractIndices[hexAddr] = ci
}

// Lookup finds the ContractEntry for a given address.
func (r *Registry) Lookup(addr types.Address) (*ContractEntry, bool) {
	entry, ok := r.byAddress[addr.GetHexAddress()]
	return entry, ok
}

// ContractIndexOf returns the ContractIndex for a given contract address.
func (r *Registry) ContractIndexOf(addr types.Address) ContractIndex {
	if idx, ok := r.contractIndices[addr.GetHexAddress()]; ok {
		return idx
	}
	return ContractUnknown
}
