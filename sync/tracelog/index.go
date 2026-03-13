package tracelog

// ContractIndex is a compact numeric identifier for known internal contract addresses.
// These values are protocol-level constants and must remain stable.
type ContractIndex uint8

const (
	ContractUnknown ContractIndex = iota
	ContractStaking
	ContractSponsor
	ContractAdmin
)

// EventIndex is a compact numeric identifier for known event method selectors.
// These values must remain stable; only append new entries.
type EventIndex uint8

const (
	EventUnknown EventIndex = iota

	// Staking events
	EventStakingDeposit
	EventStakingWithdraw
	EventStakingVoteLock

	// Sponsor events
	EventSponsorGas
	EventSponsorCollateral
	EventWhitelistAddedByAdmin
	EventWhitelistRemovedByAdmin
	EventWhitelistAdded
	EventWhitelistRemoved

	// Admin events
	EventAdminChanged
	EventContractDestroyed
)
