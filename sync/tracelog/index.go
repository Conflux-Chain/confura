package tracelog

// Contract index is a compact numeric identifier for known internal contract addresses.
// These values are protocol-level constants and must remain stable.
const (
	ContractUnknown uint8 = iota
	ContractStaking
	ContractSponsor
	ContractAdmin
)

// Event index is a compact numeric identifier for known event method selectors.
// These values must remain stable; only append new entries.
const (
	EventUnknown uint8 = iota

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
