package acl

type VipTier uint

const (
	// none VIP tier
	VipTierNone VipTier = iota
	// VIP subscription tier
	VipTierSubscription1
	VipTierSubscription2
	VipTierSubscription3
	VipTierSubscriptionEnd
	// VIP Billing tier
	VipTierBilling = 100
)

type VipStatus struct {
	ID   string  // VIP ID
	Tier VipTier // VIP tier
}
