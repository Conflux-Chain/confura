package rate

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	web3pay "github.com/Conflux-Chain/web3pay-service/client/middleware"
	"github.com/Conflux-Chain/web3pay-service/types"
)

const (
	VipSubPropTierKey = "tier"
)

type VipTier uint

const (
	// no VIP tier
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

func VipStrategyByTier(tier VipTier) string {
	return fmt.Sprintf("vip%d", tier)
}

func GetVipStatusBySubscriptionStatus(vss *web3pay.VipSubscriptionStatus) (*VipStatus, bool) {
	vi, err := vss.GetVipInfo()
	if err != nil {
		return nil, false
	}

	if tier, ok := GetVipTierBySubscription(vi); ok {
		return &VipStatus{Tier: tier, ID: vi.Account.String()}, true
	}

	return nil, false
}

func GetVipStatusByBillingStatus(bs *web3pay.BillingStatus) (*VipStatus, bool) {
	if bs.Success() {
		vs := &VipStatus{Tier: VipTierBilling, ID: bs.Receipt.Customer.String()}
		return vs, true
	}

	return nil, false
}

func GetVipTierBySubscription(vi *types.VipInfo) (VipTier, bool) {
	if vi.ExpireAt.Int64() < time.Now().Unix() { // already expired
		return VipTierNone, false
	}

	props := vi.ICardTrackerVipInfo.Props
	if len(props.Keys) == 0 { // no prop
		return VipTierNone, false
	}

	if len(props.Keys) != len(props.Values) { // malformed kv
		return VipTierNone, false
	}

	if !strings.EqualFold(props.Keys[0], VipSubPropTierKey) { // invalid kv
		return VipTierNone, false
	}

	tier, err := strconv.Atoi(props.Values[0])
	if err != nil { // no integer value
		return VipTierNone, false
	}

	if vt := VipTier(tier); vt > VipTierNone && vt < VipTierSubscriptionEnd {
		// acceptable VIP subscription tier
		return vt, true
	}

	// beyond acceptable range
	return VipTierNone, false
}
