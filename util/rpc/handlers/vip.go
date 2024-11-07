package handlers

import (
	"context"
	"strconv"
	"strings"
	"time"
	"unicode"

	web3pay "github.com/Conflux-Chain/web3pay-service/client/middleware"
	"github.com/Conflux-Chain/web3pay-service/types"
)

const (
	// Minimum length for a valid access token
	minAccessTokenLength = 20
)

type VipTier uint

const ( // !!! order is important
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

const VipSubPropTierKey = "tier"

type VipStatus struct {
	ID   string  // VIP ID
	Tier VipTier // VIP tier
}

// IsAccessTokenValid checks if access token from the context
// is at least minimum length and only contains alphanumeric chars.
func IsAccessTokenValid(ctx context.Context) bool {
	token, ok := GetAccessTokenFromContext(ctx)
	if !ok {
		return false
	}

	if len(token) < minAccessTokenLength {
		return false // Key length less than minimum
	}

	// Check for valid alphanumeric characters using loop
	for _, r := range token {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return false
		}
	}

	return true
}

// VipStatusFromContext returns VIP status from context
func VipStatusFromContext(ctx context.Context) (*VipStatus, bool) {
	var vs *VipStatus

	if ss, ok := web3pay.VipSubscriptionStatusFromContext(ctx); ok {
		// check VIP subscription status
		vs, _ = GetVipStatusBySubscriptionStatus(ss)
	} else if bs, ok := web3pay.BillingStatusFromContext(ctx); ok {
		// check billing status
		vs, _ = GetVipStatusByBillingStatus(bs)
	}

	if vs != nil && vs.Tier != VipTierNone {
		return vs, true
	}

	return nil, false
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

	vt := VipTier(tier)
	if vt < VipTierSubscription1 || vt >= VipTierSubscriptionEnd {
		// beyond acceptable range
		return VipTierNone, false
	}

	return vt, true
}
