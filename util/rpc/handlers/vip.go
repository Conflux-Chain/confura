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
	ID       string     `json:"id"`                 // VIP ID
	Tier     VipTier    `json:"tier"`               // VIP tier
	ExpireAt *time.Time `json:"expireAt,omitempty"` // VIP expiration time (optional)
}

// IsAccessTokenValid checks if the access token is at least minimum length and
// only contains alphanumeric chars.
func IsAccessTokenValid(token string) bool {
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
	if ss, ok := web3pay.VipSubscriptionStatusFromContext(ctx); ok {
		// check VIP subscription status
		return GetVipStatusBySubscriptionStatus(ss)
	} else if bs, ok := web3pay.BillingStatusFromContext(ctx); ok {
		// check billing status
		return GetVipStatusByBillingStatus(bs)
	}

	return nil, false
}

func GetVipStatusBySubscriptionStatus(vss *web3pay.VipSubscriptionStatus) (*VipStatus, bool) {
	vi, err := vss.GetVipInfo()
	if err != nil {
		return nil, false
	}

	expiredAt := time.Unix(vi.ExpireAt.Int64(), 0)
	return &VipStatus{
		ID:       vi.Account.String(),
		Tier:     GetVipTierBySubscription(vi),
		ExpireAt: &expiredAt,
	}, true
}

func GetVipStatusByBillingStatus(bs *web3pay.BillingStatus) (*VipStatus, bool) {
	if bs.Success() {
		vs := &VipStatus{Tier: VipTierBilling, ID: bs.Receipt.Customer.String()}
		return vs, true
	}

	return nil, false
}

func GetVipTierBySubscription(vi *types.VipInfo) VipTier {
	props := vi.ICardTrackerVipInfo.Props
	if len(props.Keys) == 0 { // no prop
		return VipTierNone
	}

	if len(props.Keys) != len(props.Values) { // malformed kv
		return VipTierNone
	}

	if !strings.EqualFold(props.Keys[0], VipSubPropTierKey) { // invalid kv
		return VipTierNone
	}

	tier, err := strconv.Atoi(props.Values[0])
	if err != nil { // no integer value
		return VipTierNone
	}

	vt := VipTier(tier)
	if vt < VipTierSubscription1 || vt >= VipTierSubscriptionEnd {
		// beyond acceptable range
		return VipTierNone
	}

	return vt
}
