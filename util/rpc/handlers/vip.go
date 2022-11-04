package handlers

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/Conflux-Chain/confura/util/acl"
	web3pay "github.com/Conflux-Chain/web3pay-service/client/middleware"
	"github.com/Conflux-Chain/web3pay-service/types"
)

const (
	VipSubPropTierKey = "tier"
)

// VipStatusFromContext returns VIP status from context
func VipStatusFromContext(ctx context.Context) (*acl.VipStatus, bool) {
	var vs *acl.VipStatus

	if ss, ok := web3pay.VipSubscriptionStatusFromContext(ctx); ok {
		// check VIP subscription status
		vs, _ = GetVipStatusBySubscriptionStatus(ss)
	} else if bs, ok := web3pay.BillingStatusFromContext(ctx); ok {
		// check billing status
		vs, _ = GetVipStatusByBillingStatus(bs)
	}

	if vs != nil && vs.Tier != acl.VipTierNone {
		return vs, true
	}

	return nil, false
}

func GetVipStatusBySubscriptionStatus(vss *web3pay.VipSubscriptionStatus) (*acl.VipStatus, bool) {
	vi, err := vss.GetVipInfo()
	if err != nil {
		return nil, false
	}

	if tier, ok := GetVipTierBySubscription(vi); ok {
		return &acl.VipStatus{Tier: tier, ID: vi.Account.String()}, true
	}

	return nil, false
}

func GetVipStatusByBillingStatus(bs *web3pay.BillingStatus) (*acl.VipStatus, bool) {
	if bs.Success() {
		vs := &acl.VipStatus{Tier: acl.VipTierBilling, ID: bs.Receipt.Customer.String()}
		return vs, true
	}

	return nil, false
}

func GetVipTierBySubscription(vi *types.VipInfo) (acl.VipTier, bool) {
	if vi.ExpireAt.Int64() < time.Now().Unix() { // already expired
		return acl.VipTierNone, false
	}

	props := vi.ICardTrackerVipInfo.Props
	if len(props.Keys) == 0 { // no prop
		return acl.VipTierNone, false
	}

	if len(props.Keys) != len(props.Values) { // malformed kv
		return acl.VipTierNone, false
	}

	if !strings.EqualFold(props.Keys[0], VipSubPropTierKey) { // invalid kv
		return acl.VipTierNone, false
	}

	tier, err := strconv.Atoi(props.Values[0])
	if err != nil { // no integer value
		return acl.VipTierNone, false
	}

	vt := acl.VipTier(tier)
	if vt < acl.VipTierSubscription1 || vt >= acl.VipTierSubscriptionEnd {
		// beyond acceptable range
		return acl.VipTierNone, false
	}

	return vt, true
}
