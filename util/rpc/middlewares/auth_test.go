package middlewares

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	web3pay "github.com/Conflux-Chain/web3pay-service/client/middleware"
	"github.com/Conflux-Chain/web3pay-service/contract"
	"github.com/Conflux-Chain/web3pay-service/service"
	web3paytypes "github.com/Conflux-Chain/web3pay-service/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/go-rpc-provider"
	"github.com/stretchr/testify/require"
)

func TestAuthenticateBillingWithNilExpireAt(t *testing.T) {
	customer := common.HexToAddress("0x1234")
	billingStatus := web3pay.NewBillingStatusWithReceipt("api-key", &service.BillingReceipt{
		Customer: customer,
	})
	ctx := context.WithValue(context.Background(), web3pay.CtxKeyBillingStatus, billingStatus)
	ctx = context.WithValue(ctx, handlers.CtxKeyAccessToken, "12345678901234567890")

	nextCalled := false
	next := func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
		nextCalled = true
		authID, ok := handlers.GetAuthIdFromContext(ctx)
		require.True(t, ok)
		require.Equal(t, customer.String(), authID)
		return msg
	}

	msg := &rpc.JsonRpcMessage{Version: "2.0", ID: []byte("1"), Method: "test_method"}
	resp := Authenticate(next)(ctx, msg)

	require.Same(t, msg, resp)
	require.True(t, nextCalled)
}

func TestAuthenticateRejectsInvalidBillingStatuses(t *testing.T) {
	tests := []struct {
		name   string
		status *web3pay.BillingStatus
	}{
		{
			name:   "billing failed",
			status: web3pay.NewBillingStatusWithError("api-key", errors.New("billing failed")),
		},
		{
			name:   "receipt missing",
			status: web3pay.NewBillingStatusWithReceipt("api-key", nil),
		},
		{
			name:   "status missing",
			status: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), web3pay.CtxKeyBillingStatus, test.status)
			ctx = context.WithValue(ctx, handlers.CtxKeyAccessToken, "12345678901234567890")
			nextCalled := false
			next := func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
				nextCalled = true
				return msg
			}

			msg := &rpc.JsonRpcMessage{Version: "2.0", ID: []byte("1"), Method: "test_method"}
			resp := Authenticate(next)(ctx, msg)

			require.NotNil(t, resp.Error)
			require.Equal(t, errInvalidApiKey.Error(), resp.Error.Error())
			require.False(t, nextCalled)
		})
	}
}

func TestResolveAuthIDByTier(t *testing.T) {
	customer := common.HexToAddress("0x1234")
	future := time.Now().Add(time.Hour)
	past := time.Now().Add(-time.Hour)

	tests := []struct {
		name    string
		status  *handlers.VipStatus
		wantID  string
		wantErr error
	}{
		{
			name:   "billing ignores missing expiration",
			status: &handlers.VipStatus{ID: customer.String(), Tier: handlers.VipTierBilling},
			wantID: customer.String(),
		},
		{
			name:   "billing ignores expiration value",
			status: &handlers.VipStatus{ID: customer.String(), Tier: handlers.VipTierBilling, ExpireAt: &past},
			wantID: customer.String(),
		},
		{
			name:   "subscription active",
			status: &handlers.VipStatus{ID: customer.String(), Tier: handlers.VipTierSubscription1, ExpireAt: &future},
			wantID: customer.String(),
		},
		{
			name:    "subscription expired",
			status:  &handlers.VipStatus{ID: customer.String(), Tier: handlers.VipTierSubscription1, ExpireAt: &past},
			wantErr: errApiKeyExpired,
		},
		{
			name:    "subscription expiration missing",
			status:  &handlers.VipStatus{ID: customer.String(), Tier: handlers.VipTierSubscription1},
			wantErr: errInvalidApiKey,
		},
		{
			name:    "tier none",
			status:  &handlers.VipStatus{ID: customer.String(), Tier: handlers.VipTierNone},
			wantErr: errInvalidApiKey,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			id, err := resolveVipStatus(test.status)
			require.ErrorIs(t, err, test.wantErr)
			require.Equal(t, test.wantID, id)
		})
	}
}

func TestResolveAuthIDSubscriptionStatuses(t *testing.T) {
	customer := common.HexToAddress("0x5678")
	future := time.Now().Add(time.Hour)
	past := time.Now().Add(-time.Hour)

	tests := []struct {
		name    string
		expires *time.Time
		tier    string
		wantID  string
		wantErr error
	}{
		{name: "active", expires: &future, tier: "1", wantID: customer.String()},
		{name: "expired", expires: &past, tier: "1", wantErr: errApiKeyExpired},
		{name: "expiration missing", tier: "1", wantErr: errInvalidApiKey},
		{name: "tier none", expires: &future, tier: "0", wantErr: errInvalidApiKey},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var expireAt *big.Int
			if test.expires != nil {
				expireAt = big.NewInt(test.expires.Unix())
			}
			vipInfo := &web3paytypes.VipInfo{
				ICardTrackerVipInfo: contract.ICardTrackerVipInfo{
					ExpireAt: expireAt,
					Props: contract.ICardTemplateProps{
						Keys:   []string{handlers.VipSubPropTierKey},
						Values: []string{test.tier},
					},
				},
				Account: customer,
			}
			status := web3pay.NewVipSubscriptionStatusWithInfo("api-key", vipInfo)
			ctx := context.WithValue(context.Background(), web3pay.CtxKeyVipSubscriptionStatus, status)

			id, err := resolveAuthID(ctx)
			require.ErrorIs(t, err, test.wantErr)
			require.Equal(t, test.wantID, id)
		})
	}
}
