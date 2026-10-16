package handlers

import (
	"errors"
	"testing"

	web3pay "github.com/Conflux-Chain/web3pay-service/client/middleware"
	"github.com/Conflux-Chain/web3pay-service/service"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func TestGetVipStatusByBillingStatus(t *testing.T) {
	customer := common.HexToAddress("0x1234")

	tests := []struct {
		name   string
		status *web3pay.BillingStatus
		wantOK bool
	}{
		{
			name: "success",
			status: web3pay.NewBillingStatusWithReceipt("api-key", &service.BillingReceipt{
				Customer: customer,
			}),
			wantOK: true,
		},
		{
			name:   "billing failed",
			status: web3pay.NewBillingStatusWithError("api-key", errors.New("billing failed")),
		},
		{
			name:   "receipt missing",
			status: web3pay.NewBillingStatusWithReceipt("api-key", nil),
		},
		{
			name: "status missing",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status, ok := GetVipStatusByBillingStatus(test.status)
			require.Equal(t, test.wantOK, ok)
			if !test.wantOK {
				require.Nil(t, status)
				return
			}

			require.Equal(t, VipTier(VipTierBilling), status.Tier)
			require.Equal(t, customer.String(), status.ID)
			require.Nil(t, status.ExpireAt)
		})
	}
}
