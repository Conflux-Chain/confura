package node

import (
	"context"

	"github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
)

// CfxClientProvider provides core space client by router.
type CfxClientProvider struct {
	*clientProvider
}

func NewCfxClientProvider(router Router) *CfxClientProvider {
	cp := &CfxClientProvider{
		clientProvider: newClientProvider(router, func(url string) (interface{}, error) {
			return rpc.NewCfxClient(url, rpc.WithClientHookMetrics(true))
		}),
	}

	for grp := range urlCfg {
		cp.registerGroup(grp)
	}

	return cp
}

// GetClientByIP gets client of normal HTTP group by remote IP address.
func (p *CfxClientProvider) GetClientByIP(ctx context.Context) (sdk.ClientOperator, error) {
	return p.GetClientByIPGroup(ctx, GroupCfxHttp)
}

// GetClientByIPGroup gets client of specific group by remote IP address.
func (p *CfxClientProvider) GetClientByIPGroup(ctx context.Context, group Group) (sdk.ClientOperator, error) {
	remoteAddr := remoteAddrFromContext(ctx)

	client, err := p.getClient(remoteAddr, group)
	if err != nil {
		return nil, err
	}

	return client.(sdk.ClientOperator), nil
}
