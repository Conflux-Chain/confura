package node

import (
	"context"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/conflux-chain/conflux-infura/util"
)

type CfxClientProvider struct {
	*clientProvider
}

func NewCfxClientProvider(router Router) *CfxClientProvider {
	cp := &CfxClientProvider{
		clientProvider: newClientProvider(router, func(url string) (interface{}, error) {
			return util.NewCfxClient(url, util.WithClientHookMetrics(true))
		}),
	}

	for grp := range urlCfg {
		cp.registerGroup(grp)
	}

	return cp
}

func (p *CfxClientProvider) GetClientByIP(ctx context.Context) (sdk.ClientOperator, error) {
	remoteAddr := remoteAddrFromContext(ctx)
	client, err := p.getClient(remoteAddr, GroupCfxHttp)

	return client.(sdk.ClientOperator), err
}

func (p *CfxClientProvider) GetClientByIPGroup(ctx context.Context, group Group) (sdk.ClientOperator, error) {
	remoteAddr := remoteAddrFromContext(ctx)
	client, err := p.getClient(remoteAddr, group)

	return client.(sdk.ClientOperator), err
}
