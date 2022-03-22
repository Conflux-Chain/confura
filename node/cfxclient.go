package node

import (
	"context"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/spf13/viper"
)

type CfxClientProvider struct {
	*clientProvider
}

func NewCfxClientProvider(router Router) *CfxClientProvider {
	cp := &CfxClientProvider{
		clientProvider: newClientProvider(router, func(url string) (interface{}, error) {
			requestTimeout := viper.GetDuration("cfx.requestTimeout")

			cfx, err := sdk.NewClient(url, sdk.ClientOption{
				RequestTimeout: requestTimeout,
			})
			if err == nil {
				util.HookCfxRpcMetricsMiddleware(cfx)
			}
			return cfx, err
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
