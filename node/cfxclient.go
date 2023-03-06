package node

import (
	"context"

	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
)

// CfxClientProvider provides core space client by router.
type CfxClientProvider struct {
	*clientProvider
}

func newCfxClient(url string) (interface{}, error) {
	return rpc.NewCfxClient(url, rpc.WithClientHookMetrics(true))
}

func NewCfxClientProvider(db *mysql.MysqlStore, router Router) *CfxClientProvider {
	return &CfxClientProvider{
		clientProvider: newClientProvider(db, router, newCfxClient),
	}
}

// GetClientByToken gets client of specific group (or use normal HTTP group as default) by access token.
func (p *CfxClientProvider) GetClientSetByToken(ctx context.Context, groups ...Group) (res []sdk.ClientOperator, err error) {
	accessToken := accessTokenFromContext(ctx)
	clients, err := p.getClientSetByToken(accessToken, cfxNodeGroup(groups...))
	if err != nil {
		return nil, err
	}

	return unwrapCfxClientSet(clients), nil
}

// GetClientByIP gets client of specific group (or use normal HTTP group as default) by remote IP address.
func (p *CfxClientProvider) GetClientSetByIP(ctx context.Context, groups ...Group) (res []sdk.ClientOperator, err error) {
	remoteAddr := remoteAddrFromContext(ctx)
	clients, err := p.getClientSet(remoteAddr, cfxNodeGroup(groups...))
	if err != nil {
		return nil, err
	}

	return unwrapCfxClientSet(clients), nil
}

func cfxNodeGroup(groups ...Group) Group {
	grp := GroupCfxHttp
	if len(groups) > 0 {
		grp = groups[0]
	}

	return grp
}

func unwrapCfxClientSet(clients []*wrapClient) (res []sdk.ClientOperator) {
	for _, c := range clients {
		res = append(res, c.wrapped.(sdk.ClientOperator))
	}

	return res
}
