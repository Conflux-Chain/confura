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
func (p *CfxClientProvider) GetClientByToken(ctx context.Context, groups ...Group) (sdk.ClientOperator, error) {
	accessToken := accessTokenFromContext(ctx)
	client, err := p.getClient(accessToken, cfxNodeGroup(groups...))
	if err != nil {
		return nil, err
	}

	return client.(sdk.ClientOperator), nil
}

// GetClientByIP gets client of specific group (or use normal HTTP group as default) by remote IP address.
func (p *CfxClientProvider) GetClientByIP(ctx context.Context, groups ...Group) (sdk.ClientOperator, error) {
	remoteAddr := remoteAddrFromContext(ctx)
	client, err := p.getClient(remoteAddr, cfxNodeGroup(groups...))
	if err != nil {
		return nil, err
	}

	return client.(sdk.ClientOperator), nil
}

func (p *CfxClientProvider) GetGroupByToken(ctx context.Context) (Group, bool) {
	token := accessTokenFromContext(ctx)
	return p.getRouteGroup(token)
}

func cfxNodeGroup(groups ...Group) Group {
	grp := GroupCfxHttp
	if len(groups) > 0 {
		grp = groups[0]
	}

	return grp
}
