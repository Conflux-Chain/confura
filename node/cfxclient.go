package node

import (
	"context"

	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
)

// CfxClientProvider provides core space client by router.
type CfxClientProvider struct {
	*clientProvider[sdk.ClientOperator]
}

func newCfxClient(url string) (sdk.ClientOperator, error) {
	client, err := rpc.NewCfxClient(url, rpc.WithClientHookMetrics(true))
	if err != nil {
		return nil, err
	}
	return rpc.NewCfxCoreClient(client), nil
}

func NewCfxClientProvider(db *mysql.MysqlStore, router Router) *CfxClientProvider {
	return &CfxClientProvider{
		clientProvider: newClientProvider(db, router, newCfxClient),
	}
}

// GetClient gets client of specific group (or use normal HTTP group as default).
func (p *CfxClientProvider) GetClient(key string, groups ...Group) (sdk.ClientOperator, error) {
	return p.getClient(key, cfxNodeGroup(groups...))
}

// GetClientByIP gets client of specific group (or use normal HTTP group as default) by remote IP address.
func (p *CfxClientProvider) GetClientByIP(ctx context.Context, groups ...Group) (sdk.ClientOperator, error) {
	remoteAddr := remoteAddrFromContext(ctx)
	return p.getClient(remoteAddr, cfxNodeGroup(groups...))
}

// GetClientsByGroup gets all clients of specific group.
func (p *CfxClientProvider) GetClientsByGroup(grp Group) (clients []sdk.ClientOperator, err error) {
	np := locateNodeProvider(p.router)
	if np == nil {
		return nil, ErrNotSupportedRouter
	}

	nodeUrls := np.ListNodesByGroup(grp)
	for _, url := range nodeUrls {
		if c, err := p.getOrRegisterClient(string(url), grp); err == nil {
			clients = append(clients, c)
		} else {
			return nil, err
		}
	}

	return clients, nil
}

func cfxNodeGroup(groups ...Group) Group {
	grp := GroupCfxHttp
	if len(groups) > 0 {
		grp = groups[0]
	}

	return grp
}

// locateNodeProvider finds node provider from the router chain or nil.
func locateNodeProvider(r Router) NodeProvider {
	if np, ok := r.(NodeProvider); ok {
		return np
	}

	if cr, ok := r.(*chainedRouter); ok {
		for _, r := range cr.routers {
			if np := locateNodeProvider(r); np != nil {
				return np
			}
		}
	}

	return nil
}
