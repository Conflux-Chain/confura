package node

import (
	"context"
	"fmt"
	"math/rand"

	cacheRpc "github.com/Conflux-Chain/confura-data-cache/rpc"
	"github.com/Conflux-Chain/confura/store/mysql"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
)

type Web3goClient = rpcutil.Web3goClient

// EthClientProvider provides evm space client by router.
type EthClientProvider struct {
	*clientProvider[*Web3goClient]
}

func newEthClientCtor(dataCache cacheRpc.Interface) clientFactory[*Web3goClient] {
	return func(url string) (*Web3goClient, error) {
		client, err := rpcutil.NewEthClient(url, rpcutil.WithClientHookMetrics(true))
		if err != nil {
			return nil, err
		}
		return rpcutil.NewWeb3goClient(url, client, dataCache), nil
	}
}

func NewEthClientProvider(dataCache cacheRpc.Interface, db *mysql.MysqlStore, router Router) *EthClientProvider {
	return &EthClientProvider{
		clientProvider: newClientProvider(db, router, newEthClientCtor(dataCache)),
	}
}

// GetClient gets client of specific group (or use normal HTTP group as default).
func (p *EthClientProvider) GetClient(key string, groups ...Group) (*Web3goClient, error) {
	return p.getClient(key, ethNodeGroup(groups...))
}

// GetClientByIP gets client of specific group (or use normal HTTP group as default) by remote IP address.
func (p *EthClientProvider) GetClientByIP(ctx context.Context, groups ...Group) (*Web3goClient, error) {
	remoteAddr := remoteAddrFromContext(ctx)
	return p.getClient(remoteAddr, ethNodeGroup(groups...))
}

// GetClientsByGroup gets all clients of specific group.
func (p *EthClientProvider) GetClientsByGroup(grp Group) (clients []*Web3goClient, err error) {
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

func (p *EthClientProvider) GetClientRandom() (*Web3goClient, error) {
	key := fmt.Sprintf("random_key_%v", rand.Int())
	return p.getClient(key, GroupEthHttp)
}

func ethNodeGroup(groups ...Group) Group {
	grp := GroupEthHttp
	if len(groups) > 0 {
		grp = groups[0]
	}

	return grp
}
