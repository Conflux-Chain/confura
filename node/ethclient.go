package node

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/Conflux-Chain/confura/store/mysql"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	"github.com/openweb3/web3go"
)

type Web3goClient struct {
	*web3go.Client

	URL string
}

func (w3c *Web3goClient) NodeName() string {
	return rpcutil.Url2NodeName(w3c.URL)
}

// EthClientProvider provides evm space client by router.
type EthClientProvider struct {
	*clientProvider
}

func newEthClient(url string) (interface{}, error) {
	client, err := rpcutil.NewEthClient(url, rpcutil.WithClientHookMetrics(true))
	if err != nil {
		return nil, err
	}

	return &Web3goClient{client, url}, nil
}

func NewEthClientProvider(db *mysql.MysqlStore, router Router) *EthClientProvider {
	cp := &EthClientProvider{
		clientProvider: newClientProvider(db, router, newEthClient),
	}

	return cp
}

// GetClient gets client of specific group (or use normal HTTP group as default).
func (p *EthClientProvider) GetClient(key string, groups ...Group) (*Web3goClient, error) {
	client, err := p.getClient(key, ethNodeGroup(groups...))
	if err != nil {
		return nil, err
	}

	return client.(*Web3goClient), nil
}

// GetClientByIP gets client of specific group (or use normal HTTP group as default) by remote IP address.
func (p *EthClientProvider) GetClientByIP(ctx context.Context, groups ...Group) (*Web3goClient, error) {
	remoteAddr := remoteAddrFromContext(ctx)
	client, err := p.getClient(remoteAddr, ethNodeGroup(groups...))
	if err != nil {
		return nil, err
	}

	return client.(*Web3goClient), nil
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
			clients = append(clients, c.(*Web3goClient))
		} else {
			return nil, err
		}
	}

	return clients, nil
}

func (p *EthClientProvider) GetClientRandom() (*Web3goClient, error) {
	key := fmt.Sprintf("random_key_%v", rand.Int())
	client, err := p.getClient(key, GroupEthHttp)

	return client.(*Web3goClient), err
}

func ethNodeGroup(groups ...Group) Group {
	grp := GroupEthHttp
	if len(groups) > 0 {
		grp = groups[0]
	}

	return grp
}
