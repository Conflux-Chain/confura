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

// GetClientByToken gets client of specific group (or use normal HTTP group as default) by access token.
func (p *EthClientProvider) GetClientSetByToken(ctx context.Context, groups ...Group) ([]*Web3goClient, error) {
	accessToken := accessTokenFromContext(ctx)
	clients, err := p.getClientSetByToken(accessToken, ethNodeGroup(groups...))
	if err != nil {
		return nil, err
	}

	return unwrapEthClientSet(clients), nil
}

// GetClientByIP gets client of specific group (or use normal HTTP group as default) by remote IP address.
func (p *EthClientProvider) GetClientSetByIP(ctx context.Context, groups ...Group) ([]*Web3goClient, error) {
	remoteAddr := remoteAddrFromContext(ctx)
	clients, err := p.getClientSet(remoteAddr, ethNodeGroup(groups...))
	if err != nil {
		return nil, err
	}

	return unwrapEthClientSet(clients), nil
}

func (p *EthClientProvider) GetClientSetRandom() ([]*Web3goClient, error) {
	key := fmt.Sprintf("random_key_%v", rand.Int())
	clients, err := p.getClientSet(key, GroupEthHttp)
	if err != nil {
		return nil, err
	}

	return unwrapEthClientSet(clients), nil
}

func ethNodeGroup(groups ...Group) Group {
	grp := GroupEthHttp
	if len(groups) > 0 {
		grp = groups[0]
	}

	return grp
}

func unwrapEthClientSet(clients []*wrapClient) (res []*Web3goClient) {
	for _, c := range clients {
		res = append(res, c.wrapped.(*Web3goClient))
	}

	return res
}
