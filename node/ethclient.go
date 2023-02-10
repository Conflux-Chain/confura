package node

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/Conflux-Chain/confura/util/rpc"
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

func NewEthClientProvider(router Router) *EthClientProvider {
	cp := &EthClientProvider{
		clientProvider: newClientProvider(router, func(url string) (interface{}, error) {
			client, err := rpc.NewEthClient(url, rpc.WithClientHookMetrics(true))
			if err != nil {
				return nil, err
			}

			return &Web3goClient{client, url}, nil
		}),
	}

	for grp := range ethUrlCfg {
		cp.registerGroup(grp)
	}

	return cp
}

// GetClientByToken gets client of specific group (or use normal HTTP group as default) by access token.
func (p *EthClientProvider) GetClientByToken(ctx context.Context, groups ...Group) (*Web3goClient, error) {
	accessToken := accessTokenFromContext(ctx)
	client, err := p.getClient(accessToken, ethNodeGroup(groups...))
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
