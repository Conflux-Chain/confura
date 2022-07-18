package node

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/Conflux-Chain/confura/util/rpc"
	"github.com/openweb3/web3go"
)

type Web3goClient struct {
	*web3go.Client

	URL string
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

// GetClientByIP gets client of normal HTTP group by remote IP address.
func (p *EthClientProvider) GetClientByIP(ctx context.Context) (*Web3goClient, error) {
	return p.GetClientByIPGroup(ctx, GroupEthHttp)
}

// GetClientByIPGroup gets client of specific group by remote IP address.
func (p *EthClientProvider) GetClientByIPGroup(ctx context.Context, group Group) (*Web3goClient, error) {
	remoteAddr := remoteAddrFromContext(ctx)
	client, err := p.getClient(remoteAddr, group)

	return client.(*Web3goClient), err
}

func (p *EthClientProvider) GetClientRandom() (*Web3goClient, error) {
	key := fmt.Sprintf("random_key_%v", rand.Int())
	client, err := p.getClient(key, GroupEthHttp)

	return client.(*Web3goClient), err
}
