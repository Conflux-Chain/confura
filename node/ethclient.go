package node

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/conflux-chain/conflux-infura/util"
	"github.com/openweb3/web3go"
)

type EthClientProvider struct {
	*clientProvider
}

func NewEthClientProvider(router Router) *EthClientProvider {
	cp := &EthClientProvider{
		clientProvider: newClientProvider(router, func(url string) (interface{}, error) {
			return util.NewEthClient(url, util.WithClientHookMetrics(true))
		}),
	}

	for grp := range ethUrlCfg {
		cp.registerGroup(grp)
	}

	return cp
}

func (p *EthClientProvider) GetClientByIP(ctx context.Context) (*web3go.Client, error) {
	remoteAddr := remoteAddrFromContext(ctx)
	client, err := p.getClient(remoteAddr, GroupEthHttp)

	return client.(*web3go.Client), err
}

func (p *EthClientProvider) GetClientByIPGroup(ctx context.Context, group Group) (*web3go.Client, error) {
	remoteAddr := remoteAddrFromContext(ctx)
	client, err := p.getClient(remoteAddr, group)

	return client.(*web3go.Client), err
}

func (p *EthClientProvider) GetClientRandom() (*web3go.Client, error) {
	key := fmt.Sprintf("random_key_%v", rand.Int())
	client, err := p.getClient(key, GroupEthHttp)
	return client.(*web3go.Client), err
}
