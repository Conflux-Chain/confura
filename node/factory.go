package node

import (
	"sync"

	"github.com/conflux-chain/conflux-infura/util/rpc"
)

var (
	cfxFactory *factory
	cfxOnce    sync.Once

	ethFactory *factory
	ethOnce    sync.Once
)

func Factory() *factory {
	cfxOnce.Do(func() {
		cfxFactory = newFactory(
			func(name, url string, hm HealthMonitor) (Node, error) {
				return NewCfxNode(name, url, hm), nil
			},
			cfg.Endpoint, urlCfg, cfg.Router.NodeRPCURL,
		)
	})

	return cfxFactory
}

func EthFactory() *factory {
	ethOnce.Do(func() {
		ethFactory = newFactory(
			func(name, url string, hm HealthMonitor) (Node, error) {
				return NewEthNode(name, url, hm), nil
			},
			cfg.EthEndpoint, ethUrlCfg, cfg.Router.EthNodeRPCURL,
		)
	})

	return ethFactory
}

// factory to create Conflux or EVM space struct suites.
type factory struct {
	nodeRpcUrl     string
	rpcSrvEndpoint string
	groupConf      map[Group]UrlConfig
	nodeFactory    nodeFactory
}

func newFactory(nf nodeFactory, rpcSrvEndpoint string, groupConf map[Group]UrlConfig, nodeRpcUrl string) *factory {
	return &factory{
		nodeRpcUrl:     nodeRpcUrl,
		nodeFactory:    nf,
		rpcSrvEndpoint: rpcSrvEndpoint,
		groupConf:      groupConf,
	}
}

func (f *factory) CreatRpcServer() (*rpc.Server, string) {
	return NewServer(f.nodeFactory, f.groupConf), f.rpcSrvEndpoint
}

func (f *factory) CreateRouter() Router {
	return MustNewRouter(cfg.Router.RedisURL, f.nodeRpcUrl, f.groupConf)
}
