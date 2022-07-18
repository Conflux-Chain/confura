package node

import (
	"sync"

	"github.com/Conflux-Chain/confura/util/rpc"
)

var (
	cfxFactory *factory
	cfxOnce    sync.Once

	ethFactory *factory
	ethOnce    sync.Once
)

// Factory returns core space instance factory
func Factory() *factory {
	cfxOnce.Do(func() {
		cfxFactory = newFactory(
			func(group Group, name, url string, hm HealthMonitor) (Node, error) {
				return NewCfxNode(group, name, url, hm), nil
			},
			cfg.Endpoint, urlCfg, cfg.Router.NodeRPCURL,
		)
	})

	return cfxFactory
}

// EthFactory returns evm space instance factory
func EthFactory() *factory {
	ethOnce.Do(func() {
		ethFactory = newFactory(
			func(group Group, name, url string, hm HealthMonitor) (Node, error) {
				return NewEthNode(group, name, url, hm), nil
			},
			cfg.EthEndpoint, ethUrlCfg, cfg.Router.EthNodeRPCURL,
		)
	})

	return ethFactory
}

// factory creates router and RPC server.
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

// CreatRpcServer creates node manager RPC server
func (f *factory) CreatRpcServer() (*rpc.Server, string) {
	return NewServer(f.nodeFactory, f.groupConf), f.rpcSrvEndpoint
}

// CreateRouter creates node router
func (f *factory) CreateRouter() Router {
	return MustNewRouter(cfg.Router.RedisURL, f.nodeRpcUrl, f.groupConf)
}
