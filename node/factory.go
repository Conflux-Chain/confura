package node

import (
	"context"
	"sync"

	"github.com/Conflux-Chain/confura/store/mysql"
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
			func(group Group, name, url string) (Node, error) {
				return NewCfxNode(group, name, url)
			},
			cfg.Endpoint, cfg.EndpointProto, urlCfg, cfg.Router.NodeRPCURL,
		)
	})

	return cfxFactory
}

// EthFactory returns evm space instance factory
func EthFactory() *factory {
	ethOnce.Do(func() {
		ethFactory = newFactory(
			func(group Group, name, url string) (Node, error) {
				return NewEthNode(group, name, url)
			},
			cfg.EthEndpoint, cfg.EthEndpointProto, ethUrlCfg, cfg.Router.EthNodeRPCURL,
		)
	})

	return ethFactory
}

// factory creates router and RPC server.
type factory struct {
	nodeRpcUrl      string
	rpcSrvEndpoint  string
	gRpcSrvEndpoint string
	groupConf       map[Group]UrlConfig
	nodeFactory     nodeFactory
}

func newFactory(nf nodeFactory, rpcSrvEndpoint string, gRpcSrvEndpoint string, groupConf map[Group]UrlConfig, nodeRpcUrl string) *factory {
	return &factory{
		nodeRpcUrl:      nodeRpcUrl,
		nodeFactory:     nf,
		rpcSrvEndpoint:  rpcSrvEndpoint,
		gRpcSrvEndpoint: gRpcSrvEndpoint,
		groupConf:       groupConf,
	}
}

// MustStartServer starts node manager RPC server
func (f *factory) MustStartServer(ctx context.Context, wg *sync.WaitGroup, db *mysql.MysqlStore) {
	handler := MustNewApiHandler(db, f.nodeFactory, f.groupConf)

	// start RPC server
	rpcServer := NewServer(handler)
	go rpcServer.MustServeGraceful(ctx, wg, f.rpcSrvEndpoint, rpc.ProtocolHttp)

	// start gRPC server
	MustStartGRPCRouterServer(ctx, wg, f.gRpcSrvEndpoint, handler)
}

// CreateRouter creates node router
func (f *factory) CreateRouter() Router {
	return MustNewRouter(cfg.Router.RedisURL, f.nodeRpcUrl, f.groupConf)
}
