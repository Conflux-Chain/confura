package node

import (
	"context"
	"sync"

	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util/rpc"
	"github.com/sirupsen/logrus"
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
			cfg.Endpoint, cfg.EndpointProto, urlCfg, cfg.Router.NodeRPCURL, cfg.Router.NodeRpcUrlProto,
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
			cfg.EthEndpoint, cfg.EthEndpointProto, ethUrlCfg, cfg.Router.EthNodeRPCURL, cfg.Router.EthNodeRpcUrlProto,
		)
	})

	return ethFactory
}

// factory creates router and RPC server.
type factory struct {
	nodeRpcUrl      string
	nodeGRpcUrl     string
	rpcSrvEndpoint  string
	gRpcSrvEndpoint string
	groupConf       map[Group]UrlConfig
	nodeFactory     nodeFactory
}

func newFactory(nf nodeFactory, rpcSrvEndpoint, gRpcSrvEndpoint string, groupConf map[Group]UrlConfig, nodeRpcUrl, nodeGRpcUrl string) *factory {
	return &factory{
		nodeRpcUrl:      nodeRpcUrl,
		nodeGRpcUrl:     nodeGRpcUrl,
		nodeFactory:     nf,
		rpcSrvEndpoint:  rpcSrvEndpoint,
		gRpcSrvEndpoint: gRpcSrvEndpoint,
		groupConf:       groupConf,
	}
}

// MustStartServer starts node manager RPC server
func (f *factory) MustStartServer(ctx context.Context, wg *sync.WaitGroup, db *mysql.CommonStores) {
	if err := f.StartServer(ctx, wg, db); err != nil {
		logrus.WithError(err).Fatal("Failed to start node manager")
	}
}

func (f *factory) StartServer(ctx context.Context, wg *sync.WaitGroup, db *mysql.CommonStores) error {
	handler, err := NewApiHandler(db, f.nodeFactory, f.groupConf)
	if err != nil {
		return err
	}
	child, cancel := context.WithCancel(ctx)
	var inner sync.WaitGroup
	rpcServer, err := newServer(handler)
	if err != nil {
		cancel()
		handler.pool.Close()
		return err
	}
	// Register cleanup before startup. On failure the caller can start its
	// shutdown deadline before waiting for requests or monitored nodes.
	startupDone := make(chan struct{})
	defer close(startupDone)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		<-startupDone
		<-child.Done()
		rpcServer.Close()
		inner.Wait()
		handler.pool.Close()
	}()
	if err = rpcServer.Start(child, &inner, f.rpcSrvEndpoint, rpc.ProtocolHttp); err == nil {
		err = StartGRPC(child, &inner, f.gRpcSrvEndpoint, handler)
	}
	if err != nil {
		cancel()
		return err
	}
	return nil
}

// CreateRouter creates node router
func (f *factory) CreateRouter() Router {
	return MustNewRouter(cfg.Router.RedisURL, f.nodeRpcUrl, f.nodeGRpcUrl, f.groupConf)
}

func (f *factory) NewRouter() (Router, error) {
	return NewRouter(cfg.Router.RedisURL, f.nodeRpcUrl, f.nodeGRpcUrl, f.groupConf)
}
