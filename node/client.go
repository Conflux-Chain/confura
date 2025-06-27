package node

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	cacheRpc "github.com/Conflux-Chain/confura-data-cache/rpc"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	RouteKeyCacheSize       = 5000
	RouteCacheExpirationTTL = 60 * time.Second
)

var (
	ErrClientUnavailable  = errors.New("no full node available")
	ErrNotSupportedRouter = errors.New("not supported router")
)

// clientFactory factory method to create RPC client for fullnode proxy.
type clientFactory[T any] func(url string) (T, error)

// clientProvider provides different RPC client based on request IP to achieve load balance
// or with node group for resource isolation. Generally, it is used by RPC server to delegate
// RPC requests to full node cluster.
type clientProvider[T any] struct {
	router       Router
	defaultGroup Group
	factory      clientFactory[T]
	mu           sync.Mutex

	// db store to load node route configs
	db *mysql.MysqlStore
	// route key cache: route key => route group
	routeKeyCache *util.ExpirableLruCache

	// group => node name => RPC client
	clients *util.ConcurrentMap
}

func newClientProvider[T any](db *mysql.MysqlStore, router Router, defaultGroup Group, factory clientFactory[T]) *clientProvider[T] {
	return &clientProvider[T]{
		db:            db,
		router:        router,
		defaultGroup:  defaultGroup,
		factory:       factory,
		clients:       &util.ConcurrentMap{},
		routeKeyCache: util.NewExpirableLruCache(RouteKeyCacheSize, RouteCacheExpirationTTL),
	}
}

// getOrRegisterGroup gets or registers node group
func (p *clientProvider[T]) getOrRegisterGroup(group Group) *util.ConcurrentMap {
	v, _ := p.clients.LoadOrStoreFn(group, func(k interface{}) interface{} {
		return &util.ConcurrentMap{}
	})

	return v.(*util.ConcurrentMap)
}

// GetRouteGroup get custom route group for specific route key
func (p *clientProvider[T]) GetRouteGroup(key string) (grp Group, ok bool) {
	if p.db == nil { // db not available
		return grp, false
	}

	// load from cache at first
	if grp, ok = p.cacheLoad(key); ok {
		return grp, true
	}

	// otherwise, populate the cache
	return p.populateCache(key)
}

func (p *clientProvider[T]) cacheLoad(key string) (Group, bool) {
	v, expired, found := p.routeKeyCache.GetWithoutExp(key)
	if found && !expired { // cache hit
		return v.(Group), true
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	v, expired, found = p.routeKeyCache.GetWithoutExp(key)
	if found && !expired { // double check
		return v.(Group), true
	}

	if found && expired {
		// extend lifespan for expired cache kv temporarliy for performance
		p.routeKeyCache.Add(key, v.(Group))
	}

	return Group(""), false
}

func (p *clientProvider[T]) populateCache(token string) (grp Group, ok bool) {
	// find node route by key from database
	route, err := p.db.FindNodeRoute(token)

	if err != nil {
		p.mu.Lock()
		defer p.mu.Unlock()

		// for db error, we cache an empty group for the key by which no expiry cache value existed
		// so that db pressure can be mitigrated by reducing too many subsequential queries.
		if _, _, found := p.routeKeyCache.GetWithoutExp(token); !found {
			p.routeKeyCache.Add(token, grp)
		}

		logrus.WithField("key", token).
			WithError(err).
			Error("Client provider failed to load node route from db")
		return grp, false
	}

	if route != nil {
		grp = Group(route.Group)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// cache the new route group
	p.routeKeyCache.Add(token, grp)
	return grp, true
}

// getClient gets client based on keyword and node group type.
func (p *clientProvider[T]) getClient(key string, group Group) (res T, err error) {
	url := p.router.Route(group, []byte(key))
	if len(url) == 0 {
		logrus.WithFields(logrus.Fields{
			"key":   key,
			"group": group,
		}).Error("No full node client available from router")
		return res, ErrClientUnavailable
	}

	return p.getOrRegisterClient(url, group)
}

// getOrRegisterClient gets or registers RPC client for fullnode proxy.
func (p *clientProvider[T]) getOrRegisterClient(url string, group Group) (res T, err error) {
	clients := p.getOrRegisterGroup(group)
	nodeName := rpcutil.Url2NodeName(url)

	logger := logrus.WithFields(logrus.Fields{
		"node":  nodeName,
		"url":   url,
		"group": group,
	})
	logger.Trace("Route RPC requests")

	client, loaded, err := clients.LoadOrStoreFnErr(nodeName, func(interface{}) (interface{}, error) {
		// TODO improvements required
		// 1. Necessary retry? (but longer timeout). Better to let user side to decide.
		// 2. Different metrics for different full nodes.
		return p.factory(url)
	})

	if err != nil {
		err := errors.WithMessage(err, "bad full node connection")
		logger.WithError(err).Error("Failed to get full node client from provider")

		return res, err
	}

	if !loaded {
		logger.Info("Succeeded to connect to full node")
	}

	return client.(T), nil
}

// GetClient gets client of specific group (or use normal HTTP group as default).
func (p *clientProvider[T]) GetClient(key string, groups ...Group) (T, error) {
	if len(groups) > 0 {
		return p.getClient(key, groups[0])
	}

	return p.getClient(key, p.defaultGroup)
}

// GetClientByIP gets client of specific group (or use normal HTTP group as default) by remote IP address.
func (p *clientProvider[T]) GetClientByIP(ctx context.Context, groups ...Group) (T, error) {
	if ip, ok := handlers.GetIPAddressFromContext(ctx); ok {
		return p.GetClient(ip, groups...)
	}

	return p.GetClient("unknown_ip", groups...)
}

func (p *clientProvider[T]) GetClientRandom() (T, error) {
	key := fmt.Sprintf("random_key_%v", rand.Int())
	return p.GetClient(key)
}

// GetClientsByGroup gets all clients of specific group.
func (p *clientProvider[T]) GetClientsByGroup(grp Group) (clients []T, err error) {
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

// locateNodeProvider finds node provider from the router chain or nil.
func locateNodeProvider(r Router) NodeProvider {
	if np, ok := r.(NodeProvider); ok {
		return np
	}

	if cr, ok := r.(*chainedRouter); ok {
		for _, r := range cr.routers {
			if np := locateNodeProvider(r); np != nil {
				return np
			}
		}
	}

	return nil
}

// CfxClientProvider provides core space client by router.
type CfxClientProvider = clientProvider[sdk.ClientOperator]

func NewCfxClientProvider(db *mysql.MysqlStore, router Router) *CfxClientProvider {
	return newClientProvider(db, router, GroupCfxHttp, func(url string) (sdk.ClientOperator, error) {
		client, err := rpcutil.NewCfxClient(url, rpcutil.WithClientHookMetrics(true))
		if err != nil {
			return nil, err
		}
		return rpcutil.NewCfxCoreClient(client), nil
	})
}

type Web3goClient = rpcutil.Web3goClient

// EthClientProvider provides evm space client by router.
type EthClientProvider = clientProvider[*Web3goClient]

func NewEthClientProvider(dataCache cacheRpc.Interface, db *mysql.MysqlStore, router Router) *EthClientProvider {
	return newClientProvider(db, router, GroupEthHttp, func(url string) (*Web3goClient, error) {
		client, err := rpcutil.NewEthClient(url, rpcutil.WithClientHookMetrics(true))
		if err != nil {
			return nil, err
		}
		return rpcutil.NewWeb3goClientFromViper(url, client, dataCache)
	})
}
