package node

import (
	"context"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/rpc"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
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
type clientFactory func(url string) (interface{}, error)

// clientProvider provides different RPC client based on request IP to achieve load balance
// or with node group for resource isolation. Generally, it is used by RPC server to delegate
// RPC requests to full node cluster.
type clientProvider struct {
	router  Router
	factory clientFactory
	mu      sync.Mutex

	// db store to load node route configs
	db *mysql.MysqlStore
	// route key cache: route key => route group
	routeKeyCache *util.ExpirableLruCache

	// group => node name => RPC client
	clients *util.ConcurrentMap
}

func newClientProvider(db *mysql.MysqlStore, router Router, factory clientFactory) *clientProvider {
	return &clientProvider{
		db:            db,
		router:        router,
		factory:       factory,
		clients:       &util.ConcurrentMap{},
		routeKeyCache: util.NewExpirableLruCache(RouteKeyCacheSize, RouteCacheExpirationTTL),
	}
}

// getOrRegisterGroup gets or registers node group
func (p *clientProvider) getOrRegisterGroup(group Group) *util.ConcurrentMap {
	v, _ := p.clients.LoadOrStoreFn(group, func(k interface{}) interface{} {
		return &util.ConcurrentMap{}
	})

	return v.(*util.ConcurrentMap)
}

// getRouteGroup get custom route group for specific route key
func (p *clientProvider) GetRouteGroup(key string) (grp Group, ok bool) {
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

func (p *clientProvider) cacheLoad(key string) (Group, bool) {
	v, expired, found := p.routeKeyCache.GetNoExp(key)
	if found && !expired { // cache hit
		return v.(Group), true
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	v, expired, found = p.routeKeyCache.GetNoExp(key)
	if found && !expired { // double check
		return v.(Group), true
	}

	if found && expired {
		// extend lifespan for expired cache kv temporarliy for performance
		p.routeKeyCache.Add(key, v.(Group))
	}

	return Group(""), false
}

func (p *clientProvider) populateCache(token string) (grp Group, ok bool) {
	// find node route by key from database
	route, err := p.db.FindNodeRoute(token)

	if err != nil {
		p.mu.Lock()
		defer p.mu.Unlock()

		// for db error, we cache an empty group for the key by which no expiry cache value existed
		// so that db pressure can be mitigrated by reducing too many subsequential queries.
		if _, _, found := p.routeKeyCache.GetNoExp(token); !found {
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
func (p *clientProvider) getClient(key string, group Group) (interface{}, error) {
	url := p.router.Route(group, []byte(key))
	if len(url) == 0 {
		logrus.WithFields(logrus.Fields{
			"key":   key,
			"group": group,
		}).Error("No full node client available from router")
		return nil, ErrClientUnavailable
	}

	return p.getOrRegisterClient(url, group)
}

// getOrRegisterClient gets or registers RPC client for fullnode proxy.
func (p *clientProvider) getOrRegisterClient(url string, group Group) (interface{}, error) {
	clients := p.getOrRegisterGroup(group)
	nodeName := rpc.Url2NodeName(url)

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

		return nil, err
	}

	if !loaded {
		logger.Info("Succeeded to connect to full node")
	}

	return client, nil
}

func remoteAddrFromContext(ctx context.Context) string {
	if ip, ok := handlers.GetIPAddressFromContext(ctx); ok {
		return ip
	}

	return "unknown_ip"
}

func accessTokenFromContext(ctx context.Context) string {
	if token, ok := handlers.GetAccessTokenFromContext(ctx); ok {
		return token
	}

	return "unknown_access_token"
}
