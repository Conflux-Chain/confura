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
	RouteCacheExpirationTTL = 90 * time.Second
)

var (
	ErrClientUnavailable = errors.New("no full node available")
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

	// db store to load route configs
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

func (p *clientProvider) getClientByToken(token string, group Group) (interface{}, error) {
	if p.db == nil {
		// use provided group if no custom db provided
		return p.getClient(token, group)
	}

	routeGrp, err := p.getRouteGroup(token)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"token":        token,
			"defaultGroup": group,
		}).Error("Client provider failed to load node route info")
		return nil, errors.WithMessage(err, "failed to load route group")
	}

	if len(routeGrp) > 0 {
		// use custom route group if configured
		group = Group(routeGrp)
	}

	return p.getClient(token, group)
}

func (p *clientProvider) getRouteGroup(token string) (Group, error) {
	v, expired, found := p.routeKeyCache.GetNoExp(token)
	if found && !expired { // cache hit
		return v.(Group), nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	v, expired, found = p.routeKeyCache.GetNoExp(token)
	if found && !expired { // double check
		return v.(Group), nil
	}

	if expired && v != nil {
		// extend lifespan for expired cache kv temporarliy for performance
		p.routeKeyCache.Add(token, v.(Group))
	}

	// load route group from database
	routegrp, err := p.db.GetRouteGroup(token)
	if err != nil {
		return Group(""), errors.WithMessage(err, "failed to load route group")
	}

	// cache the new route group
	grp := Group(routegrp)
	p.routeKeyCache.Add(token, grp)

	return grp, nil
}

// getClient gets client based on keyword and node group type.
func (p *clientProvider) getClient(key string, group Group) (interface{}, error) {
	clients := p.getOrRegisterGroup(group)

	logger := logrus.WithFields(logrus.Fields{
		"key":   key,
		"group": group,
	})

	url := p.router.Route(group, []byte(key))
	if len(url) == 0 {
		logger.WithError(ErrClientUnavailable).Error("Failed to get full node client from provider")
		return nil, ErrClientUnavailable
	}

	nodeName := rpc.Url2NodeName(url)

	logger = logger.WithFields(logrus.Fields{
		"node": nodeName,
		"url":  url,
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
