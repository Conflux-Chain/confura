package node

import (
	"context"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

// Router is used to route RPC requests to multiple full nodes.
type Router interface {
	// Route returns the full node URL for specified key.
	Route(key []byte) string
	// WSRoute returns the websocket full node URL for specified key.
	WSRoute(key []byte) string
}

// MustNewRouterFromViper creates an instance of Router based on viper.
func MustNewRouterFromViper() Router {
	var routers []Router

	// Add redis router if configured
	if url := cfg.Router.RedisURL; len(url) > 0 {
		// redis://<user>:<password>@<host>:<port>/<db_number>
		opt, err := redis.ParseURL(url)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to parse redis URL")
		}

		client := redis.NewClient(opt)
		routers = append(routers, NewRedisRouter(client))
	}

	// Add node rpc router if configured
	if url := cfg.Router.NodeRPCURL; len(url) > 0 {
		// http://127.0.0.1:22530
		client, err := rpc.DialHTTP(url)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to create rpc client")
		}

		routers = append(routers, NewNodeRpcRouter(client))

		// Also add local router in case node rpc temporary unavailable
		localRouter, err := NewLocalRouterFromNodeRPC(client)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to new local router with node rpc")
		}
		routers = append(routers, localRouter)
	}

	// If redis and node rpc not configured, add local router from viper.
	if len(routers) == 0 {
		routers = append(routers, NewLocalRouterFromViper())
	}

	return NewChainedRouter(cfg.Router.ChainedFailover, routers...)
}

// chainedRouter routes RPC requests in chained responsibility pattern.
type chainedRouter struct {
	failover chainedFailoverConfig
	routers  []Router
}

func NewChainedRouter(failover chainedFailoverConfig, routers ...Router) Router {
	return &chainedRouter{failover: failover, routers: routers}
}

func (r *chainedRouter) Route(key []byte) string {
	for _, r := range r.routers {
		if val := r.Route(key); len(val) > 0 {
			return val
		}
	}

	l := logrus.WithFields(logrus.Fields{
		"failover": r.failover.URL, "key": string(key),
	})
	l.Warn("No router handled the route key, failover to chained default")

	return r.failover.URL
}

func (r *chainedRouter) WSRoute(key []byte) string {
	for _, r := range r.routers {
		if val := r.WSRoute(key); len(val) > 0 {
			return val
		}
	}

	l := logrus.WithFields(logrus.Fields{
		"failover": r.failover.WSURL, "key": string(key),
	})
	l.Warn("No router handled the websocket route key, failover to chained default")

	return r.failover.WSURL
}

// RedisRouter routes RPC requests via redis.
type RedisRouter struct {
	client *redis.Client
}

func NewRedisRouter(client *redis.Client) *RedisRouter {
	return &RedisRouter{
		client: client,
	}
}

func (r *RedisRouter) Route(key []byte) string {
	uintKey := xxhash.Sum64(key)
	redisKey := redisRepartitionKey(uintKey)

	return r.routeWithRedisKey(redisKey)
}

func (r *RedisRouter) WSRoute(key []byte) string {
	uintKey := xxhash.Sum64(key)
	redisKey := redisRepartitionKey(uintKey, "ws")

	return r.routeWithRedisKey(redisKey)
}

func (r *RedisRouter) routeWithRedisKey(redisKey string) string {
	node, err := r.client.Get(context.Background(), redisKey).Result()
	if err == redis.Nil {
		return ""
	}

	if err != nil {
		logrus.WithError(err).WithField("key", redisKey).Error("Failed to route key from redis")
		return ""
	}

	return node
}

// NodeRpcRouter routes RPC requests via node management RPC service.
type NodeRpcRouter struct {
	client *rpc.Client
}

func NewNodeRpcRouter(client *rpc.Client) *NodeRpcRouter {
	return &NodeRpcRouter{
		client: client,
	}
}

func (r *NodeRpcRouter) Route(key []byte) string {
	var result string
	if err := r.client.Call(&result, "node_route", hexutil.Bytes(key)); err != nil {
		logrus.WithError(err).Error("Failed to route key from node RPC")
		return ""
	}

	return result
}

func (r *NodeRpcRouter) WSRoute(key []byte) string {
	var result string
	if err := r.client.Call(&result, "node_wsRoute", hexutil.Bytes(key)); err != nil {
		logrus.WithError(err).Error("Failed to route key for websocket from node RPC")
		return ""
	}

	return result
}

type localNode string

func (n localNode) String() string { return string(n) }

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

// LocalRouter routes RPC requests based on local hash ring.
type LocalRouter struct {
	nodes    map[string]localNode // name -> node
	hashRing *consistent.Consistent

	// used for websocket routing
	wsNodes    map[string]localNode
	wsHashRing *consistent.Consistent
}

func NewLocalRouter(urls, wsUrls []string) *LocalRouter {
	batchNodes := make([]map[string]localNode, 2)
	batchHashRings := make([]*consistent.Consistent, 2)

	for i, urls := range [][]string{urls, wsUrls} {
		nodes := make(map[string]localNode)
		var members []consistent.Member

		for _, url := range urls {
			nodeName := util.Url2NodeName(url)
			if _, ok := nodes[nodeName]; !ok {
				nodes[nodeName] = localNode(url)
				members = append(members, localNode(url))
			}
		}

		batchNodes[i] = nodes
		batchHashRings[i] = consistent.New(members, cfg.HashRingRaw())
	}

	return &LocalRouter{
		nodes:      batchNodes[0],
		hashRing:   batchHashRings[0],
		wsNodes:    batchNodes[1],
		wsHashRing: batchHashRings[1],
	}
}

func NewLocalRouterFromViper() *LocalRouter {
	return NewLocalRouter(cfg.URLs, cfg.WSURLs)
}

func (r *LocalRouter) Route(key []byte) string {
	if member := r.hashRing.LocateKey(key); member != nil {
		return member.String()
	}
	return ""
}

func (r *LocalRouter) WSRoute(key []byte) string {
	if member := r.wsHashRing.LocateKey(key); member != nil {
		return member.String()
	}
	return ""
}

func NewLocalRouterFromNodeRPC(client *rpc.Client) (*LocalRouter, error) {
	batchUrls := make([][]string, 2)

	for i, method := range []string{"node_list", "node_wsList"} {
		var urls []string
		if err := client.Call(&urls, method); err != nil {
			logrus.WithError(err).WithField("rpcMethod", method).Error("Failed to get nodes from node RPC")
			return nil, err
		}

		batchUrls[i] = urls
	}

	router := NewLocalRouter(batchUrls[0], batchUrls[1])

	go router.update(client)

	return router, nil
}

func (r *LocalRouter) update(client *rpc.Client) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	// could update nodes periodically all the time
	for range ticker.C {
		for i, method := range []string{"node_list", "node_wsList"} {
			var urls []string
			if err := client.Call(&urls, method); err != nil {
				logrus.WithError(err).WithField("rpcMethod", method).Debug("Failed to get nodes from node RPC periodically")
				continue
			}

			r.updateOnce(urls, i == 1)
		}
	}
}

func (r *LocalRouter) updateOnce(urls []string, isWebsocket bool) {
	fnNodes, hashRing := r.nodes, r.hashRing
	if isWebsocket {
		fnNodes, hashRing = r.wsNodes, r.wsHashRing
	}

	// detect new added
	for _, v := range urls {
		nodeName := util.Url2NodeName(v)
		if _, ok := fnNodes[nodeName]; !ok {
			fnNodes[nodeName] = localNode(v)
			hashRing.Add(localNode(v))
		}
	}

	// detect removed
	nodes := make(map[string]bool)
	for _, v := range urls {
		nodes[util.Url2NodeName(v)] = true
	}

	var removed []string
	for name := range fnNodes {
		if !nodes[name] {
			removed = append(removed, name)
		}
	}

	for _, v := range removed {
		node := fnNodes[v]
		delete(fnNodes, v)
		hashRing.Remove(node.String())
	}
}
