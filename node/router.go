package node

import (
	"context"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

// Router is used to route RPC requests to multiple full nodes.
type Router interface {
	// Route returns the full node URL for specified key.
	Route(key []byte) string
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

	return NewChainedRouter(routers...)
}

// chainedRouter routes RPC requests in chained responsibility pattern.
type chainedRouter struct {
	routers []Router
}

func NewChainedRouter(routers ...Router) Router {
	return &chainedRouter{routers}
}

func (r *chainedRouter) Route(key []byte) string {
	for _, r := range r.routers {
		if val := r.Route(key); len(val) > 0 {
			return val
		}
	}

	logrus.Warn("No router handled the key")

	return ""
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
}

func NewLocalRouter(urls []string) *LocalRouter {
	nodes := make(map[string]localNode)
	var members []consistent.Member

	for _, url := range urls {
		nodeName := url2NodeName(url)
		if _, ok := nodes[nodeName]; !ok {
			nodes[nodeName] = localNode(url)
			members = append(members, localNode(url))
		}
	}

	return &LocalRouter{
		nodes:    nodes,
		hashRing: consistent.New(members, cfg.HashRing),
	}
}

func NewLocalRouterFromViper() *LocalRouter {
	return NewLocalRouter(cfg.URLs)
}

func (r *LocalRouter) Route(key []byte) string {
	return r.hashRing.LocateKey(key).String()
}

func NewLocalRouterFromNodeRPC(client *rpc.Client) (*LocalRouter, error) {
	var urls []string
	if err := client.Call(&urls, "node_list"); err != nil {
		logrus.WithError(err).Error("Failed to get nodes from node RPC")
		return nil, err
	}

	router := NewLocalRouter(urls)

	go router.update(client)

	return router, nil
}

func (r *LocalRouter) update(client *rpc.Client) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	// could update nodes periodically all the time
	for range ticker.C {
		var urls []string
		if err := client.Call(&urls, "node_list"); err != nil {
			logrus.WithError(err).Debug("Failed to get nodes from node RPC periodically")
		} else {
			r.updateOnce(urls)
		}
	}
}

func (r *LocalRouter) updateOnce(urls []string) {
	// detect new added
	for _, v := range urls {
		nodeName := url2NodeName(v)
		if _, ok := r.nodes[nodeName]; !ok {
			r.nodes[nodeName] = localNode(v)
			r.hashRing.Add(localNode(v))
		}
	}

	// detect removed
	nodes := make(map[string]bool)
	for _, v := range urls {
		nodes[url2NodeName(v)] = true
	}

	var removed []string
	for name := range r.nodes {
		if !nodes[name] {
			removed = append(removed, name)
		}
	}

	for _, v := range removed {
		node := r.nodes[v]
		delete(r.nodes, v)
		r.hashRing.Remove(node.String())
	}
}
