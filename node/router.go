package node

import (
	"context"
	"strings"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	rpcutil "github.com/conflux-chain/conflux-infura/util/rpc"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

// Group allows to manage full nodes in multiple groups.
type Group string

const (
	// Note, group name must begin with cfx or eth as space name for metrics

	// core space fullnode groups
	GroupCfxHttp     = "cfxhttp"
	GroupCfxWs       = "cfxws"
	GroupCfxLogs     = "cfxlog"
	GroupCfxArchives = "cfxarchives"

	// evm space fullnode groups
	GroupEthHttp = "ethhttp"
	GroupEthLogs = "ethlogs"
)

// Space parses space from group name
func (g Group) Space() string {
	if strings.HasPrefix(string(g), "eth") {
		return "eth"
	}

	return "cfx"
}

func (g Group) String() string {
	return string(g)
}

// Router is used to route RPC requests to multiple full nodes.
type Router interface {
	// Route returns the full node URL for specified group and key.
	Route(group Group, key []byte) string
}

// MustNewRouter creates an instance of Router.
func MustNewRouter(redisURL string, nodeRPCURL string, groupConf map[Group]UrlConfig) Router {
	var routers []Router

	// Add redis router if configured
	if len(redisURL) > 0 {
		// redis://<user>:<password>@<host>:<port>/<db_number>
		opt, err := redis.ParseURL(redisURL)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to parse redis URL")
		}

		client := redis.NewClient(opt)
		routers = append(routers, NewRedisRouter(client))
	}

	// Add node rpc router if configured
	if len(nodeRPCURL) > 0 {
		// http://127.0.0.1:22530
		client, err := rpc.DialHTTP(nodeRPCURL)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to create rpc client")
		}

		routers = append(routers, NewNodeRpcRouter(client))

		// Also add local router in case node rpc temporary unavailable
		localRouter, err := NewLocalRouterFromNodeRPC(client, groupConf)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to new local router with node rpc")
		}
		routers = append(routers, localRouter)
	}

	// If redis and node rpc not configured, add local router for failover.
	if len(routers) == 0 && len(groupConf) > 0 {
		group2Urls := make(map[Group][]string)
		for k, v := range groupConf {
			group2Urls[k] = v.Nodes
		}

		routers = append(routers, NewLocalRouter(group2Urls))
	}

	return NewChainedRouter(groupConf, routers...)
}

// chainedRouter routes RPC requests in chained responsibility pattern.
type chainedRouter struct {
	groupConf map[Group]UrlConfig
	routers   []Router
}

func NewChainedRouter(groupConf map[Group]UrlConfig, routers ...Router) Router {
	return &chainedRouter{
		groupConf: groupConf,
		routers:   routers,
	}
}

func (r *chainedRouter) Route(group Group, key []byte) string {
	for _, r := range r.routers {
		if val := r.Route(group, key); len(val) > 0 {
			return val
		}
	}

	// Failover if configured
	config, ok := r.groupConf[group]
	if !ok {
		return ""
	}

	logrus.WithFields(logrus.Fields{
		"failover": config.Failover,
		"key":      string(key),
	}).Warn("No router handled the route key, failover to chained default")

	return config.Failover
}

// RedisRouter routes RPC requests via redis.
// It should be used together with RedisRepartitionResolver.
type RedisRouter struct {
	client *redis.Client
}

func NewRedisRouter(client *redis.Client) *RedisRouter {
	return &RedisRouter{
		client: client,
	}
}

func (r *RedisRouter) Route(group Group, key []byte) string {
	uintKey := xxhash.Sum64(key)
	redisKey := redisRepartitionKey(uintKey, string(group))

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

func (r *NodeRpcRouter) Route(group Group, key []byte) string {
	var result string
	if err := r.client.Call(&result, "node_route", group, hexutil.Bytes(key)); err != nil {
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

type localNodeGroup struct {
	nodes    map[string]localNode // name -> node URL
	hashRing *consistent.Consistent
}

func newLocalNodeGroup(urls []string) *localNodeGroup {
	item := localNodeGroup{
		nodes: make(map[string]localNode),
	}

	var members []consistent.Member

	for _, v := range urls {
		nodeName := rpcutil.Url2NodeName(v)
		if _, ok := item.nodes[nodeName]; !ok {
			item.nodes[nodeName] = localNode(v)
			members = append(members, localNode(v))
		}
	}

	item.hashRing = consistent.New(members, cfg.HashRingRaw())

	return &item
}

// LocalRouter routes RPC requests based on local hash ring.
type LocalRouter struct {
	groups map[Group]*localNodeGroup
}

func NewLocalRouter(group2Urls map[Group][]string) *LocalRouter {
	groups := make(map[Group]*localNodeGroup)

	for k, v := range group2Urls {
		groups[k] = newLocalNodeGroup(v)
	}

	return &LocalRouter{groups}
}

func (r *LocalRouter) Route(group Group, key []byte) string {
	item, ok := r.groups[group]
	if !ok {
		return ""
	}

	if member := item.hashRing.LocateKey(key); member != nil {
		return member.String()
	}

	return ""
}

func NewLocalRouterFromNodeRPC(client *rpc.Client, groupConf map[Group]UrlConfig) (*LocalRouter, error) {
	group2Urls := make(map[Group][]string)

	for key := range groupConf {
		var urls []string
		if err := client.Call(&urls, "node_list", key); err != nil {
			logrus.WithError(err).WithField("group", key).Error("Failed to get nodes from node manager RPC")
			return nil, err
		}

		group2Urls[key] = urls
	}

	router := NewLocalRouter(group2Urls)

	go router.update(client)

	return router, nil
}

func (r *LocalRouter) update(client *rpc.Client) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	// could update nodes periodically all the time
	for range ticker.C {
		for grp := range r.groups {
			var urls []string
			if err := client.Call(&urls, "node_list", grp); err != nil {
				logrus.WithError(err).WithField("group", grp).Debug("Failed to get nodes from node manager RPC periodically")
				continue
			}

			r.updateOnce(urls, grp)
		}
	}
}

func (r *LocalRouter) updateOnce(urls []string, group Group) {
	fnNodes, hashRing := r.groups[group].nodes, r.groups[group].hashRing

	// detect new added
	for _, v := range urls {
		nodeName := rpcutil.Url2NodeName(v)
		if _, ok := fnNodes[nodeName]; !ok {
			fnNodes[nodeName] = localNode(v)
			hashRing.Add(localNode(v))
		}
	}

	// detect removed
	nodes := make(map[string]bool)
	for _, v := range urls {
		nodes[rpcutil.Url2NodeName(v)] = true
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
