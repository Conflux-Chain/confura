package node

import (
	"context"
	stderrors "errors"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	pb "github.com/Conflux-Chain/confura/node/router/proto"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Group allows to manage full nodes in multiple groups.
type Group string

const (
	// Note, group name must begin with cfx or eth as space name for metrics

	// core space fullnode groups
	GroupCfxHttp      Group = "cfxhttp"
	GroupCfxFullState Group = "cfxfullstate"
	GroupCfxWs        Group = "cfxws"
	GroupCfxFilter    Group = "cfxfilter"
	GroupCfxLogs      Group = "cfxlog"
	GroupCfxArchives  Group = "cfxarchives"

	// evm space fullnode groups
	GroupEthHttp      Group = "ethhttp"
	GroupEthFullState Group = "ethfullstate"
	GroupEthWs        Group = "ethws"
	GroupEthFilter    Group = "ethfilter"
	GroupEthLogs      Group = "ethlogs"
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
	// Route returns a URL, or an empty cache miss. A manager with no available
	// node returns ErrClientUnavailable; transport and protocol errors are retained.
	Route(ctx context.Context, group Group, key []byte) (string, error)
}

// NodeProvider provides full node URLs by group.
type NodeProvider interface {
	ListNodesByGroup(group Group) (urls []string)
}

// MustNewRouter creates an instance of Router.
func MustNewRouter(redisURL string, nodeRPCURL, nodeGRpcUrl string, groupConf map[Group]UrlConfig) Router {
	var routers []Router
	group2Urls := make(map[Group][]string, len(groupConf))
	for group, config := range groupConf {
		group2Urls[group] = config.Nodes
	}

	// Add redis router if configured
	if len(redisURL) > 0 {
		// redis://<user>:<password>@<host>:<port>/<db_number>
		opt, err := redis.ParseURL(redisURL)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to parse redis URL")
		}

		// The TLS dialer in go-redis v8 also needs an explicit dial limit.
		opt.DialTimeout = routerTimeout()
		client := redis.NewClient(opt)
		routers = append(routers, NewRedisRouter(client))
	}

	// Add node gRPC router if configured.
	// Note, gRPC is prior to RPC for performance consideration.
	if len(nodeGRpcUrl) > 0 {
		router, err := NewNodeGRPCRouter(nodeGRpcUrl)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to create gRPC router")
		}

		logrus.Info("Succeeded to create gRPC router")

		routers = append(routers, router)
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
		localRouter, err := NewLocalRouterFromNodeRPC(client)
		if err != nil {
			if !isRouterUnavailable(err) {
				logrus.WithError(err).Fatal("Failed to load local routes from node RPC")
			}
			// Presets are used only before the first successful full snapshot.
			localRouter.update(group2Urls)
			go localRouter.poll(client)
			logrus.WithError(err).Warn("Node manager unavailable; starting with configured local routes")
		}
		routers = append(routers, localRouter)
	}

	// If redis and node rpc not configured, add local router for failover.
	if len(routers) == 0 && len(groupConf) > 0 {
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

func (r *chainedRouter) Route(ctx context.Context, group Group, key []byte) (string, error) {
	var lastErr error
	for _, source := range r.routers {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		url, err := source.Route(ctx, group, key)
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if errors.Is(err, ErrClientUnavailable) {
			lastErr = nil
			break
		}
		if err != nil {
			if !isRouterUnavailable(err) {
				return "", err
			}
			lastErr = err
			continue
		}
		if url != "" {
			return url, nil
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if config := r.groupConf[group]; config.Failover != "" {
		return config.Failover, nil
	}
	if lastErr == nil {
		return "", ErrClientUnavailable
	}
	return "", stderrors.Join(ErrClientUnavailable, lastErr)
}

// RedisRouter routes RPC requests via redis.
// It should be used together with RedisRepartitionResolver.
type RedisRouter struct {
	client  *redis.Client
	timeout time.Duration
}

func NewRedisRouter(client *redis.Client) *RedisRouter {
	return &RedisRouter{
		client:  client.WithTimeout(routerTimeout()),
		timeout: routerTimeout(),
	}
}

func (r *RedisRouter) Route(ctx context.Context, group Group, key []byte) (string, error) {
	uintKey := xxhash.Sum64(key)
	redisKey := redisRepartitionKey(uintKey, string(group))

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	node, err := r.client.Get(ctx, redisKey).Result()
	if err == redis.Nil {
		return "", nil
	}

	if err != nil {
		return "", err
	}

	return node, nil
}

// NodeRpcRouter routes RPC requests via node management RPC service.
type NodeRpcRouter struct {
	client  *rpc.Client
	timeout time.Duration
}

func NewNodeRpcRouter(client *rpc.Client) *NodeRpcRouter {
	return &NodeRpcRouter{
		client:  client,
		timeout: routerTimeout(),
	}
}

func (r *NodeRpcRouter) Route(ctx context.Context, group Group, key []byte) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	var result string
	if err := r.client.CallContext(ctx, &result, "node_route", group, hexutil.Bytes(key)); err != nil {
		return "", err
	}

	if result == "" {
		return "", ErrClientUnavailable
	}
	return result, nil
}

// NodeGRPCRouter routes RPC requests via node management gRPC service.
type NodeGRPCRouter struct {
	client pb.RouterClient

	timeout time.Duration
}

func NewNodeGRPCRouter(url string) (*NodeGRPCRouter, error) {
	conn, err := grpc.Dial(
		url,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to dial grpc router")
	}

	return &NodeGRPCRouter{
		client:  pb.NewRouterClient(conn),
		timeout: routerTimeout(),
	}, nil
}

func (r *NodeGRPCRouter) Route(ctx context.Context, group Group, key []byte) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	req := pb.RouteRequest{
		Group: string(group),
		Key:   key,
	}

	resp, err := r.client.Route(ctx, &req)
	if err != nil {
		// The existing gRPC service reports unknown groups as InvalidArgument.
		if status.Code(err) == codes.InvalidArgument && status.Convert(err).Message() == "Invalid group ["+string(group)+"]" {
			return "", ErrClientUnavailable
		}
		return "", err
	}
	if resp.GetUrl() == "" {
		return "", ErrClientUnavailable
	}
	return resp.GetUrl(), nil
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
	mu      sync.Mutex
	groups  map[Group]*localNodeGroup
	timeout time.Duration
}

func NewLocalRouter(group2Urls map[Group][]string) *LocalRouter {
	groups := make(map[Group]*localNodeGroup)

	for k, v := range group2Urls {
		groups[k] = newLocalNodeGroup(v)
	}
	return &LocalRouter{groups: groups, timeout: routerTimeout()}
}

func (r *LocalRouter) Route(ctx context.Context, group Group, key []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	item, ok := r.groups[group]
	if !ok {
		return "", nil
	}

	if member := item.hashRing.LocateKey(key); member != nil {
		return member.String(), nil
	}

	return "", nil
}

// ListNodesByGroup returns all node URLs in a group.
func (r *LocalRouter) ListNodesByGroup(group Group) (urls []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	item, ok := r.groups[group]
	if !ok {
		return nil
	}

	for _, v := range item.nodes {
		urls = append(urls, v.String())
	}

	return urls
}

func NewLocalRouterFromNodeRPC(client *rpc.Client) (*LocalRouter, error) {
	router := &LocalRouter{
		groups:  make(map[Group]*localNodeGroup),
		timeout: routerTimeout(),
	}

	if err := router.pollOnce(client); err != nil {
		return router, err
	}

	go router.poll(client)

	return router, nil
}

func (r *LocalRouter) poll(client *rpc.Client) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	// could update nodes periodically all the time
	for range ticker.C {
		if err := r.pollOnce(client); err != nil {
			logrus.WithError(err).Error("Failed to poll route groups from node manager RPC")
		}
	}
}

func (r *LocalRouter) pollOnce(client *rpc.Client) error {
	groupNodes := make(map[Group][]string)

	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()
	if err := client.CallContext(ctx, &groupNodes, "node_listAll"); err != nil {
		return errors.WithMessage(err, "failed to list all group nodes")
	}

	r.update(groupNodes)
	return nil
}

// update publishes a successful full snapshot, including removed or empty groups.
func (r *LocalRouter) update(groupNodes map[Group][]string) {
	groups := make(map[Group]*localNodeGroup, len(groupNodes))
	for group, urls := range groupNodes {
		groups[group] = newLocalNodeGroup(urls)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.groups = groups
}

// isRouterUnavailable distinguishes control-plane outages from rejected or
// malformed requests. Only outages permit using a less authoritative source.
func isRouterUnavailable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, rpc.ErrClientQuit) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	}
	var httpErr rpc.HTTPError
	if errors.As(err, &httpErr) {
		switch httpErr.StatusCode {
		case 408, 502, 503, 504:
			return true
		default:
			return false
		}
	}
	// url.Error also implements net.Error for failures such as bad certificates.
	// Inspect its cause before deciding that the failure is a network outage.
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return isRouterUnavailable(urlErr.Err)
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}
