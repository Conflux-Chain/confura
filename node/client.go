package node

import (
	"context"
	"net/http"
	"sync"

	"github.com/conflux-chain/conflux-infura/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var ErrClientUnavailable = errors.New("No full node available")

// clientFactory factory methods to create RPC client
type clientFactory func(url string) (interface{}, error)

// clientProvider provides different RPC client based on user IP to achieve load balance.
// Generally, it is used by RPC server to delegate RPC requests to full node cluster.
type clientProvider struct {
	router  Router
	clients map[Group]*util.ConcurrentMap // group => node name => RPC client
	factory clientFactory
	mutex   sync.Mutex
}

func newClientProvider(router Router, factory clientFactory) *clientProvider {
	return &clientProvider{
		router: router, factory: factory,
		clients: make(map[Group]*util.ConcurrentMap),
	}
}

func (p *clientProvider) registerGroup(group Group) *util.ConcurrentMap {
	if _, ok := p.clients[group]; !ok {
		p.mutex.Lock()
		defer p.mutex.Unlock()

		if _, ok := p.clients[group]; !ok { // double check
			p.clients[group] = &util.ConcurrentMap{}
		}
	}

	return p.clients[group]
}

func (p *clientProvider) getClient(key string, group Group) (interface{}, error) {
	clients, ok := p.clients[group]
	if !ok {
		return nil, errors.Errorf("Unknown node group %v", group)
	}

	url := p.router.Route(group, []byte(key))

	logger := logrus.WithFields(logrus.Fields{
		"key":   key,
		"group": group,
	})

	if len(url) == 0 {
		logger.WithError(ErrClientUnavailable).Error("Failed to get full node client from provider")
		return nil, ErrClientUnavailable
	}

	nodeName := Url2NodeName(url)

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
	request := ctx.Value("request").(*http.Request)
	remoteAddr := util.GetIPAddress(request)
	logrus.WithField("remoteAddr", remoteAddr).Debug("Get remote address from context")
	return remoteAddr
}
