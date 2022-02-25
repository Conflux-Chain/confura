package node

import (
	"context"
	"strings"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// ClientProvider provides different RPC client based on user IP to achieve load balance.
// Generally, it is used by RPC server to delegate RPC requests to full node cluster.
type ClientProvider struct {
	router    Router
	clients   util.ConcurrentMap // node name => RPC client (HTTP)
	wsClients util.ConcurrentMap // node name => RPC client (WebSocket)
}

func NewClientProvider(router Router) *ClientProvider {
	return &ClientProvider{
		router: router,
	}
}

func (p *ClientProvider) GetClientByIP(ctx context.Context) (sdk.ClientOperator, error) {
	remoteAddr := remoteAddrFromContext(ctx)
	return p.GetClient(remoteAddr, false)
}

func (p *ClientProvider) GetWSClientByIP(ctx context.Context) (sdk.ClientOperator, error) {
	remoteAddr := remoteAddrFromContext(ctx)
	return p.GetClient(remoteAddr, true)
}

func (p *ClientProvider) GetClient(key string, isWebsocket bool) (sdk.ClientOperator, error) {
	var url string
	var mapClients *util.ConcurrentMap

	if isWebsocket {
		url = p.router.WSRoute([]byte(key))
		mapClients = &p.wsClients
	} else {
		url = p.router.Route([]byte(key))
		mapClients = &p.clients
	}

	logger := logrus.WithFields(logrus.Fields{
		"key": key, "isWebsocket": isWebsocket,
	})

	if len(url) == 0 {
		err := errors.New("no full node routed from the router")
		logger.WithError(err).Error("Failed to get full node client from provider")

		return nil, err
	}

	nodeName := util.Url2NodeName(url)

	logger = logger.WithFields(logrus.Fields{
		"node": nodeName, "url": url,
	})
	logger.Trace("Route RPC requests")

	client, loaded, err := mapClients.LoadOrStoreFnErr(nodeName, func(interface{}) (interface{}, error) {
		// TODO improvements required
		// 1. Necessary retry? (but longer timeout). Better to let user side to decide.
		// 2. Different metrics for different full nodes.

		requestTimeout := viper.GetDuration("cfx.requestTimeout")
		return sdk.NewClient(url, sdk.ClientOption{
			RequestTimeout: requestTimeout,
		})
	})

	if err != nil {
		err := errors.WithMessage(err, "bad full node connection")
		logger.WithError(err).Error("Failed to get full node client from provider")

		return nil, err
	}

	if !loaded {
		logger.Info("Succeeded to connect to full node")
	}

	return client.(sdk.ClientOperator), nil
}

func remoteAddrFromContext(ctx context.Context) string {
	// http.Request.RemoteAddr in string type
	remoteAddr := ctx.Value("remote").(string)
	if idx := strings.Index(remoteAddr, ":"); idx != -1 {
		remoteAddr = remoteAddr[:idx]
	}

	return remoteAddr
}
