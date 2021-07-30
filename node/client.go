package node

import (
	"context"
	"strings"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type ClientProvider struct {
	router    Router
	clients   util.ConcurrentMap
	wsClients util.ConcurrentMap // used for pubsub on connection to fullnode
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

	nodeName := util.Url2NodeName(url)

	logrus.WithFields(logrus.Fields{
		"key":  key,
		"node": nodeName,
		"url":  url,
	}).Trace("Route RPC requests")

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
		logrus.WithError(err).WithField("url", url).Error("Failed to connect to full node")
		return nil, err
	}

	if !loaded {
		logrus.WithFields(logrus.Fields{
			"node": nodeName,
			"url":  url,
		}).Info("Succeeded to connect to full node")
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
