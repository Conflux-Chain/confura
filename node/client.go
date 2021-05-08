package node

import (
	"context"
	"strings"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/sirupsen/logrus"
)

// Router is used to route RPC requests to multiple full nodes.
type Router interface {
	// Route returns the full node URL for specified key.
	Route(key []byte) string
}

type ClientProvider struct {
	router  Router
	clients util.ConcurrentMap
}

func NewClientProvider(router Router) *ClientProvider {
	return &ClientProvider{
		router: router,
	}
}

func (p *ClientProvider) GetClientByIP(ctx context.Context) (sdk.ClientOperator, error) {
	// http.Request.RemoteAddr in string type
	remoteAddr := ctx.Value("remote").(string)
	if idx := strings.Index(remoteAddr, ":"); idx != -1 {
		remoteAddr = remoteAddr[:idx]
	}
	return p.GetClient(remoteAddr)
}

func (p *ClientProvider) GetClient(key string) (sdk.ClientOperator, error) {
	url := p.router.Route([]byte(key))
	nodeName := url2NodeName(url)

	logrus.WithFields(logrus.Fields{
		"key":  key,
		"node": nodeName,
	}).Trace("Route RPC requests")

	client, loaded, err := p.clients.LoadOrStoreFnErr(nodeName, func(interface{}) (interface{}, error) {
		// TODO improvements required
		// 1. Necessary retry? (but longer timeout). Better to let user side to decide.
		// 2. Different metrics for different full nodes.
		return sdk.NewClient(url)
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
