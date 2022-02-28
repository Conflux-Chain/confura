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

var ErrClientUnavailable = errors.New("No full node available")

// ClientProvider provides different RPC client based on user IP to achieve load balance.
// Generally, it is used by RPC server to delegate RPC requests to full node cluster.
type ClientProvider struct {
	router  Router
	clients map[Group]*util.ConcurrentMap // group => node name => RPC client
}

func NewClientProvider(router Router) *ClientProvider {
	clients := make(map[Group]*util.ConcurrentMap)
	for k := range urlCfg {
		clients[k] = &util.ConcurrentMap{}
	}

	return &ClientProvider{router, clients}
}

func (p *ClientProvider) GetClientByIP(ctx context.Context) (sdk.ClientOperator, error) {
	remoteAddr := remoteAddrFromContext(ctx)
	return p.GetClient(remoteAddr, GroupCfxHttp)
}

func (p *ClientProvider) GetClientByIPGroup(ctx context.Context, group Group) (sdk.ClientOperator, error) {
	remoteAddr := remoteAddrFromContext(ctx)
	return p.GetClient(remoteAddr, group)
}

func (p *ClientProvider) GetClient(key string, group Group) (sdk.ClientOperator, error) {
	clients, ok := p.clients[group]
	if !ok {
		return nil, errors.Errorf("Unknown group %v", group)
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

	nodeName := util.Url2NodeName(url)

	logger = logger.WithFields(logrus.Fields{
		"node": nodeName,
		"url":  url,
	})
	logger.Trace("Route RPC requests")

	client, loaded, err := clients.LoadOrStoreFnErr(nodeName, func(interface{}) (interface{}, error) {
		// TODO improvements required
		// 1. Necessary retry? (but longer timeout). Better to let user side to decide.
		// 2. Different metrics for different full nodes.

		requestTimeout := viper.GetDuration("cfx.requestTimeout")
		cfx, err := sdk.NewClient(url, sdk.ClientOption{
			RequestTimeout: requestTimeout,
		})
		if err == nil {
			util.HookCfxRpcMetricsMiddleware(cfx)
		}

		return cfx, err
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
