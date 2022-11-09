package virtualfilter

import (
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/util/rpc"
)

// MustNewServer creates virtual filters RPC server from viper settings
func MustNewServerFromViper(handler *handler.EthLogsApiHandler) *rpc.Server {
	conf := mustNewConfigFromViper()
	fs := NewFilterSystem(handler, conf)

	return rpc.MustNewServer("node", map[string]interface{}{
		"eth": NewFilterApi(fs, conf.TTL),
	})
}
