package virtualfilter

import (
	"github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util/rpc"
)

// MustServeFromViper creates virtual filters RPC server from viper settings
func MustNewServerFromViper(
	shutdownContext util.GracefulShutdownContext, vfls *mysql.VirtualFilterLogStore, handler *handler.EthLogsApiHandler,
) (*rpc.Server, string) {
	conf := mustNewConfigFromViper()

	var fs *FilterSystem
	if handler != nil {
		fs = NewFilterSystem(shutdownContext, vfls, handler, conf)
	}

	srv := rpc.MustNewServer("vfilter", map[string]interface{}{
		"eth": NewFilterApi(fs, conf.TTL),
	})

	return srv, conf.Endpoint
}
