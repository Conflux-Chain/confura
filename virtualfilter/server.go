package virtualfilter

import (
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util/rpc"
)

// MustNewServer creates virtual filters RPC server from viper settings
func MustNewServerFromViper(vfls *mysql.VirtualFilterLogStore, handler *handler.EthLogsApiHandler) *rpc.Server {
	conf := mustNewConfigFromViper()

	var fs *FilterSystem
	if handler != nil {
		fs = NewFilterSystem(vfls, handler, conf)
	}

	return rpc.MustNewServer("vfilter", map[string]interface{}{
		"eth": NewFilterApi(fs, conf.TTL),
	})
}
