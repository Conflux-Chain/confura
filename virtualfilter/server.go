package virtualfilter

import (
	"github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util/rpc"
)

// MustNewEvmSpaceServerFromViper creates evm space virtual filters RPC server from viper settings
func MustNewEvmSpaceServerFromViper(
	shutdownContext util.GracefulShutdownContext,
	vfls *mysql.VirtualFilterLogStore,
	handler *handler.EthLogsApiHandler,
) (*rpc.Server, string) {
	conf := mustNewEthConfigFromViper()
	fs := newEthFilterSystem(conf, vfls, shutdownContext)

	srv := rpc.MustNewServer("eth_vfilter", map[string]interface{}{
		"eth": newEthFilterApi(fs, handler),
	})

	return srv, conf.Endpoint
}
