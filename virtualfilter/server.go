package virtualfilter

import (
	"github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util/rpc"
)

// MustNewEvmSpaceServerFromViper creates evm space virtual filters RPC server from viper settings
func MustNewEvmSpaceServerFromViper(
	shutdownContext util.GracefulShutdownContext,
	vfls *mysql.VirtualFilterLogStore,
) (*rpc.Server, string) {
	conf := mustNewEthConfigFromViper()
	fs := newEthFilterSystem(conf, vfls, shutdownContext)

	srv := rpc.MustNewServer("eth_vfilter", map[string]any{
		"eth": newEthFilterApi(fs),
	})

	return srv, conf.Endpoint
}

// MustNewCoreSpaceServerFromViper creates core space virtual filters RPC server from viper settings
func MustNewCoreSpaceServerFromViper(
	shutdownContext util.GracefulShutdownContext,
	vfls *mysql.VirtualFilterLogStore,
) (*rpc.Server, string) {
	conf := mustNewCfxConfigFromViper()
	fs := newCfxFilterSystem(conf, vfls, shutdownContext)

	srv := rpc.MustNewServer("cfx_vfilter", map[string]any{
		"cfx": newCfxFilterApi(fs),
	})

	return srv, conf.Endpoint
}
