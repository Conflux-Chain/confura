package virtualfilter

import (
	"fmt"

	"github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util/rpc"
	"github.com/Conflux-Chain/confura/util/viper"
	"github.com/sirupsen/logrus"
)

// NewEvmSpaceServerFromViper creates the evm space virtual filters RPC server.
func NewEvmSpaceServerFromViper(
	shutdownContext util.GracefulShutdownContext,
	vfls *mysql.VirtualFilterLogStore,
) (*rpc.Server, string, error) {
	conf := new(ethConfig)
	if err := viper.UnmarshalKey("ethVirtualFilters", conf); err != nil {
		return nil, "", err
	}
	if conf.TTL < 2 || conf.MaxFilterChainNodes <= 0 {
		return nil, "", fmt.Errorf("invalid virtual filter TTL or capacity")
	}
	fs := newEthFilterSystem(conf, vfls, shutdownContext)

	srv, err := rpc.NewServer("eth_vfilter", map[string]interface{}{
		"eth": newEthFilterApi(fs),
	}, nil)

	return srv, conf.Endpoint, err
}

// NewCoreSpaceServerFromViper creates the core space virtual filters RPC server.
func NewCoreSpaceServerFromViper(
	shutdownContext util.GracefulShutdownContext,
	vfls *mysql.VirtualFilterLogStore,
) (*rpc.Server, string, error) {
	conf := new(cfxConfig)
	if err := viper.UnmarshalKey("virtualFilters", conf); err != nil {
		return nil, "", err
	}
	if conf.TTL < 2 || conf.MaxFilterChainNodes <= 0 {
		return nil, "", fmt.Errorf("invalid virtual filter TTL or capacity")
	}
	fs := newCfxFilterSystem(conf, vfls, shutdownContext)

	srv, err := rpc.NewServer("cfx_vfilter", map[string]interface{}{
		"cfx": newCfxFilterApi(fs),
	}, nil)

	return srv, conf.Endpoint, err
}

func MustNewEvmSpaceServerFromViper(ctx util.GracefulShutdownContext, store *mysql.VirtualFilterLogStore) (*rpc.Server, string) {
	srv, endpoint, err := NewEvmSpaceServerFromViper(ctx, store)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize evm virtual filters")
	}
	return srv, endpoint
}

func MustNewCoreSpaceServerFromViper(ctx util.GracefulShutdownContext, store *mysql.VirtualFilterLogStore) (*rpc.Server, string) {
	srv, endpoint, err := NewCoreSpaceServerFromViper(ctx, store)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize core virtual filters")
	}
	return srv, endpoint
}
