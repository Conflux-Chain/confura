package rpc

import (
	infuraNode "github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/relay"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/sirupsen/logrus"
)

const (
	nativeSpaceRpcServerName = "native_space_rpc"
	evmSpaceRpcServerName    = "evm_space_rpc"

	nativeSpaceBridgeRpcServerName = "native_space_bridge_rpc"
)

// MustNewNativeSpaceServer new native space RPC server by specifying router, handler
// and exposed modules.  Argument exposedModules is a list of API modules to expose
// via the RPC interface. If the module list is empty, all RPC API endpoints designated
// public will be exposed.
func MustNewNativeSpaceServer(
	router infuraNode.Router, handler cfxHandler, gashandler *GasStationHandler,
	exposedModules []string, relayer *relay.TxnRelayer,
) *util.RpcServer {
	// retrieve all available native space rpc apis
	allApis := nativeSpaceApis(router, handler, gashandler, relayer)

	exposedApis, err := filterExposedApis(allApis, exposedModules)
	if err != nil {
		logrus.WithError(err).Fatal(
			"Failed to new native space RPC server with bad exposed modules",
		)
	}

	return util.MustNewRpcServer(nativeSpaceRpcServerName, exposedApis)
}

// MustNewNativeSpaceServer new EVM space RPC server by specifying router, and exposed modules.
// `exposedModules` is a list of API modules to expose via the RPC interface. If the module
// list is empty, all RPC API endpoints designated public will be exposed.
func MustNewEvmSpaceServer(ethNodeURL string, exposedModules []string) *util.RpcServer {
	// retrieve all available EVM space rpc apis
	allApis, err := evmSpaceApis(ethNodeURL)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to new EVM space RPC server")
	}

	exposedApis, err := filterExposedApis(allApis, exposedModules)
	if err != nil {
		logrus.WithError(err).Fatal(
			"Failed to new EVM space RPC server with bad exposed modules",
		)
	}

	return util.MustNewRpcServer(evmSpaceRpcServerName, exposedApis)
}

func MustNewNativeSpaceBridgeServer(ethNodeURL, cfxNodeURL string, exposedModules []string) *util.RpcServer {
	allApis, err := nativeSpaceBridgeApis(ethNodeURL, cfxNodeURL)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to new CFX bridge RPC server")
	}

	exposedApis, err := filterExposedApis(allApis, exposedModules)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to new CFX bridge RPC server with bad exposed modules")
	}

	return util.MustNewRpcServer(nativeSpaceBridgeRpcServerName, exposedApis)
}
