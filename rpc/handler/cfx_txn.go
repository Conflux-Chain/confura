package handler

import (
	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/relay"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/openweb3/go-rpc-provider/utils"
	"github.com/sirupsen/logrus"
)

// CfxTxnHandler RPC handler to optimize sending transaction by relaying txn broadcasting asynchronously
// and replicating txn sending synchronously to all full nodes of some node group to improve consistency
// and availability once consistent hashing LB repartitioned.
type CfxTxnHandler struct {
	relayer relay.TxnRelayer    // transaction relayer
	nclient *rpc.Client         // node RPC client
	clients *util.ConcurrentMap // sdk clients: node name => RPC client
}

func MustNewCfxTxnHandler(relayer relay.TxnRelayer) *CfxTxnHandler {
	var nodeRpcClient *rpc.Client

	if nodeRpcUrl := node.Config().Router.NodeRPCURL; len(nodeRpcUrl) > 0 {
		var err error

		nodeRpcClient, err = rpc.DialHTTP(nodeRpcUrl)
		if err != nil {
			logrus.WithField("nodeRpcUrl", nodeRpcUrl).
				WithError(err).
				Fatal("Txn handler failed to create node RPC client")
		}
	}

	return &CfxTxnHandler{
		relayer: relayer,
		nclient: nodeRpcClient,
		clients: &util.ConcurrentMap{},
	}
}

func (h *CfxTxnHandler) SendRawTxn(cfx sdk.ClientOperator, group node.Group, signedTx hexutil.Bytes) (types.Hash, error) {
	txHash, err := cfx.SendRawTransaction(signedTx)
	if err != nil {
		return txHash, err
	}

	// relay transaction broadcasting asynchronously
	if h.relayer != nil && !h.relayer.Relay(signedTx) {
		logrus.Info("Txn relay pool is full, dropping transaction")
	}

	// replicate raw txn sending synchronously
	h.replicateRawTxnSendingByGroup(group, signedTx)
	return txHash, err
}

// replicateRawTxnSendingByGroup synchronously replicate raw txn sending to all full nodes of some specific group
func (h *CfxTxnHandler) replicateRawTxnSendingByGroup(group node.Group, signedTx hexutil.Bytes) {
	if h.nclient != nil { // fetch group nodes from node RPC
		var nodeUrls []string

		if err := h.nclient.Call(&nodeUrls, "node_list", group); err != nil {
			logrus.WithField("group", group).
				WithError(err).
				Error("Txn handler failed to get group full nodes from node RPC")
			return
		}

		h.replicateRawTxnSendingToNodes(nodeUrls, signedTx)
		return
	}

	// otherwise get group nodes from local config
	if conf, ok := node.CfxUrlConfig()[group]; ok {
		h.replicateRawTxnSendingToNodes(conf.Nodes, signedTx)
	}
}

func (h *CfxTxnHandler) replicateRawTxnSendingToNodes(nodeUrls []string, signedTx hexutil.Bytes) {
	for _, url := range nodeUrls {
		nodeName := rpcutil.Url2NodeName(url)
		c, _, err := h.clients.LoadOrStoreFnErr(nodeName, func(interface{}) (interface{}, error) {
			return rpcutil.NewCfxClient(url)
		})

		if err != nil {
			logrus.WithField("url", url).
				WithError(err).
				Error("Txn handler failed to new cfx client for raw txn replication")
			continue
		}

		_, err = c.(sdk.ClientOperator).SendRawTransaction(signedTx)
		if err != nil && !utils.IsRPCJSONError(err) {
			logrus.WithField("url", url).
				WithError(err).
				Info("Txn handler failed to replicate sending cfx raw transaction")
		}
	}
}
