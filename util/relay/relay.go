package relay

import (
	"time"

	"github.com/Conflux-Chain/confura/util"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/openweb3/web3go"
	"github.com/sirupsen/logrus"
)

type TxnRelayerConfig struct {
	BufferSize     int           `default:"2000"`
	Concurrency    int           `default:"1"`
	Retry          int           `default:"3"`
	RetryInterval  time.Duration `default:"1s"`
	RequestTimeout time.Duration `default:"3s"`
	NodeUrls       []string
	EthNodeUrls    []string
}

// TxnRelayer relays raw transaction by broadcasting to node pool
// of different regions to accelerate P2P diffusion.
type TxnRelayer interface {
	// relays raw transaction broadcasting asynchronously.
	Relay(signedTx hexutil.Bytes) bool
}

func MustNewTxnRelayerFromViper() TxnRelayer {
	var relayConf TxnRelayerConfig
	viper.MustUnmarshalKey("relay", &relayConf)

	return newCfxTxnRelayer(&relayConf)
}

func MustNewEthTxnRelayerFromViper() TxnRelayer {
	var relayConf TxnRelayerConfig
	viper.MustUnmarshalKey("relay", &relayConf)

	return newEthTxnRelayer(&relayConf)
}

// txnRelayer base transaction relayer
type txnRelayer struct {
	*TxnRelayerConfig

	poolClients *util.ConcurrentMap // fullnode client pool
	txnQueue    chan hexutil.Bytes  // transactions queued to relay
}

func newTxnRelayer(relayConf *TxnRelayerConfig) *txnRelayer {
	return &txnRelayer{
		TxnRelayerConfig: relayConf,
		poolClients:      &util.ConcurrentMap{},
		txnQueue:         make(chan hexutil.Bytes, relayConf.BufferSize),
	}
}

// Relay relays raw transaction broadcasting asynchronously.
func (relayer *txnRelayer) Relay(signedTx hexutil.Bytes) bool {
	if len(relayer.txnQueue) == relayer.BufferSize { // queue is full?
		return false
	}

	relayer.txnQueue <- signedTx
	return true
}

// run starts concurrency worker(s) asynchronously
func (relayer *txnRelayer) run(process func()) {
	concurrency := max(relayer.Concurrency, 1)
	for i := 0; i < concurrency; i++ {
		go process()
	}
}

// cfxTxnRelayer core space transaction relayer
type cfxTxnRelayer struct {
	*txnRelayer
}

func newCfxTxnRelayer(relayConf *TxnRelayerConfig) *cfxTxnRelayer {
	relayer := &cfxTxnRelayer{
		txnRelayer: newTxnRelayer(relayConf),
	}

	if len(relayConf.NodeUrls) > 0 {
		relayer.run(relayer.process)
	}

	return relayer
}

// Relay implements `TxnRelayer` interface
func (relayer *cfxTxnRelayer) Relay(signedTx hexutil.Bytes) bool {
	if len(relayer.NodeUrls) == 0 {
		return true
	}

	return relayer.txnRelayer.Relay(signedTx)
}

// process processes raw transaction queue
func (relayer *cfxTxnRelayer) process() {
	for rawTxn := range relayer.txnQueue {
		relayer.relay(rawTxn)
	}
}

func (relayer *cfxTxnRelayer) relay(signedTx hexutil.Bytes) {
	for _, url := range relayer.NodeUrls {
		nodeName := rpcutil.Url2NodeName(url)
		cfx, _, err := relayer.poolClients.LoadOrStoreFnErr(nodeName, func(k any) (any, error) {
			return sdk.NewClient(url, sdk.ClientOption{
				RetryCount:     relayer.Retry,
				RetryInterval:  relayer.RetryInterval,
				RequestTimeout: relayer.RequestTimeout,
			})
		})

		if err != nil {
			logrus.WithField("url", url).Trace("Txn relayer failed to new cfx client")
			continue
		}

		txHash, err := cfx.(sdk.ClientOperator).SendRawTransaction(signedTx)
		logrus.WithFields(logrus.Fields{
			"url":    url,
			"txHash": txHash,
		}).WithError(err).Trace("Core space raw transaction relayed")
	}
}

// ethTxnRelayer evm space transaction relayer
type ethTxnRelayer struct {
	*txnRelayer
}

func newEthTxnRelayer(relayConf *TxnRelayerConfig) *ethTxnRelayer {
	relayer := &ethTxnRelayer{
		txnRelayer: newTxnRelayer(relayConf),
	}

	if len(relayConf.EthNodeUrls) > 0 {
		relayer.run(relayer.process)
	}

	return relayer
}

// Relay implements `TxnRelayer` interface
func (relayer *ethTxnRelayer) Relay(signedTx hexutil.Bytes) bool {
	if len(relayer.EthNodeUrls) == 0 {
		return true
	}

	return relayer.txnRelayer.Relay(signedTx)
}

// process processes raw transaction queue
func (relayer *ethTxnRelayer) process() {
	for rawTxn := range relayer.txnQueue {
		relayer.relay(rawTxn)
	}
}

func (relayer *ethTxnRelayer) relay(signedTx hexutil.Bytes) {
	for _, url := range relayer.EthNodeUrls {
		nodeName := rpcutil.Url2NodeName(url)
		w3c, _, err := relayer.poolClients.LoadOrStoreFnErr(nodeName, func(k any) (any, error) {
			return rpcutil.NewEthClient(url,
				rpcutil.WithClientRequestTimeout(relayer.RequestTimeout),
				rpcutil.WithClientRetryInterval(relayer.RetryInterval),
				rpcutil.WithClientRetryCount(relayer.Retry),
			)
		})

		if err != nil {
			logrus.WithField("url", url).Trace("Txn relayer failed to new eth client")
			continue
		}

		txHash, err := w3c.(*web3go.Client).Eth.SendRawTransaction(signedTx)
		logrus.WithFields(logrus.Fields{
			"url":    url,
			"txHash": txHash,
		}).WithError(err).Trace("Evm space raw transaction relayed")
	}
}
