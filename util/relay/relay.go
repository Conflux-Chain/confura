package relay

import (
	"time"

	"github.com/Conflux-Chain/confura/util"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/sirupsen/logrus"
)

type TxnRelayerConfig struct {
	BufferSize     int           `default:"2000"`
	Concurrency    int           `default:"1"`
	Retry          int           `default:"3"`
	RetryInterval  time.Duration `default:"1s"`
	RequestTimeout time.Duration `default:"3s"`
	NodeUrls       []string
}

// TxnRelayer relays raw transaction by broadcasting to node pool
// of different regions to accelerate P2P diffusion.
type TxnRelayer struct {
	poolClients *util.ConcurrentMap // fullnode client pool
	txnQueue    chan hexutil.Bytes  // transactions queued to relay
	config      *TxnRelayerConfig
}

func MustNewTxnRelayerFromViper() *TxnRelayer {
	var relayConf TxnRelayerConfig
	viper.MustUnmarshalKey("relay", &relayConf)

	return NewTxnRelayer(&relayConf)
}

func NewTxnRelayer(relayConf *TxnRelayerConfig) *TxnRelayer {
	if len(relayConf.NodeUrls) == 0 {
		return &TxnRelayer{config: relayConf}
	}

	relayer := &TxnRelayer{
		poolClients: &util.ConcurrentMap{},
		txnQueue:    make(chan hexutil.Bytes, relayConf.BufferSize),
		config:      relayConf,
	}

	// start concurrency worker(s)
	concurrency := util.MaxInt(relayConf.Concurrency, 1)
	for i := 0; i < concurrency; i++ {
		go func() {
			for rawTxn := range relayer.txnQueue {
				relayer.doRelay(rawTxn)
			}
		}()
	}

	return relayer
}

// AsyncRelay relays raw transaction broadcasting asynchronously.
func (relayer *TxnRelayer) AsyncRelay(signedTx hexutil.Bytes) bool {
	if len(relayer.config.NodeUrls) == 0 {
		return true
	}

	if len(relayer.txnQueue) == relayer.config.BufferSize { // queue is full?
		return false
	}

	relayer.txnQueue <- signedTx
	return true
}

func (relayer *TxnRelayer) doRelay(signedTx hexutil.Bytes) {
	for _, url := range relayer.config.NodeUrls {
		nodeName := rpcutil.Url2NodeName(url)
		cfx, _, err := relayer.poolClients.LoadOrStoreFnErr(nodeName, func(k interface{}) (interface{}, error) {
			return sdk.NewClient(url, sdk.ClientOption{
				RetryCount:     relayer.config.Retry,
				RetryInterval:  relayer.config.RetryInterval,
				RequestTimeout: relayer.config.RequestTimeout,
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
		}).WithError(err).Trace("Raw transaction relayed")
	}
}
