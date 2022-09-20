package relay

import (
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/scroll-tech/rpc-gateway/util"
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
	poolClients []*sdk.Client      // fullnode pool
	txnQueue    chan hexutil.Bytes // transactions queued to relay
	config      *TxnRelayerConfig
}

func MustNewTxnRelayerFromViper() *TxnRelayer {
	relayer, err := NewTxnRelayerFromViper()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to new transaction relayer from viper")
	}

	return relayer
}

func NewTxnRelayerFromViper() (*TxnRelayer, error) {
	var relayConf TxnRelayerConfig
	viper.MustUnmarshalKey("relay", &relayConf)
	return NewTxnRelayer(&relayConf)
}

func NewTxnRelayer(relayConf *TxnRelayerConfig) (*TxnRelayer, error) {
	if len(relayConf.NodeUrls) == 0 {
		return &TxnRelayer{}, nil
	}

	var cfxClients []*sdk.Client

	for _, url := range relayConf.NodeUrls {
		cfx, err := sdk.NewClient(url, sdk.ClientOption{
			RetryCount:     relayConf.Retry,
			RetryInterval:  relayConf.RetryInterval,
			RequestTimeout: relayConf.RequestTimeout,
		})
		if err != nil {
			return nil, err
		}

		cfxClients = append(cfxClients, cfx)
	}

	relayer := &TxnRelayer{
		poolClients: cfxClients,
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

	return relayer, nil
}

// AsyncRelay relays raw transaction broadcasting asynchronously.
func (relayer *TxnRelayer) AsyncRelay(signedTx hexutil.Bytes) bool {
	if len(relayer.poolClients) == 0 {
		return true
	}

	if len(relayer.txnQueue) == relayer.config.BufferSize { // queue is full?
		return false
	}

	relayer.txnQueue <- signedTx
	return true
}

func (relayer *TxnRelayer) doRelay(signedTx hexutil.Bytes) {
	for _, client := range relayer.poolClients {
		txHash, err := client.SendRawTransaction(signedTx)

		logrus.WithFields(logrus.Fields{
			"nodeUrl": client.GetNodeURL(), "txHash": txHash,
		}).WithError(err).Trace("Raw transaction relayed")
	}
}
