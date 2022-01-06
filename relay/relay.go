package relay

import (
	"errors"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	txnRelayChanSize = 2000 // default transaction relay channel size
)

// TODO: use `go-defaults` for default setting value.
type txnRelayerConfig struct {
	// sdk client option
	RetryCount     int           `mapstructure:"retry"`
	RetryInterval  time.Duration `mapstructure:"retryInterval"`
	RequestTimeout time.Duration `mapstructure:"requestTimeout"`
	// full node URL lists
	NodeUrls []string `mapstructure:"nodeUrls"`
	// num of relay concurrency
	Concurrency int `mapstructure:"concurrency"`
}

// TxnRelayer relays raw transaction by broadcasting to node pool
// of different regions to accelerate P2P diffusion.
// TODO: move this to `go-conflux-util` repository
type TxnRelayer struct {
	poolClients []*sdk.Client      // sdk clients representing node pool
	txnQueue    chan hexutil.Bytes // transactions queued to relay
}

func MustNewTxnRelayerFromViper() *TxnRelayer {
	relayer, err := NewTxnRelayerFromViper()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to new transaction relayer from viper")
	}

	return relayer
}

func NewTxnRelayerFromViper() (*TxnRelayer, error) {
	var relayConf txnRelayerConfig

	subViper := util.ViperSub(viper.GetViper(), "relay")
	if err := subViper.Unmarshal(&relayConf); err != nil {
		return nil, errors.New("failed to load viper settings")
	}

	return NewTxnRelayer(&relayConf)
}

func NewTxnRelayer(relayConf *txnRelayerConfig) (*TxnRelayer, error) {
	var cfxClients []*sdk.Client

	for _, url := range relayConf.NodeUrls {
		cfx, err := sdk.NewClient(url, sdk.ClientOption{
			RetryCount:     relayConf.RetryCount,
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
		txnQueue:    make(chan hexutil.Bytes, txnRelayChanSize),
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
	if len(relayer.txnQueue) == txnRelayChanSize { // queue is full?
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
