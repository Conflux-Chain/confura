package relay

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura/util"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	"github.com/Conflux-Chain/confura/util/viper"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
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
	r, err := NewTxnRelayerFromViper(false)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize transaction relay")
	}
	return r
}

func MustNewEthTxnRelayerFromViper() TxnRelayer {
	r, err := NewTxnRelayerFromViper(true)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize transaction relay")
	}
	return r
}

// ManagedTxnRelayer owns its workers and clients.
type ManagedTxnRelayer interface {
	TxnRelayer
	Close()
}

func NewTxnRelayerFromViper(eth bool) (ManagedTxnRelayer, error) {
	var relayConf TxnRelayerConfig
	if err := viper.UnmarshalKey("relay", &relayConf); err != nil {
		return nil, err
	}
	if relayConf.BufferSize < 0 {
		return nil, fmt.Errorf("relay buffer size must not be negative")
	}
	if eth {
		return newEthTxnRelayer(&relayConf), nil
	}
	return newCfxTxnRelayer(&relayConf), nil
}

// txnRelayer base transaction relayer
type txnRelayer struct {
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
	*TxnRelayerConfig

	poolClients *util.ConcurrentMap // fullnode client pool
	txnQueue    chan hexutil.Bytes  // transactions queued to relay
}

func newTxnRelayer(relayConf *TxnRelayerConfig) *txnRelayer {
	ctx, cancel := context.WithCancel(context.Background())
	return &txnRelayer{ctx: ctx, cancel: cancel,
		TxnRelayerConfig: relayConf,
		poolClients:      &util.ConcurrentMap{},
		txnQueue:         make(chan hexutil.Bytes, relayConf.BufferSize),
	}
}

// Relay relays raw transaction broadcasting asynchronously.
func (relayer *txnRelayer) Relay(signedTx hexutil.Bytes) bool {
	if relayer.ctx.Err() != nil {
		return false
	}
	// Avoid copying a transaction while a buffered queue is already full.
	// This is only an early rejection: select still decides admission atomically.
	if capacity := cap(relayer.txnQueue); capacity > 0 && len(relayer.txnQueue) == capacity {
		return false
	}
	// Own queued bytes; callers may reuse their request buffer after return.
	raw := append(hexutil.Bytes(nil), signedTx...)
	select {
	case <-relayer.ctx.Done():
		return false
	case relayer.txnQueue <- raw:
		return true
	default:
		return false
	}
}

// run starts concurrency worker(s) asynchronously
func (relayer *txnRelayer) run(process func()) {
	concurrency := max(relayer.Concurrency, 1)
	for i := 0; i < concurrency; i++ {
		relayer.wg.Add(1)
		go func() { defer relayer.wg.Done(); process() }()
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
	for {
		select {
		case <-relayer.ctx.Done():
			return
		case rawTxn := <-relayer.txnQueue:
			relayer.relay(rawTxn)
		}
	}
}

func (relayer *cfxTxnRelayer) relay(signedTx hexutil.Bytes) {
	for _, url := range relayer.NodeUrls {
		nodeName := rpcutil.Url2NodeName(url)
		cfx, _, err := relayer.poolClients.LoadOrStoreFnErr(nodeName, func(k interface{}) (interface{}, error) {
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

		txHash, err := rpcutil.CfxWithContext(relayer.ctx, cfx.(sdk.ClientOperator)).SendRawTransaction(signedTx)
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
	for {
		select {
		case <-relayer.ctx.Done():
			return
		case rawTxn := <-relayer.txnQueue:
			relayer.relay(rawTxn)
		}
	}
}

func (relayer *ethTxnRelayer) relay(signedTx hexutil.Bytes) {
	for _, url := range relayer.EthNodeUrls {
		nodeName := rpcutil.Url2NodeName(url)
		w3c, _, err := relayer.poolClients.LoadOrStoreFnErr(nodeName, func(k interface{}) (interface{}, error) {
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

		txHash, err := w3c.(*web3go.Client).WithContext(relayer.ctx).Eth.SendRawTransaction(signedTx)
		logrus.WithFields(logrus.Fields{
			"url":    url,
			"txHash": txHash,
		}).WithError(err).Trace("Evm space raw transaction relayed")
	}
}

func (relayer *txnRelayer) Close() {
	relayer.closeOnce.Do(func() {
		relayer.cancel()
		relayer.wg.Wait()
		relayer.poolClients.Range(func(_, value interface{}) bool {
			if c, ok := value.(interface{ Close() }); ok {
				c.Close()
			}
			return true
		})
	})
}
