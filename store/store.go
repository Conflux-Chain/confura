package store

import (
	"strings"

	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/sirupsen/logrus"
)

var (
	cfxStoreConfig storeConfig
	ethStoreConfig storeConfig
)

const (
	IndexTypeBlock       = "block"
	IndexTypeTransaction = "transaction"
	IndexTypeReceipt     = "receipt"
	IndexTypeLog         = "log"
)

// All supported index types
var AllIndexTypes = []string{
	IndexTypeBlock,
	IndexTypeTransaction,
	IndexTypeReceipt,
	IndexTypeLog,
}

func StoreConfig() *storeConfig {
	return &cfxStoreConfig
}

func EthStoreConfig() *storeConfig {
	return &ethStoreConfig
}

type ChainDataFilter interface {
	IsBlockDisabled() bool
	IsTxnDisabled() bool
	IsReceiptDisabled() bool
	IsLogDisabled() bool
}

type storeConfig struct {
	// Disabled chain data types: block, transaction, receipt, log
	DisabledTypes []string `default:"[block,transaction,receipt]"`
	disabledSet   map[string]bool
}

func (c *storeConfig) mustInit(viperRoot string) {
	viper.MustUnmarshalKey(viperRoot, c)

	disabled := make(map[string]bool, len(AllIndexTypes))
	for _, t := range AllIndexTypes {
		disabled[t] = false
	}

	for _, t := range c.DisabledTypes {
		key := strings.ToLower(t)
		if _, ok := disabled[key]; !ok {
			logrus.WithField("dataType", t).
				Fatal("Failed to init store config due to invalid disabled data type")
		}
		disabled[key] = true
	}

	c.disabledSet = disabled
}

func (c *storeConfig) IsBlockDisabled() bool   { return c.disabledSet[IndexTypeBlock] }
func (c *storeConfig) IsTxnDisabled() bool     { return c.disabledSet[IndexTypeTransaction] }
func (c *storeConfig) IsReceiptDisabled() bool { return c.disabledSet[IndexTypeReceipt] }
func (c *storeConfig) IsLogDisabled() bool     { return c.disabledSet[IndexTypeLog] }

func MustInit() {
	cfxStoreConfig.mustInit("store")
	ethStoreConfig.mustInit("ethstore")

	initLogFilter()
	initEth()
}
