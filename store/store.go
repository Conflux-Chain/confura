package store

import (
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/sirupsen/logrus"
)

var (
	cfxStoreConfig persistConfig
	ethStoreConfig persistConfig
)

type persistType string

const (
	PersistTypeBlock       persistType = "block"
	PersistTypeTransaction persistType = "transaction"
	PersistTypeReceipt     persistType = "receipt"
	PersistTypeLog         persistType = "log"
)

// All supported persist types
var AllPersistTypes = []persistType{
	PersistTypeBlock,
	PersistTypeTransaction,
	PersistTypeReceipt,
	PersistTypeLog,
}

func StoreConfig() *persistConfig {
	return &cfxStoreConfig
}

func EthStoreConfig() *persistConfig {
	return &ethStoreConfig
}

type ChainDataFilter interface {
	IsBlockDisabled() bool
	IsTxnDisabled() bool
	IsReceiptDisabled() bool
	IsLogDisabled() bool
}

type persistConfig struct {
	Types       []persistType `default:"[log]"`
	disabledSet map[persistType]bool
}

func (c *persistConfig) mustInit(viperRoot string) {
	viper.MustUnmarshalKey(viperRoot, c)

	disabled := make(map[persistType]bool, len(AllPersistTypes))
	for _, t := range AllPersistTypes {
		disabled[t] = true
	}

	for _, t := range c.Types {
		if _, ok := disabled[t]; !ok {
			logrus.WithField("dataType", t).
				Fatal("Failed to init store config due to invalid persistence data type")
		}
		disabled[t] = false
	}

	c.disabledSet = disabled
}

func (c *persistConfig) IsBlockDisabled() bool   { return c.disabledSet[PersistTypeBlock] }
func (c *persistConfig) IsTxnDisabled() bool     { return c.disabledSet[PersistTypeTransaction] }
func (c *persistConfig) IsReceiptDisabled() bool { return c.disabledSet[PersistTypeReceipt] }
func (c *persistConfig) IsLogDisabled() bool     { return c.disabledSet[PersistTypeLog] }

func MustInit() {
	cfxStoreConfig.mustInit("store.persistence")
	ethStoreConfig.mustInit("ethstore.persistence")

	initLogFilter()
	initEth()
}
