package node

import (
	"time"

	"github.com/buraksezer/consistent"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var cfg config // node config from viper

func init() {
	nodeUrlsGetter := &util.ViperKeyGetter{
		Key: "node.urls",
		Getter: func(key string) interface{} {
			return viper.GetViper().GetStringSlice(key)
		},
	}
	if err := util.ViperSub(viper.GetViper(), "node", nodeUrlsGetter).Unmarshal(&cfg); err != nil {
		logrus.WithError(err).Fatal("Failed to unmarshal node config from viper")
	}

	logrus.WithField("config", cfg).Info("Node manager configurations loaded.")

	cfg.HashRing.Hasher = &hasher{}
}

type config struct {
	Endpoint string
	URLs     []string
	HashRing consistent.Config
	Monitor  monitorConfig
	Router   routerConfig
}

type monitorConfig struct {
	Interval time.Duration
	Unhealth unhealthConfig
	Recover  recoverConfig
}

type unhealthConfig struct {
	Failures          uint64
	EpochsFallBehind  uint64
	LatencyPercentile float64
	MaxLatency        time.Duration
}

type recoverConfig struct {
	RemindInterval time.Duration
	SuccessCounter uint64
}

type routerConfig struct {
	RedisURL   string
	NodeRPCURL string
}
