package node

import (
	"time"

	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/buraksezer/consistent"
	"github.com/sirupsen/logrus"
)

var cfg config // node config from viper

func init() {
	viper.MustUnmarshalKey("node", &cfg)
	logrus.WithField("config", cfg).Debug("Node manager configurations loaded.")
}

// Config returns the configuration from viper.
func Config() config {
	return cfg
}

type config struct {
	Endpoint string `default:":22530"`
	URLs     []string
	WSURLs   []string
	HashRing struct {
		PartitionCount    int     `default:"15739"`
		ReplicationFactor int     `default:"51"`
		Load              float64 `default:"1.25"`
	}
	Monitor struct {
		Interval time.Duration `default:"1s"`
		Unhealth struct {
			Failures          uint64        `default:"3"`
			EpochsFallBehind  uint64        `default:"30"`
			LatencyPercentile float64       `default:"0.9"`
			MaxLatency        time.Duration `default:"3s"`
		}
		Recover struct {
			RemindInterval time.Duration `default:"5m"`
			SuccessCounter uint64        `default:"60"`
		}
	}
	Router struct {
		RedisURL        string
		NodeRPCURL      string
		ChainedFailover chainedFailoverConfig
	}
}

func (c *config) HashRingRaw() consistent.Config {
	return consistent.Config{
		PartitionCount:    c.HashRing.PartitionCount,
		ReplicationFactor: c.HashRing.ReplicationFactor,
		Load:              c.HashRing.Load,
		Hasher:            &hasher{},
	}
}

type chainedFailoverConfig struct {
	URL   string
	WSURL string
}
