package node

import (
	"time"

	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/buraksezer/consistent"
	"github.com/sirupsen/logrus"
)

// Node manager component always uses configuration from viper.

var cfg config
var urlCfg map[Group]UrlConfig
var ethUrlCfg map[Group]UrlConfig

func init() {
	viper.MustUnmarshalKey("node", &cfg)
	logrus.WithField("config", cfg).Debug("Node manager configurations loaded.")

	urlCfg = map[Group]UrlConfig{
		GroupCfxHttp: {
			Nodes:    cfg.URLs,
			Failover: cfg.Router.ChainedFailover.URL,
		},
		GroupCfxWs: {
			Nodes:    cfg.WSURLs,
			Failover: cfg.Router.ChainedFailover.WSURL,
		},
		GroupCfxArchives: {
			Nodes: cfg.ArchiveNodes,
		},
		GroupCfxLogs: {
			Nodes: cfg.LogNodes,
		},
	}

	ethUrlCfg = map[Group]UrlConfig{
		GroupEthHttp: {
			Nodes:    cfg.EthURLs,
			Failover: cfg.Router.ChainedFailover.EthURL,
		},
		GroupEthLogs: {
			Nodes: cfg.EthLogNodes,
		},
	}
}

type config struct {
	Endpoint     string `default:":22530"`
	EthEndpoint  string `default:":28530"`
	URLs         []string
	EthURLs      []string
	WSURLs       []string
	LogNodes     []string
	EthLogNodes  []string
	ArchiveNodes []string
	HashRing     struct {
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
		EthNodeRPCURL   string
		ChainedFailover struct {
			URL    string
			WSURL  string
			EthURL string
		}
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

type UrlConfig struct {
	Nodes    []string
	Failover string
}
