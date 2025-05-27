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

func MustInit() {
	viper.MustUnmarshalKey("node", &cfg)
	logrus.WithField("config", cfg).Debug("Node manager configurations loaded.")

	urlCfg = map[Group]UrlConfig{
		GroupCfxHttp: {
			Nodes:    cfg.URLs,
			Failover: cfg.Router.ChainedFailover.URL,
		},
		GroupCfxFullState: {
			Nodes: cfg.FullStateURLs,
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
		GroupCfxFilter: {
			Nodes: cfg.FilterNodes,
		},
	}

	ethUrlCfg = map[Group]UrlConfig{
		GroupEthHttp: {
			Nodes:    cfg.EthURLs,
			Failover: cfg.Router.ChainedFailover.EthURL,
		},
		GroupEthFullState: {
			Nodes: cfg.EthFullStateURLs,
		},
		GroupEthWs: {
			Nodes:    cfg.EthWSURLs,
			Failover: cfg.Router.ChainedFailover.EthWSURL,
		},
		GroupEthLogs: {
			Nodes: cfg.EthLogNodes,
		},
		GroupEthFilter: {
			Nodes: cfg.EthFilterNodes,
		},
	}
}

type config struct {
	Endpoint         string `default:":22530"`
	EndpointProto    string `default:":22531"`
	EthEndpoint      string `default:":28530"`
	EthEndpointProto string `default:":28531"`
	URLs             []string
	EthURLs          []string
	FullStateURLs    []string
	EthFullStateURLs []string
	WSURLs           []string
	EthWSURLs        []string
	LogNodes         []string
	EthLogNodes      []string
	FilterNodes      []string
	EthFilterNodes   []string
	ArchiveNodes     []string
	HashRing         struct {
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
		RedisURL           string
		NodeRPCURL         string
		NodeRpcUrlProto    string
		EthNodeRPCURL      string
		EthNodeRpcUrlProto string
		ChainedFailover    struct {
			URL      string
			WSURL    string
			EthURL   string
			EthWSURL string
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

func Config() *config {
	return &cfg
}

type UrlConfig struct {
	Nodes    []string
	Failover string
}

func CfxUrlConfig() map[Group]UrlConfig {
	return urlCfg
}

func EthUrlConfig() map[Group]UrlConfig {
	return ethUrlCfg
}
