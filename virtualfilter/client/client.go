package client

import (
	"context"
	"time"

	"github.com/Conflux-Chain/confura/util/viper"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/go-rpc-provider/interfaces"
	providers "github.com/openweb3/go-rpc-provider/provider_wrapper"
	ethtypes "github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
)

const (
	defaultClientRetryCount     = 0
	defaultClientRetryInterval  = 1 * time.Second
	defaultClientRequestTimeout = 3 * time.Second
)

type clientConfig struct {
	Enabled       bool
	ServiceRpcUrl string
}

type EthClient struct {
	// underlying rpc client provider to request virtual filter service
	p interfaces.Provider
}

func NewEthClientFromViper() (*EthClient, bool, error) {
	var conf clientConfig
	if err := viper.UnmarshalKey("ethVirtualFilters.client", &conf); err != nil {
		return nil, false, err
	}

	if !conf.Enabled {
		return nil, false, nil
	}

	option := providers.Option{
		RetryCount:     defaultClientRetryCount,
		RetryInterval:  defaultClientRetryInterval,
		RequestTimeout: defaultClientRequestTimeout,
	}

	p, err := providers.NewProviderWithOption(conf.ServiceRpcUrl, option)
	if err != nil {
		return nil, false, err
	}

	return &EthClient{p: p}, true, nil
}

func (client *EthClient) NewFilter(delFnUrl string, fq *ethtypes.FilterQuery) (val *rpc.ID, err error) {
	err = client.p.CallContext(context.Background(), &val, "eth_newFilter", delFnUrl, fq)
	return
}

func (client *EthClient) NewBlockFilter(delFnUrl string) (val *rpc.ID, err error) {
	err = client.p.CallContext(context.Background(), &val, "eth_newBlockFilter", delFnUrl)
	return
}

func (client *EthClient) NewPendingTransactionFilter(delFnUrl string) (val *rpc.ID, err error) {
	err = client.p.CallContext(context.Background(), &val, "eth_newPendingTransactionFilter", delFnUrl)
	return
}

func (client *EthClient) GetFilterChanges(filterID rpc.ID) (val *ethtypes.FilterChanges, err error) {
	err = client.p.CallContext(context.Background(), &val, "eth_getFilterChanges", filterID)
	return
}

func (client *EthClient) GetLogFilter(filterID rpc.ID) (val *ethtypes.FilterQuery, err error) {
	err = client.p.CallContext(context.Background(), &val, "eth_getLogFilter", filterID)
	return
}

func (client *EthClient) UninstallFilter(filterID rpc.ID) (val bool, err error) {
	err = client.p.CallContext(context.Background(), &val, "eth_uninstallFilter", filterID)
	return
}

type CfxClient struct {
	// underlying rpc client provider to request virtual filter service
	p interfaces.Provider
}

func NewCfxClientFromViper() (*CfxClient, bool, error) {
	var conf clientConfig
	if err := viper.UnmarshalKey("virtualFilters.client", &conf); err != nil {
		return nil, false, err
	}

	if !conf.Enabled {
		return nil, false, nil
	}

	option := providers.Option{
		RetryCount:     defaultClientRetryCount,
		RetryInterval:  defaultClientRetryInterval,
		RequestTimeout: defaultClientRequestTimeout,
	}

	p, err := providers.NewProviderWithOption(conf.ServiceRpcUrl, option)
	if err != nil {
		return nil, false, err
	}

	return &CfxClient{p: p}, true, nil
}

func (client *CfxClient) NewFilter(delFnUrl string, filterCrit *cfxtypes.LogFilter) (val *rpc.ID, err error) {
	err = client.p.CallContext(context.Background(), &val, "cfx_newFilter", delFnUrl, filterCrit)
	return
}

func (client *CfxClient) NewBlockFilter(delFnUrl string) (val *rpc.ID, err error) {
	err = client.p.CallContext(context.Background(), &val, "cfx_newBlockFilter", delFnUrl)
	return
}

func (client *CfxClient) NewPendingTransactionFilter(delFnUrl string) (val *rpc.ID, err error) {
	err = client.p.CallContext(context.Background(), &val, "cfx_newPendingTransactionFilter", delFnUrl)
	return
}

func (client *CfxClient) GetFilterChanges(filterID rpc.ID) (val *cfxtypes.CfxFilterChanges, err error) {
	err = client.p.CallContext(context.Background(), &val, "cfx_getFilterChanges", filterID)
	return
}

func (client *CfxClient) GetLogFilter(filterID rpc.ID) (val *cfxtypes.LogFilter, err error) {
	err = client.p.CallContext(context.Background(), &val, "cfx_getLogFilter", filterID)
	return
}

func (client *CfxClient) UninstallFilter(filterID rpc.ID) (val bool, err error) {
	err = client.p.CallContext(context.Background(), &val, "cfx_uninstallFilter", filterID)
	return
}

func MustNewCfxClientFromViper() (*CfxClient, bool) {
	c, ok, err := NewCfxClientFromViper()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize virtual filter client")
	}
	return c, ok
}
func (c *CfxClient) Close() { c.p.Close() }

func MustNewEthClientFromViper() (*EthClient, bool) {
	c, ok, err := NewEthClientFromViper()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize virtual filter client")
	}
	return c, ok
}
func (c *EthClient) Close() { c.p.Close() }
