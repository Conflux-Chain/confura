package client

import (
	"context"
	"time"

	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-util/viper"
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

func MustNewEthClientFromViper() (*EthClient, bool) {
	var conf clientConfig
	viper.MustUnmarshalKey("ethVirtualFilters.client", &conf)

	if !conf.Enabled {
		return nil, false
	}

	option := providers.Option{
		RetryCount:     defaultClientRetryCount,
		RetryInterval:  defaultClientRetryInterval,
		RequestTimeout: defaultClientRequestTimeout,
	}

	p, err := providers.NewProviderWithOption(conf.ServiceRpcUrl, option)
	if err != nil {
		logrus.WithError(err).
			WithField("serviceRpcUrl", conf.ServiceRpcUrl).
			Fatal("Failed to create RPC provider for virtual filter client")
	}

	return &EthClient{p: p}, true
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

func (client *EthClient) GetFilterLogs(filterID rpc.ID) (val []ethtypes.Log, err error) {
	err = client.p.CallContext(context.Background(), &val, "eth_getFilterLogs", filterID)
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

func MustNewCfxClientFromViper() (*CfxClient, bool) {
	var conf clientConfig
	viper.MustUnmarshalKey("virtualFilters.client", &conf)

	if !conf.Enabled {
		return nil, false
	}

	option := providers.Option{
		RetryCount:     defaultClientRetryCount,
		RetryInterval:  defaultClientRetryInterval,
		RequestTimeout: defaultClientRequestTimeout,
	}

	p, err := providers.NewProviderWithOption(conf.ServiceRpcUrl, option)
	if err != nil {
		logrus.WithError(err).
			WithField("serviceRpcUrl", conf.ServiceRpcUrl).
			Fatal("Failed to create RPC provider for virtual filter client")
	}

	return &CfxClient{p: p}, true
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

func (client *CfxClient) GetFilterLogs(filterID rpc.ID) (val []cfxtypes.Log, err error) {
	err = client.p.CallContext(context.Background(), &val, "cfx_getFilterLogs", filterID)
	return
}

func (client *CfxClient) UninstallFilter(filterID rpc.ID) (val bool, err error) {
	err = client.p.CallContext(context.Background(), &val, "cfx_uninstallFilter", filterID)
	return
}
