package client

import (
	"context"
	"time"

	w3rpc "github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/go-rpc-provider/interfaces"
	providers "github.com/openweb3/go-rpc-provider/provider_wrapper"
	"github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	defaultClientRetryCount     = 0
	defaultClientRetryInterval  = 1 * time.Second
	defaultClientRequestTimeout = 3 * time.Second
)

type EthClient struct {
	// underlying rpc client provider to request virtual filter service
	p interfaces.Provider
}

func MustNewEthClientFromViper() *EthClient {
	svcRpcUrl := viper.GetString("ethVirtualFilters.serviceRpcUrl")
	option := providers.Option{
		RetryCount:     defaultClientRetryCount,
		RetryInterval:  defaultClientRetryInterval,
		RequestTimeout: defaultClientRequestTimeout,
	}

	p, err := providers.NewProviderWithOption(svcRpcUrl, option)
	if err != nil {
		logrus.WithError(err).
			WithField("serviceRpcUrl", svcRpcUrl).
			Fatal("Failed to create RPC provider for virtual filter client")
	}

	return &EthClient{p: p}
}

func (client *EthClient) NewFilter(delFnUrl string, fq *types.FilterQuery) (val *w3rpc.ID, err error) {
	err = client.p.CallContext(context.Background(), &val, "eth_newFilter", delFnUrl, fq)
	return
}

func (client *EthClient) NewBlockFilter(delFnUrl string) (val *w3rpc.ID, err error) {
	err = client.p.CallContext(context.Background(), &val, "eth_newBlockFilter", delFnUrl)
	return
}

func (client *EthClient) NewPendingTransactionFilter(delFnUrl string) (val *w3rpc.ID, err error) {
	err = client.p.CallContext(context.Background(), &val, "eth_newPendingTransactionFilter", delFnUrl)
	return
}

func (client *EthClient) GetFilterChanges(filterID w3rpc.ID) (val *types.FilterChanges, err error) {
	err = client.p.CallContext(context.Background(), &val, "eth_getFilterChanges", filterID)
	return
}

func (client *EthClient) GetFilterLogs(filterID w3rpc.ID) (val []types.Log, err error) {
	err = client.p.CallContext(context.Background(), &val, "eth_getFilterLogs", filterID)
	return
}

func (client *EthClient) UninstallFilter(filterID w3rpc.ID) (val bool, err error) {
	err = client.p.CallContext(context.Background(), &val, "eth_uninstallFilter", filterID)
	return
}
