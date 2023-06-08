package cfxbridge

import (
	"context"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/openweb3/web3go"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
)

type TraceAPI struct {
	ethClient    *web3go.Client
	ethNetworkId uint32
}

func NewTraceAPI(ethClient *web3go.Client, ethNetworkId uint32) *TraceAPI {
	return &TraceAPI{
		ethClient:    ethClient,
		ethNetworkId: ethNetworkId,
	}
}

func (api *TraceAPI) Block(ctx context.Context, blockHash types.Hash) (*types.LocalizedBlockTrace, error) {
	ethBlockHash := *blockHash.ToCommonHash()
	ethBlock, err := api.ethClient.Eth.BlockByHash(ethBlockHash, false)
	if err != nil {
		logrus.WithField("ethBlockHash", ethBlockHash).WithError(err).Info("api.ethClient.Eth.BlockByHash")
		return nil, err
	}

	if ethBlock == nil {
		return nil, nil
	}

	bnh := ethTypes.BlockNumberOrHashWithHash(ethBlockHash, true)
	traces, err := api.ethClient.Trace.Blocks(bnh)
	if err != nil {
		logrus.WithField("bnh", bnh).WithError(err).Info("api.ethClient.Trace.Blocks")
		return nil, err
	}

	if traces == nil {
		return nil, nil
	}

	var builder BlockTraceBuilder
	for i := range traces {
		cfxTrace, cfxTraceResult := ConvertTrace(&traces[i], api.ethNetworkId)
		if err := builder.Append(cfxTrace, cfxTraceResult, traces[i].Subtraces); err != nil {
			return nil, err
		}
	}

	txnTraces, err := builder.Build()
	if err != nil {
		return nil, err
	}

	return &types.LocalizedBlockTrace{
		TransactionTraces: txnTraces,
		EpochHash:         blockHash,
		EpochNumber:       *types.NewBigIntByRaw(ethBlock.Number),
		BlockHash:         blockHash,
	}, nil
}

func (api *TraceAPI) Filter(ctx context.Context, filter types.TraceFilter) ([]types.LocalizedTrace, error) {
	// not supported yet
	return emptyTraces, nil
}

func (api *TraceAPI) Transaction(ctx context.Context, txHash types.Hash) ([]types.LocalizedTrace, error) {
	traces, err := api.ethClient.Trace.Transactions(*txHash.ToCommonHash())
	if err != nil {
		return nil, err
	}

	if traces == nil {
		return nil, nil
	}

	var builder TraceBuilder
	for i := range traces {
		cfxTrace, cfxTraceResult := ConvertTrace(&traces[i], api.ethNetworkId)
		if err := builder.Append(cfxTrace, cfxTraceResult, traces[i].Subtraces); err != nil {
			return nil, err
		}
	}

	return builder.Build()
}
