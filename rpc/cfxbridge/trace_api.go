package cfxbridge

import (
	"context"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/openweb3/web3go"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

type TraceAPI struct {
	ethClient    *web3go.Client
	ethNetworkId uint32
}

func NewTraceAPI(ethNodeURL string) (*TraceAPI, error) {
	eth, err := web3go.NewClient(ethNodeURL)
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to connect to eth space")
	}

	util.HookEthRpcMetricsMiddleware(eth)

	ethChainId, err := eth.Eth.ChainId()
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to get chain ID from eth space")
	}
	return &TraceAPI{
		ethClient:    eth,
		ethNetworkId: uint32(*ethChainId),
	}, nil
}

func (api *TraceAPI) Block(ctx context.Context, blockHash types.Hash) (*types.LocalizedBlockTrace, error) {
	ethBlockHash := *blockHash.ToCommonHash()
	ethBlock, err := api.ethClient.Eth.BlockByHash(ethBlockHash, false)
	if err != nil {
		return nil, err
	}

	if ethBlock == nil {
		return nil, nil
	}

	bnh := ethTypes.BlockNumberOrHashWithHash(ethBlockHash, true)
	traces, err := api.ethClient.Trace.Blocks(bnh)
	if err != nil {
		return nil, err
	}

	if traces == nil {
		return nil, nil
	}

	var builder BlockTraceBuilder
	for i := range traces {
		cfxTrace, cfxTraceResult := ConvertTrace(&traces[i], api.ethNetworkId)
		builder.Append(cfxTrace, cfxTraceResult, traces[i].Subtraces)
	}

	return &types.LocalizedBlockTrace{
		TransactionTraces: builder.Build(),
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
		builder.Append(cfxTrace, cfxTraceResult, traces[i].Subtraces)
	}

	return builder.Build(), nil
}
