package cfxbridge

import (
	"context"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
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

	// TODO use block hash to query when eSpace supports blockNumberOrHash
	bnh := ethTypes.BlockNumberOrHashWithNumber(ethTypes.BlockNumber(ethBlock.Number.Int64()))
	traces, err := api.ethClient.Trace.Blocks(bnh)
	if err != nil {
		return nil, err
	}

	if traces == nil {
		return nil, nil
	}

	cfxTxTraces := []types.LocalizedTransactionTrace{}
	var lastTxTrace types.LocalizedTransactionTrace
	var lastTxHash types.Hash

	for i := range traces {
		cfxTrace, cfxTraceResult := ConvertTrace(&traces[i], api.ethNetworkId)
		if cfxTrace == nil {
			continue
		}

		if txHash := *cfxTrace.TransactionHash; txHash != lastTxHash {
			if len(lastTxHash) > 0 {
				cfxTxTraces = append(cfxTxTraces, lastTxTrace)
			}

			lastTxHash = txHash

			var txPos hexutil.Uint64
			if cfxTrace.TransactionPosition != nil {
				txPos = *cfxTrace.TransactionPosition
			}

			lastTxTrace = types.LocalizedTransactionTrace{
				Traces:              []types.LocalizedTrace{},
				TransactionPosition: txPos,
				TransactionHash:     txHash,
			}
		}

		lastTxTrace.Traces = append(lastTxTrace.Traces, *cfxTrace)

		// TODO use `ethTrace.subtraces` to construct traces in stack manner.
		if cfxTraceResult != nil {
			lastTxTrace.Traces = append(lastTxTrace.Traces, *cfxTraceResult)
		}
	}

	if len(lastTxHash) > 0 {
		cfxTxTraces = append(cfxTxTraces, lastTxTrace)
	}

	return &types.LocalizedBlockTrace{
		TransactionTraces: cfxTxTraces,
		EpochHash:         blockHash,
		EpochNumber:       *types.NewBigIntByRaw(ethBlock.Number),
		BlockHash:         blockHash,
	}, nil
}

func (api *TraceAPI) Filter(ctx context.Context, filter types.TraceFilter) ([]types.LocalizedTrace, error) {
	// not supported yet
	return []types.LocalizedTrace{}, nil
}

func (api *TraceAPI) Transaction(ctx context.Context, txHash types.Hash) ([]types.LocalizedTrace, error) {
	traces, err := api.ethClient.Trace.Transactions(*txHash.ToCommonHash())
	if err != nil {
		return nil, err
	}

	if traces == nil {
		return nil, nil
	}

	cfxTraces := []types.LocalizedTrace{}
	for i := range traces {
		cfxTrace, cfxTraceResult := ConvertTrace(&traces[i], api.ethNetworkId)

		if cfxTrace != nil {
			// TODO remove when eSpace always return tx hash
			if cfxTrace.TransactionHash == nil {
				cfxTrace.TransactionHash = &txHash
			}

			cfxTraces = append(cfxTraces, *cfxTrace)
		}

		// TODO use `ethTrace.subtraces` to construct traces in stack manner.
		if cfxTraceResult != nil {
			// TODO remove when eSpace always return tx hash
			if cfxTraceResult.TransactionHash == nil {
				cfxTraceResult.TransactionHash = &txHash
			}

			cfxTraces = append(cfxTraces, *cfxTraceResult)
		}
	}

	return cfxTraces, nil
}
