package handler

import (
	"fmt"
	"strings"

	"github.com/Conflux-Chain/confura/store"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	web3types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

var (
	checkMixedSourceConsistency = true
	ErrGetLogsConsistency       = errors.New("inconsistent canonical views")
)

// LogQueryOptions preserves normalization provenance; it never changes the FN
// log range. ResolvedHead is a block number in eSpace and an epoch in Core.
// Use it only for an upper bound originally specified as latest/latest_state
// (including the default), and only while that numeric upper bound is unchanged.
type LogQueryOptions struct {
	ResolvedHead *uint64
}

func (o LogQueryOptions) matchesHead(upper uint64) bool {
	return o.ResolvedHead != nil && *o.ResolvedHead == upper
}

// Keep the cause for metrics while allowing both causes to share retry policy.
type logsConsistencyError string

func (e logsConsistencyError) Error() string {
	return fmt.Sprintf("%s: %s", ErrGetLogsConsistency, string(e))
}
func (e logsConsistencyError) Unwrap() error { return ErrGetLogsConsistency }

type logBlockIdentity struct {
	number uint64 // Core uses the epoch's pivot identity, not an arbitrary block.
	hash   string
}

// A fence spans the FN log read: DB identity must match FN at the seam, and FN
// must retain the pre-read checkpoint at a fixed height. Empty results also use
// a chain view, so inspect metadata even with no logs. Independent RPCs cannot
// exclude ABA or prove that a range above head was returned completely.
type logsConsistencyFence struct {
	boundary   logBlockIdentity
	checkpoint logBlockIdentity
	readHash   func(uint64) (string, error)
}

func (f *logsConsistencyFence) check() error {
	if f == nil {
		return nil
	}
	boundary, err := f.checkIdentity(f.boundary, "boundary")
	if err != nil {
		return err
	}
	// A bad seam already invalidates the attempt. If the heights coincide,
	// one metadata read can validate both identities.
	if f.checkpoint.number == f.boundary.number {
		if !strings.EqualFold(boundary, f.checkpoint.hash) {
			return logsConsistencyError("checkpoint")
		}
		return nil
	}
	_, err = f.checkIdentity(f.checkpoint, "checkpoint")
	return err
}

func (f *logsConsistencyFence) checkIdentity(expected logBlockIdentity, reason string) (string, error) {
	hash, err := f.readHash(expected.number)
	if err != nil {
		if errors.Is(err, ErrGetLogsConsistency) {
			return "", logsConsistencyError(reason)
		}
		return "", err
	}
	if hash == "" || !strings.EqualFold(hash, expected.hash) {
		return "", logsConsistencyError(reason)
	}
	return hash, nil
}

// newLogsFence reads the DB seam, then fixes an FN checkpoint for this attempt.
func (h *EthLogsApiHandler) newLogsFence(eth ethLogsClient, dbFilter *store.LogFilter, fnFilter *web3types.FilterQuery, options LogQueryOptions) (*logsConsistencyFence, error) {
	if !checkMixedSourceConsistency || dbFilter == nil || fnFilter == nil {
		return nil, nil
	}
	pivot, ok, err := h.es.PivotHash(dbFilter.BlockTo)
	if err != nil {
		return nil, err
	}
	if !ok || pivot == "" {
		return nil, logsConsistencyError("boundary")
	}
	checkpoint, err := ethLogCheckpoint(eth, fnFilter, options)
	if err != nil {
		return nil, err
	}
	return &logsConsistencyFence{
		boundary: logBlockIdentity{dbFilter.BlockTo, pivot}, checkpoint: checkpoint,
		readHash: func(number uint64) (string, error) {
			block, err := ethLogBlock(eth, web3types.BlockNumber(number))
			return block.hash, err
		},
	}, nil
}

func ethLogCheckpoint(eth ethLogsClient, filter *web3types.FilterQuery, options LogQueryOptions) (logBlockIdentity, error) {
	if filter.ToBlock != nil && *filter.ToBlock >= 0 && options.matchesHead(uint64(*filter.ToBlock)) {
		return ethLogBlock(eth, *filter.ToBlock)
	}
	latest, err := ethLogBlock(eth, web3types.LatestBlockNumber)
	if err != nil {
		return logBlockIdentity{}, err
	}
	if filter.ToBlock != nil && *filter.ToBlock >= 0 && uint64(*filter.ToBlock) < latest.number {
		return ethLogBlock(eth, *filter.ToBlock)
	}
	return latest, nil
}

func ethLogBlock(eth ethLogsClient, number web3types.BlockNumber) (logBlockIdentity, error) {
	block, err := eth.BlockByNumber(number, false)
	if err != nil {
		return logBlockIdentity{}, err
	}
	if block == nil || block.Number == nil || !block.Number.IsUint64() || block.Hash == (common.Hash{}) ||
		number >= 0 && block.Number.Uint64() != uint64(number) {
		return logBlockIdentity{}, logsConsistencyError("checkpoint")
	}
	return logBlockIdentity{block.Number.Uint64(), block.Hash.Hex()}, nil
}

func (h *CfxLogsApiHandler) newLogsFence(cfx cfxScanClient, dbFilter *store.LogFilter, fnFilter *cfxtypes.LogFilter, options LogQueryOptions) (*logsConsistencyFence, error) {
	if !checkMixedSourceConsistency || dbFilter == nil || fnFilter == nil {
		return nil, nil
	}
	boundary, err := h.cfxLogBoundary(dbFilter.BlockTo)
	if err != nil {
		return nil, err
	}
	checkpoint, err := cfxLogCheckpoint(cfx, fnFilter, options)
	if err != nil {
		return nil, err
	}
	return &logsConsistencyFence{
		boundary: boundary, checkpoint: checkpoint,
		readHash: func(epoch uint64) (string, error) {
			block, err := cfxLogBlock(cfx, cfxtypes.NewEpochNumberUint64(epoch))
			if err != nil {
				return "", err
			}
			return block.Hash.String(), nil
		},
	}, nil
}

func (h *CfxLogsApiHandler) cfxLogBoundary(blockTo uint64) (logBlockIdentity, error) {
	mapping, ok, err := h.ms.LatestBlockMapping()
	if err != nil {
		return logBlockIdentity{}, err
	}
	if !ok {
		return logBlockIdentity{}, logsConsistencyError("boundary")
	}
	epoch, pivot := mapping.Epoch, mapping.PivotHash
	if mapping.BnMax != blockTo {
		epoch, ok, err = h.ms.LatestEpochBeforeBlock(mapping.Epoch, blockTo)
		if err != nil {
			return logBlockIdentity{}, err
		}
		if !ok {
			return logBlockIdentity{}, logsConsistencyError("boundary")
		}
		pivot, ok, err = h.ms.PivotHash(epoch)
		if err != nil {
			return logBlockIdentity{}, err
		}
		if !ok {
			return logBlockIdentity{}, logsConsistencyError("boundary")
		}
	}
	if pivot == "" {
		return logBlockIdentity{}, logsConsistencyError("boundary")
	}
	return logBlockIdentity{epoch, pivot}, nil
}

func cfxLogCheckpoint(cfx cfxScanClient, filter *cfxtypes.LogFilter, options LogQueryOptions) (logBlockIdentity, error) {
	if filter.ToEpoch != nil {
		if upper, numeric := filter.ToEpoch.ToInt(); numeric && upper.IsUint64() && options.matchesHead(upper.Uint64()) {
			block, err := cfxLogBlock(cfx, filter.ToEpoch)
			if err != nil {
				return logBlockIdentity{}, err
			}
			return logBlockIdentity{upper.Uint64(), block.Hash.String()}, nil
		}
	}
	latest, err := cfxLogBlock(cfx, cfxtypes.EpochLatestState)
	if err != nil {
		return logBlockIdentity{}, err
	}
	checkpoint := latest.EpochNumber.ToInt().Uint64()
	if filter.ToEpoch != nil {
		if upper, numeric := filter.ToEpoch.ToInt(); numeric && upper.IsUint64() {
			checkpoint = min(checkpoint, upper.Uint64())
		}
	} else if filter.ToBlock != nil && filter.ToBlock.ToInt().Uint64() < latest.BlockNumber.ToInt().Uint64() {
		block, err := cfx.GetBlockSummaryByBlockNumber(hexutil.Uint64(filter.ToBlock.ToInt().Uint64()))
		if err != nil {
			return logBlockIdentity{}, err
		}
		if block == nil || block.EpochNumber == nil || !block.EpochNumber.ToInt().IsUint64() {
			return logBlockIdentity{}, logsConsistencyError("checkpoint")
		}
		checkpoint = block.EpochNumber.ToInt().Uint64()
	}
	if checkpoint == latest.EpochNumber.ToInt().Uint64() {
		return logBlockIdentity{checkpoint, latest.Hash.String()}, nil
	}
	block, err := cfxLogBlock(cfx, cfxtypes.NewEpochNumberUint64(checkpoint))
	if err != nil {
		return logBlockIdentity{}, err
	}
	return logBlockIdentity{checkpoint, block.Hash.String()}, nil
}

func cfxLogBlock(cfx cfxScanClient, epoch *cfxtypes.Epoch) (*cfxtypes.BlockSummary, error) {
	block, err := cfx.GetBlockSummaryByEpoch(epoch)
	if err != nil {
		return nil, err
	}
	if block == nil || block.EpochNumber == nil || !block.EpochNumber.ToInt().IsUint64() ||
		block.BlockNumber == nil || !block.BlockNumber.ToInt().IsUint64() || block.Hash == "" {
		return nil, logsConsistencyError("checkpoint")
	}
	if number, numeric := epoch.ToInt(); numeric && number.Cmp(block.EpochNumber.ToInt()) != 0 {
		return nil, logsConsistencyError("checkpoint")
	}
	return block, nil
}
