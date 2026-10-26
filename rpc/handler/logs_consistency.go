package handler

import (
	"fmt"
	"strings"

	"github.com/Conflux-Chain/confura/store"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	web3types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

var (
	checkMixedSourceConsistency = true
	ErrGetLogsConsistency       = errors.New("inconsistent canonical views")
)

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

// A fence compares FN against the same DB boundary before and after the log
// read, including empty results. Reorgs above that boundary do not invalidate
// the merge. Independent RPCs cannot exclude ABA or prove range completeness.
type logsConsistencyFence struct {
	boundary logBlockIdentity
	readHash func(uint64) (string, error)
}

func (f *logsConsistencyFence) check() error {
	if f == nil {
		return nil
	}
	hash, err := f.readHash(f.boundary.number)
	if err != nil {
		return err
	}
	if hash == "" || !strings.EqualFold(hash, f.boundary.hash) {
		return logsConsistencyError("boundary")
	}
	return nil
}

// newLogsFence captures the DB boundary; the caller checks it around the FN read.
func (h *EthLogsApiHandler) newLogsFence(eth ethLogsClient, dbFilter *store.LogFilter) (*logsConsistencyFence, error) {
	if !checkMixedSourceConsistency || dbFilter == nil {
		return nil, nil
	}
	pivot, ok, err := h.es.PivotHash(dbFilter.BlockTo)
	if err != nil {
		return nil, err
	}
	if !ok || pivot == "" {
		return nil, logsConsistencyError("boundary")
	}
	return &logsConsistencyFence{
		boundary: logBlockIdentity{dbFilter.BlockTo, pivot},
		readHash: func(number uint64) (string, error) {
			return ethLogBoundaryHash(eth, number)
		},
	}, nil
}

func ethLogBoundaryHash(eth ethLogsClient, number uint64) (string, error) {
	block, err := eth.BlockByNumber(web3types.BlockNumber(number), false)
	if err != nil {
		return "", err
	}
	if block == nil || block.Number == nil || !block.Number.IsUint64() ||
		block.Number.Uint64() != number || block.Hash == (common.Hash{}) {
		return "", logsConsistencyError("boundary")
	}
	return block.Hash.Hex(), nil
}

func (h *CfxLogsApiHandler) newLogsFence(cfx cfxScanClient, dbFilter *store.LogFilter) (*logsConsistencyFence, error) {
	if !checkMixedSourceConsistency || dbFilter == nil {
		return nil, nil
	}
	boundary, err := h.cfxLogBoundary(dbFilter.BlockTo)
	if err != nil {
		return nil, err
	}
	return &logsConsistencyFence{
		boundary: boundary,
		readHash: func(epoch uint64) (string, error) {
			return cfxLogBoundaryHash(cfx, epoch)
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

func cfxLogBoundaryHash(cfx cfxScanClient, epoch uint64) (string, error) {
	block, err := cfx.GetBlockSummaryByEpoch(cfxtypes.NewEpochNumberUint64(epoch))
	if err != nil {
		return "", err
	}
	if block == nil || block.EpochNumber == nil || !block.EpochNumber.ToInt().IsUint64() ||
		block.EpochNumber.ToInt().Uint64() != epoch || block.Hash == "" {
		return "", logsConsistencyError("boundary")
	}
	return block.Hash.String(), nil
}
