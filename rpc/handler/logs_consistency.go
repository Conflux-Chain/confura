package handler

import (
	"strings"

	"github.com/Conflux-Chain/confura/store"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	web3types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

var (
	checkMixedSourceConsistency = true
	ErrGetLogsConsistency       = errors.New("inconsistent canonical views")
)

// A fence detects lasting canonical changes; separate RPCs cannot exclude ABA.
// Empty log results consume a chain view too, so validation uses block metadata.
type logsConsistencyFence struct {
	dbBoundary string
	before     string
	boundary   func() (string, error)
	checkpoint func() (string, error)
}

func (f *logsConsistencyFence) check() error {
	if f == nil {
		return nil
	}
	boundary, err := f.boundary()
	if err != nil {
		return err
	}
	after, err := f.checkpoint()
	if err != nil {
		return err
	}
	if f.before == "" || after == "" || f.dbBoundary == "" || !strings.EqualFold(boundary, f.dbBoundary) || !strings.EqualFold(after, f.before) {
		return ErrGetLogsConsistency
	}
	return nil
}

func (h *EthLogsApiHandler) newLogsFence(eth ethLogsClient, dbFilter *store.LogFilter, fnFilter *web3types.FilterQuery) (*logsConsistencyFence, error) {
	if !checkMixedSourceConsistency || dbFilter == nil || fnFilter == nil {
		return nil, nil
	}
	pivot, ok, err := h.es.PivotHash(dbFilter.BlockTo)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrGetLogsConsistency
	}
	latest, err := eth.BlockByNumber(web3types.LatestBlockNumber, false)
	if err != nil {
		return nil, err
	}
	if latest == nil || latest.Number == nil || !latest.Number.IsUint64() {
		return nil, ErrGetLogsConsistency
	}
	checkpoint := latest.Number.Uint64()
	before := latest
	if fnFilter.ToBlock != nil && *fnFilter.ToBlock >= 0 && uint64(*fnFilter.ToBlock) < checkpoint {
		checkpoint = uint64(*fnFilter.ToBlock)
		before, err = eth.BlockByNumber(web3types.BlockNumber(checkpoint), false)
		if err != nil {
			return nil, err
		}
		if before == nil {
			return nil, ErrGetLogsConsistency
		}
	}
	readHash := func(number uint64) (string, error) {
		block, err := eth.BlockByNumber(web3types.BlockNumber(number), false)
		if err != nil {
			return "", err
		}
		if block == nil {
			return "", nil
		}
		return block.Hash.Hex(), nil
	}
	return &logsConsistencyFence{
		dbBoundary: pivot,
		before:     before.Hash.Hex(),
		boundary:   func() (string, error) { return readHash(dbFilter.BlockTo) },
		checkpoint: func() (string, error) { return readHash(checkpoint) },
	}, nil
}

func (h *CfxLogsApiHandler) newLogsFence(cfx cfxScanClient, dbFilter *store.LogFilter, fnFilter *cfxtypes.LogFilter) (*logsConsistencyFence, error) {
	if !checkMixedSourceConsistency || dbFilter == nil || fnFilter == nil {
		return nil, nil
	}
	mapping, ok, err := h.ms.LatestBlockMapping()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrGetLogsConsistency
	}
	epoch, pivot := mapping.Epoch, mapping.PivotHash
	if mapping.BnMax != dbFilter.BlockTo {
		epoch, ok, err = h.ms.LatestEpochBeforeBlock(mapping.Epoch, dbFilter.BlockTo)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, ErrGetLogsConsistency
		}
		pivot, ok, err = h.ms.PivotHash(epoch)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, ErrGetLogsConsistency
		}
	}

	latest, err := cfx.GetBlockSummaryByEpoch(cfxtypes.EpochLatestState)
	if err != nil {
		return nil, err
	}
	if latest == nil || latest.EpochNumber == nil || latest.BlockNumber == nil {
		return nil, ErrGetLogsConsistency
	}
	checkpoint := latest.EpochNumber.ToInt().Uint64()
	if fnFilter.ToEpoch != nil {
		if upper, numeric := fnFilter.ToEpoch.ToInt(); numeric && upper.Uint64() < checkpoint {
			checkpoint = upper.Uint64()
		}
	} else if fnFilter.ToBlock != nil && fnFilter.ToBlock.ToInt().Uint64() < latest.BlockNumber.ToInt().Uint64() {
		block, err := cfx.GetBlockSummaryByBlockNumber(hexutil.Uint64(fnFilter.ToBlock.ToInt().Uint64()))
		if err != nil {
			return nil, err
		}
		if block == nil || block.EpochNumber == nil {
			return nil, ErrGetLogsConsistency
		}
		checkpoint = block.EpochNumber.ToInt().Uint64()
	}
	before := latest.Hash.String()
	if checkpoint != latest.EpochNumber.ToInt().Uint64() {
		block, err := cfx.GetBlockSummaryByEpoch(cfxtypes.NewEpochNumberUint64(checkpoint))
		if err != nil {
			return nil, err
		}
		if block == nil {
			return nil, ErrGetLogsConsistency
		}
		before = block.Hash.String()
	}
	readPivot := func(epoch uint64) (string, error) {
		block, err := cfx.GetBlockSummaryByEpoch(cfxtypes.NewEpochNumberUint64(epoch))
		if err != nil {
			return "", err
		}
		if block == nil {
			return "", nil
		}
		return block.Hash.String(), nil
	}
	return &logsConsistencyFence{
		dbBoundary: pivot,
		before:     before,
		boundary:   func() (string, error) { return readPivot(epoch) },
		checkpoint: func() (string, error) { return readPivot(checkpoint) },
	}, nil
}
