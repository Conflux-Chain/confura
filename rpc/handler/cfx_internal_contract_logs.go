package handler

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/sync/tracelog"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	rpcprovider "github.com/openweb3/go-rpc-provider"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type internalContractLogReader interface {
	GetReorgVersionWithContext(context.Context) (int, error)
	GetLogs(context.Context, mysql.CfxInternalContractLogFilter) ([]mysql.InternalContractLog, error)
	BlockMapping(context.Context, uint64) (mysql.CfxTraceSyncEpochBlockMap, error)
}

// CfxInternalContractLogsApiHandler handles log queries for registered internal contracts.
type CfxInternalContractLogsApiHandler struct {
	registry *tracelog.Registry
	logStore internalContractLogReader
}

func NewCfxInternalContractLogsApiHandler(
	registry *tracelog.Registry,
	logStore *mysql.InternalContractLogStore,
) *CfxInternalContractLogsApiHandler {
	return &CfxInternalContractLogsApiHandler{
		registry: registry, logStore: logStore,
	}
}

func MustNewCfxInternalContractLogsApiHandler(
	cfxDB *mysql.CfxStore,
	clientProvider *node.CfxClientProvider,
) *CfxInternalContractLogsApiHandler {
	cfx, err := clientProvider.GetClientRandom()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to get random client from node client provider")
	}

	registry, err := tracelog.NewRegistry(cfx)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create internal contract registry")
	}

	internalContractStore := mysql.NewInternalContractLogStore(cfxDB.DB())
	return NewCfxInternalContractLogsApiHandler(registry, internalContractStore)
}

// FilterRegisteredInternalContractAddresses filters the given addresses to only include registered internal contracts.
func (h *CfxInternalContractLogsApiHandler) FilterRegisteredInternalContractAddresses(addrs []types.Address) []types.Address {
	var filtered []types.Address
	for _, addr := range addrs {
		if _, ok := h.registry.Lookup(addr); ok {
			filtered = append(filtered, addr)
		}
	}
	return filtered
}

// GetLogs retrieves internal contract logs matching the given filter.
func (h *CfxInternalContractLogsApiHandler) GetLogs(
	ctx context.Context,
	cfx sdk.ClientOperator,
	filter *types.LogFilter,
) ([]types.Log, error) {
	retry := newLogsQueryRetry(ctx, RequireBoundChecks(filter), "cfx_getInternalContractLogs")
	defer retry.close()
	// record the reorg version before query to ensure data consistence
	lastReorgVersion, err := h.logStore.GetReorgVersionWithContext(retry.ctx)
	if err != nil {
		return nil, err
	}

	for {
		if err := retry.check(); err != nil {
			return nil, err
		}
		// query data with reorg guard
		logs, queryErr := h.getLogsReorgGuard(retry.ctx, cfxLogsWithContext(retry.ctx, cfx), filter)
		if err := retry.check(); err != nil {
			return nil, err
		}
		if queryErr != nil && !errors.Is(queryErr, ErrGetLogsConsistency) {
			return nil, queryErr
		}

		// A view-dependent error is final only if the queried view stayed unchanged.
		reorgVersion, err := h.logStore.GetReorgVersionWithContext(retry.ctx)
		if err != nil {
			return nil, err
		}

		if reorgVersion == lastReorgVersion {
			return logs, queryErr
		}

		// when reorg occurred, check timeout before retry.
		if err := retry.retry("database"); err != nil {
			return nil, err
		}

		// reorg version changed during data query and try again.
		lastReorgVersion = reorgVersion
	}
}

func (h *CfxInternalContractLogsApiHandler) getLogsReorgGuard(
	ctx context.Context,
	cfx sdk.ClientOperator,
	filter *types.LogFilter,
) ([]types.Log, error) {
	// Resolve filter components to internal indices.
	contractIndices, contractByIdx, err := h.resolveContractIndices(filter.Address)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to resolve contract indices")
	}

	blockHash2Number, err := h.resolveBlockHashes(ctx, cfx, filter.BlockHashes)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to resolve block hashes")
	}

	eventIndices, unregisteredEventHashes := h.resolveEventIndices(filter.Topics)
	if len(eventIndices) == 0 && len(unregisteredEventHashes) > 0 {
		// No registered events matched: all topics are unrecognized, so nothing to process.
		logrus.WithFields(logrus.Fields{
			"unregisteredEvents": unregisteredEventHashes,
			"filter":             filter,
		}).Debug("Skipping logs: no registered internal contract events matched (all topics unrecognized)")
		return nil, nil
	}

	// Configure context based on bound-check requirements.
	useBoundCheck := RequireBoundChecks(filter)
	ctx, cancel := prepareContext(ctx, useBoundCheck)
	if cancel != nil {
		defer cancel()
	}

	// Query internal contract logs.
	internalLogs, err := h.logStore.GetLogs(ctx, mysql.CfxInternalContractLogFilter{
		LogFilter:         filter,
		BlockHash2Numbers: blockHash2Number,
		ContractIndices:   contractIndices,
		EventIndices:      eventIndices,
	})
	if err != nil {
		return nil, err
	}

	validated := make(map[string]uint64, len(blockHash2Number))
	for hash, bn := range blockHash2Number {
		validated[strings.ToLower(hash.String())] = bn
	}
	for _, log := range internalLogs {
		if len(validated) == 0 {
			break
		}
		bn, ok := validated[strings.ToLower(log.BlockHash)]
		if !ok || bn != log.BlockNumber {
			return nil, errors.WithMessage(ErrGetLogsConsistency, "trace log identity differs from validated block")
		}
	}

	// Convert and validate results.
	return convertLogs(cfx, filter, internalLogs, contractByIdx, h.registry.EventHashByIndex, useBoundCheck)
}

// resolveContractIndices maps addresses to registered contract indices.
func (h *CfxInternalContractLogsApiHandler) resolveContractIndices(
	addrs []types.Address,
) ([]uint8, map[uint8]types.Address, error) {
	byIdx := make(map[uint8]types.Address, len(addrs))
	indices := make([]uint8, 0, len(addrs))

	for _, addr := range addrs {
		idx, ok := h.registry.ContractIndexOf(addr)
		if !ok {
			return nil, nil, errors.Errorf("%v is not a registered internal contract", addr)
		}
		if _, dup := byIdx[idx]; !dup {
			byIdx[idx] = addr
			indices = append(indices, idx)
		}
	}
	return indices, byIdx, nil
}

// resolveEventIndices maps topic0 values to registered event indices.
func (h *CfxInternalContractLogsApiHandler) resolveEventIndices(
	topics [][]types.Hash,
) (
	registeredEventIndices []uint8,
	unregisteredEventHashes []types.Hash,
) {
	if len(topics) <= 0 || len(topics[0]) <= 0 {
		return nil, nil
	}

	registeredEventHashesByIdx := make(map[uint8]types.Hash, len(topics[0]))
	registeredEventIndices = make([]uint8, 0, len(topics[0]))

	for _, topic0 := range topics[0] {
		idx, ok := h.registry.EventIndexOf(topic0)
		if !ok {
			unregisteredEventHashes = append(unregisteredEventHashes, topic0)
			continue
		}

		if _, dup := registeredEventHashesByIdx[idx]; !dup {
			registeredEventHashesByIdx[idx] = topic0
			registeredEventIndices = append(registeredEventIndices, idx)
		}
	}
	return
}

// resolveBlockHashes resolves block hashes to block numbers via the Conflux client.
func (h *CfxInternalContractLogsApiHandler) resolveBlockHashes(ctx context.Context, cfx sdk.ClientOperator, hashes []types.Hash) (map[types.Hash]uint64, error) {
	if len(hashes) == 0 {
		return nil, nil
	}

	// TODO: resolve hashes in parallel (e.g. goroutines + semaphore) with a capped concurrency?
	result := make(map[types.Hash]uint64, len(hashes))
	for _, hash := range hashes {
		if _, exists := result[hash]; exists {
			continue
		}

		block, err := cfx.GetBlockSummaryByHash(hash)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to get block by hash")
		}
		if block == nil {
			return nil, fmt.Errorf("unable to identify block %v", hash)
		}
		if block.BlockNumber == nil {
			return nil, fmt.Errorf("block with hash %v is not executed yet", hash)
		}

		if !strings.EqualFold(block.Hash.String(), hash.String()) || block.EpochNumber == nil || !block.EpochNumber.ToInt().IsUint64() {
			return nil, fmt.Errorf("invalid block identity for %v", hash)
		}
		epoch := block.EpochNumber.ToInt().Uint64()
		mapping, err := h.logStore.BlockMapping(ctx, epoch)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, fmt.Errorf("%w: trace mapping unavailable for epoch %d: %w", ErrGetLogsConsistency, epoch, err)
			}
			return nil, errors.WithMessagef(err, "trace mapping unavailable for epoch %d", epoch)
		}
		if mapping.Epoch != epoch || mapping.PivotHash == "" || mapping.BnMin > mapping.BnMax {
			return nil, fmt.Errorf("invalid trace mapping for epoch %d", epoch)
		}
		// Validate against the pivot saved with the traces, not the independently
		// synchronized event-log mapping. Never fetch traces on the query path.
		checked, err := cfx.GetBlockByHashWithPivotAssumption(hash, types.Hash(mapping.PivotHash), hexutil.Uint64(epoch))
		if err != nil {
			if isTracePivotAssumptionError(err) {
				return nil, fmt.Errorf("%w: trace pivot assumption failed: %w", ErrGetLogsConsistency, err)
			}
			return nil, errors.WithMessage(err, "trace pivot assumption failed")
		}
		if !strings.EqualFold(checked.Hash.String(), hash.String()) || checked.EpochNumber == nil || checked.EpochNumber.ToInt().Cmp(block.EpochNumber.ToInt()) != 0 ||
			checked.BlockNumber == nil || !checked.BlockNumber.ToInt().IsUint64() {
			return nil, fmt.Errorf("%w: block %v does not match trace epoch %d", ErrGetLogsConsistency, hash, epoch)
		}
		bn := checked.BlockNumber.ToInt().Uint64()
		if bn < mapping.BnMin || bn > mapping.BnMax {
			return nil, fmt.Errorf("%w: block %v is outside trace epoch %d", ErrGetLogsConsistency, hash, epoch)
		}
		result[hash] = bn
	}
	return result, nil
}

// Fullnode reports a pivot conflict as invalid parameters. Other RPC and
// transport errors from the same method must not enter the reorg retry path.
func isTracePivotAssumptionError(err error) bool {
	var rpcErr rpcprovider.Error
	if !errors.As(err, &rpcErr) || rpcErr.ErrorCode() != -32602 {
		return false
	}
	switch rpcErr.Error() {
	case "Invalid parameters: pivot chain assumption failed", "Invalid parameters: Error: pivot chain assumption failed":
		return true
	default:
		return false
	}
}

// prepareContext applies a timeout when bound checks are required,
// otherwise disables bound checks on the context.
func prepareContext(ctx context.Context, useBoundCheck bool) (context.Context, context.CancelFunc) {
	if useBoundCheck {
		return context.WithTimeout(ctx, store.TimeoutGetLogs)
	}
	return store.NewContextWithBoundChecksDisabled(ctx), nil
}

// convertLogs transforms internal contract logs into types.Log entries,
// enforcing size and count limits when bound checks are enabled.
func convertLogs(
	cfx sdk.ClientOperator,
	filter *types.LogFilter,
	internalLogs []mysql.InternalContractLog,
	contractByIdx map[uint8]types.Address,
	eventIdxResolver func(uint8) (types.Hash, bool),
	useBoundCheck bool,
) ([]types.Log, error) {
	var totalDataBytes int
	cspace := types.SPACE_NATIVE
	logs := make([]types.Log, 0, len(internalLogs))

	for _, clog := range internalLogs {
		topics, err := buildTopics(clog.Topic0Index, clog.Topic1, clog.Topic2, clog.Topic3, eventIdxResolver)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to build log topics")
		}

		log := types.Log{
			Address:             contractByIdx[clog.AddressIndex],
			Topics:              topics,
			Data:                clog.Data,
			BlockHash:           (*types.Hash)(&clog.BlockHash),
			TransactionHash:     (*types.Hash)(&clog.TxHash),
			EpochNumber:         hexBig(clog.Epoch),
			LogIndex:            hexBig(uint64(clog.LogIndex)),
			TransactionIndex:    hexBig(uint64(clog.TxIndex)),
			TransactionLogIndex: hexBig(uint64(clog.TxLogIndex)),
			Space:               &cspace,
			BlockTimestamp:      hexBig(clog.BlockTimestamp),
		}

		totalDataBytes += len(log.Data)
		if useBoundCheck && uint64(totalDataBytes) > maxGetLogsResponseBytes {
			return nil, newSuggestedBodyBytesOversizedError(cfx, filter, &log)
		}

		logs = append(logs, log)
	}

	if useBoundCheck && len(logs) > int(store.MaxLogLimit) {
		return nil, newSuggestedResultSetOversizedError(cfx, filter, &logs[store.MaxLogLimit])
	}

	return logs, nil
}

// buildTopics assembles the topics slice from indexed and optional topic fields.
func buildTopics(
	topic0Idx uint8,
	topic1, topic2, topic3 string,
	eventIdxResolver func(uint8) (types.Hash, bool),
) ([]types.Hash, error) {
	topic0, ok := eventIdxResolver(topic0Idx)
	if !ok {
		return nil, errors.Errorf("unknown event index %d", topic0Idx)
	}

	topics := make([]types.Hash, 1, 4)
	topics[0] = topic0

	for _, t := range [3]string{topic1, topic2, topic3} {
		if len(t) > 0 {
			topics = append(topics, types.Hash(t))
		}
	}
	return topics, nil
}

// hexBig converts a uint64 to *hexutil.Big (eliminates repetitive boilerplate).
func hexBig(v uint64) *hexutil.Big {
	return (*hexutil.Big)(new(big.Int).SetUint64(v))
}
