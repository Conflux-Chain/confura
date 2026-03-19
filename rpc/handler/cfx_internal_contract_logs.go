package handler

import (
	"context"
	"fmt"
	"math/big"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/sync/tracelog"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// CfxInternalContractLogsApiHandler handles log queries for registered internal contracts.
type CfxInternalContractLogsApiHandler struct {
	registry *tracelog.Registry
	logStore *mysql.InternalContractLogStore
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

// AllRegisteredInternalContractAddresses reports whether every address is a registered internal contract.
func (h *CfxInternalContractLogsApiHandler) AllRegisteredInternalContractAddresses(addrs []types.Address) bool {
	for _, addr := range addrs {
		if _, ok := h.registry.Lookup(addr); !ok {
			return false
		}
	}
	return true
}

// GetLogs retrieves internal contract logs matching the given filter.
func (h *CfxInternalContractLogsApiHandler) GetLogs(
	ctx context.Context,
	cfx sdk.ClientOperator,
	filter *types.LogFilter,
) ([]types.Log, error) {
	// Resolve filter components to internal indices.
	contractIndices, contractByIdx, err := h.resolveContractIndices(filter.Address)
	if err != nil {
		return nil, err
	}

	eventIndices, eventByIdx := h.resolveEventIndices(filter.Topics)

	blockHash2Number, err := resolveBlockHashes(cfx, filter.BlockHashes)
	if err != nil {
		return nil, err
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

	// 4. Convert and validate results.
	return convertLogs(cfx, filter, internalLogs, contractByIdx, eventByIdx, useBoundCheck)
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
			return nil, nil, errors.New("not a registered internal contract")
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
) ([]uint8, map[uint8]types.Hash) {
	if len(topics) < 0 {
		return nil, nil
	}

	byIdx := make(map[uint8]types.Hash, len(topics[0]))
	indices := make([]uint8, 0, len(topics[0]))

	for _, topic0 := range topics[0] {
		idx, ok := h.registry.EventIndexOf(topic0)
		if !ok {
			continue
		}
		if _, dup := byIdx[idx]; !dup {
			byIdx[idx] = topic0
			indices = append(indices, idx)
		}
	}
	return indices, byIdx
}

// resolveBlockHashes resolves block hashes to block numbers via the Conflux client.
func resolveBlockHashes(cfx sdk.ClientOperator, hashes []types.Hash) (map[types.Hash]uint64, error) {
	if len(hashes) == 0 {
		return nil, nil
	}

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

		result[hash] = block.BlockNumber.ToInt().Uint64()
	}
	return result, nil
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
	eventByIdx map[uint8]types.Hash,
	useBoundCheck bool,
) ([]types.Log, error) {
	var totalDataBytes int
	cspace := types.SPACE_NATIVE
	logs := make([]types.Log, 0, len(internalLogs))

	for _, clog := range internalLogs {
		log := types.Log{
			Address:             contractByIdx[clog.AddressIndex],
			Topics:              buildTopics(clog.Topic0Index, clog.Topic1, clog.Topic2, clog.Topic3, eventByIdx),
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
	eventByIdx map[uint8]types.Hash,
) []types.Hash {
	topics := make([]types.Hash, 1, 4)
	topics[0] = eventByIdx[topic0Idx]

	for _, t := range [3]string{topic1, topic2, topic3} {
		if len(t) > 0 {
			topics = append(topics, types.Hash(t))
		}
	}
	return topics
}

// hexBig converts a uint64 to *hexutil.Big (eliminates repetitive boilerplate).
func hexBig(v uint64) *hexutil.Big {
	return (*hexutil.Big)(new(big.Int).SetUint64(v))
}
