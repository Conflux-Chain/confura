package nearhead

import (
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/spf13/viper"
)

var defaultLogLimit = viper.GetInt("nearhead.logLimit")

// TODO use store.LogFilter instead
type logFilter struct {
	epochFrom uint64
	epochTo   uint64
	blocks    map[string]bool   // block hash filter
	contracts map[string]bool   // contract address filter
	topics    []map[string]bool // event hash and indexed data 1, 2, 3
	limit     int
}

func newLogFilter(epochNumFrom, epochNumTo uint64, filter *types.LogFilter) *logFilter {
	result := logFilter{
		epochFrom: epochNumFrom,
		epochTo:   epochNumTo,
		blocks:    newStringFilterWithHashes(filter.BlockHashes),
		limit:     defaultLogLimit,
	}

	// init contract address filter
	if len(filter.Address) > 0 {
		result.contracts = make(map[string]bool)
		for _, v := range filter.Address {
			result.contracts[v.String()] = true
		}
	}

	// init topics filter
	for _, v := range filter.Topics {
		result.topics = append(result.topics, newStringFilterWithHashes(v))
	}

	// remove empty topic filter at tail
	for len(result.topics) > 0 && result.topics[len(result.topics)-1] == nil {
		result.topics = result.topics[:len(result.topics)-1]
	}

	// init limit filter
	if filter.Limit != nil && uint64(*filter.Limit) < uint64(defaultLogLimit) {
		result.limit = int(*filter.Limit)
	}

	return &result
}

func newStringFilterWithHashes(hashes []types.Hash) map[string]bool {
	if len(hashes) == 0 {
		return nil
	}

	filter := make(map[string]bool)

	for _, v := range hashes {
		filter[v.String()] = true
	}

	return filter
}

func (filter *logFilter) matches(log *types.Log) bool {
	// validate epoch number
	if log.EpochNumber == nil {
		return false
	}

	epochNum := log.EpochNumber.ToInt().Uint64()
	if epochNum < filter.epochFrom || epochNum > filter.epochTo {
		return false
	}

	// validate block
	if log.BlockHash == nil {
		return false
	}

	if len(filter.blocks) > 0 && !filter.blocks[log.BlockHash.String()] {
		return false
	}

	// validate contract address
	if len(filter.contracts) > 0 && !filter.contracts[log.Address.String()] {
		return false
	}

	// validate topics
	if len(filter.topics) > len(log.Topics) {
		return false
	}

	for i, len := 0, len(filter.topics); i < len; i++ {
		topicFilter := filter.topics[i]
		topic := log.Topics[i].String()

		if topicFilter != nil && !topicFilter[topic] {
			return false
		}
	}

	return true
}

func (filter *logFilter) matchesBlock(blockHash string) bool {
	return len(filter.blocks) == 0 || filter.blocks[blockHash]
}
