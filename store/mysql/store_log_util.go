package mysql

import (
	"encoding/json"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
)

const defaultBatchSizeLogInsert = 500

var (
	emptyLogs    = []types.Log{}
	emptyLogExts = []*store.LogExtra{}

	errTooManyLogs = errors.New("too many logs")
)

// convertLogTopic converts RPC log topic to store topic.
func convertLogTopic(log *types.Log, index int) string {
	if index < 0 || index >= len(log.Topics) {
		return ""
	}

	return log.Topics[index].String()
}

// constructLogTopics constructs RPC log topics via store topics.
func constructLogTopics(topics ...string) []types.Hash {
	result := make([]types.Hash, 0, len(topics))

	for _, v := range topics {
		if len(v) == 0 {
			break
		}

		result = append(result, types.Hash(v))
	}

	return result
}

// logExtraData is made up of non-indexed log fields.
type logExtraData struct {
	Data                hexutil.Bytes   `json:"data,omitempty"`
	BlockHash           *types.Hash     `json:"bh,omitempty"`
	TransactionHash     *types.Hash     `json:"th,omitempty"`
	TransactionIndex    *hexutil.Big    `json:"ti,omitempty"`
	LogIndex            *hexutil.Big    `json:"li,omitempty"`
	TransactionLogIndex *hexutil.Big    `json:"tli,omitempty"`
	EthExt              *store.LogExtra `json:"eth,omitempty"`
}

func mustMarshalLogExtraData(log *types.Log, ext *store.LogExtra) []byte {
	return util.MustMarshalJson(logExtraData{
		Data:                log.Data,
		BlockHash:           log.BlockHash,
		TransactionHash:     log.TransactionHash,
		TransactionIndex:    log.TransactionIndex,
		LogIndex:            log.LogIndex,
		TransactionLogIndex: log.TransactionLogIndex,
		EthExt:              ext,
	})
}

func silentUnmarshalLogExtraData(extraData []byte) (types.Log, *store.LogExtra, error) {
	var data logExtraData
	if err := json.Unmarshal(extraData, &data); err != nil {
		return types.Log{}, nil, err
	}

	return types.Log{
		Data:                data.Data,
		BlockHash:           data.BlockHash,
		TransactionHash:     data.TransactionHash,
		TransactionIndex:    data.TransactionIndex,
		LogIndex:            data.LogIndex,
		TransactionLogIndex: data.TransactionLogIndex,
	}, data.EthExt, nil
}
