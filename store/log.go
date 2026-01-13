package store

import (
	"encoding/json"

	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	web3go "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

type Log struct {
	ID          uint64
	BlockNumber uint64
	Epoch       uint64
	Topic0      string
	Topic1      string
	Topic2      string
	Topic3      string
	LogIndex    uint64
	Extra       []byte
}

func (log *Log) cmp(other *Log) int {
	if log.BlockNumber < other.BlockNumber {
		return -1
	}

	if log.BlockNumber > other.BlockNumber {
		return 1
	}

	if log.LogIndex < other.LogIndex {
		return -1
	}

	if log.LogIndex > other.LogIndex {
		return 1
	}

	return 0
}

type LogSlice []*Log

func (s LogSlice) Len() int           { return len(s) }
func (s LogSlice) Less(i, j int) bool { return s[i].cmp(s[j]) < 0 }
func (s LogSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func getTopic[T interface{ String() string }](topics []T, i int) string {
	if i < 0 || i >= len(topics) {
		return ""
	}
	return topics[i].String()
}

type cfxLogExtra struct {
	Address             cfxaddress.Address `json:"addr,omitempty"`
	BlockHash           *types.Hash        `json:"bh,omitempty"`
	TransactionHash     *types.Hash        `json:"th,omitempty"`
	TransactionIndex    *hexutil.Big       `json:"ti,omitempty"`
	TransactionLogIndex *hexutil.Big       `json:"tli,omitempty"`
	Data                hexutil.Bytes      `json:"data,omitempty"`
}

func ParseCfxLog(log *types.Log, bn uint64) *Log {
	return &Log{
		BlockNumber: bn,
		Epoch:       log.EpochNumber.ToInt().Uint64(),
		Topic0:      getTopic(log.Topics, 0),
		Topic1:      getTopic(log.Topics, 1),
		Topic2:      getTopic(log.Topics, 2),
		Topic3:      getTopic(log.Topics, 3),
		LogIndex:    log.LogIndex.ToInt().Uint64(),
		Extra: util.MustMarshalJson(cfxLogExtra{
			Address:             log.Address,
			BlockHash:           log.BlockHash,
			TransactionHash:     log.TransactionHash,
			TransactionIndex:    log.TransactionIndex,
			TransactionLogIndex: log.TransactionLogIndex,
			Data:                log.Data,
		}),
	}
}

func (log *Log) ToCfxLog() (*types.Log, error) {
	var extra cfxLogExtra
	if err := json.Unmarshal(log.Extra, &extra); err != nil {
		return nil, errors.WithMessage(err, "failed to unmarshal cfx log from extra field")
	}

	var topics []types.Hash
	for _, v := range []string{log.Topic0, log.Topic1, log.Topic2, log.Topic3} {
		if len(v) > 0 {
			topics = append(topics, types.Hash(v))
		}
	}

	return &types.Log{
		Address:             extra.Address,
		Topics:              topics,
		Data:                extra.Data,
		BlockHash:           extra.BlockHash,
		EpochNumber:         types.NewBigInt(log.Epoch),
		TransactionHash:     extra.TransactionHash,
		TransactionIndex:    extra.TransactionIndex,
		LogIndex:            types.NewBigInt(log.LogIndex),
		TransactionLogIndex: extra.TransactionLogIndex,
	}, nil
}

type ethLogExtra struct {
	Address             common.Address `json:"addr,omitempty"`
	BlockHash           common.Hash    `json:"bh,omitempty"`
	TransactionHash     common.Hash    `json:"th,omitempty"`
	TransactionIndex    uint           `json:"ti,omitempty"`
	TransactionLogIndex *uint          `json:"tli,omitempty"`
	Data                hexutil.Bytes  `json:"data,omitempty"`
	LogType             *string        `json:"type,omitempty"`
	Removed             bool           `json:"removed,omitempty"`
	BlockTimestamp      uint64         `json:"bts,omitempty"`
}

func ParseEthLog(log *web3go.Log) *Log {
	return &Log{
		BlockNumber: log.BlockNumber,
		Epoch:       log.BlockNumber,
		Topic0:      getTopic(log.Topics, 0),
		Topic1:      getTopic(log.Topics, 1),
		Topic2:      getTopic(log.Topics, 2),
		Topic3:      getTopic(log.Topics, 3),
		LogIndex:    uint64(log.Index),
		Extra: util.MustMarshalJson(ethLogExtra{
			Address:             log.Address,
			BlockHash:           log.BlockHash,
			TransactionHash:     log.TxHash,
			TransactionIndex:    log.TxIndex,
			TransactionLogIndex: log.TransactionLogIndex,
			Data:                log.Data,
			LogType:             log.LogType,
			Removed:             log.Removed,
			BlockTimestamp:      log.BlockTimestamp,
		}),
	}
}

func (log *Log) ToEthLog() (*web3go.Log, error) {
	var extra ethLogExtra
	if err := json.Unmarshal(log.Extra, &extra); err != nil {
		return nil, errors.WithMessage(err, "failed to unmarshal eth log from extra field")
	}

	var topics []common.Hash
	for _, v := range []string{log.Topic0, log.Topic1, log.Topic2, log.Topic3} {
		if len(v) > 0 {
			topics = append(topics, common.HexToHash(v))
		}
	}

	return &web3go.Log{
		Address:             extra.Address,
		BlockHash:           extra.BlockHash,
		BlockNumber:         log.BlockNumber,
		BlockTimestamp:      extra.BlockTimestamp,
		Topics:              topics,
		Data:                extra.Data,
		Index:               uint(log.LogIndex),
		LogType:             extra.LogType,
		Removed:             extra.Removed,
		TxHash:              extra.TransactionHash,
		TxIndex:             extra.TransactionIndex,
		TransactionLogIndex: extra.TransactionLogIndex,
	}, nil
}
