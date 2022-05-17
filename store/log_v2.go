package store

import (
	"encoding/json"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/sirupsen/logrus"
)

type LogV2 struct {
	ID          uint64
	ContractID  uint64
	BlockNumber uint64
	Epoch       uint64
	Topic0      string
	Topic1      string
	Topic2      string
	Topic3      string
	LogIndex    uint64
	Extra       []byte
}

func (log *LogV2) cmp(other *LogV2) int {
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

type LogSlice []*LogV2

func (s LogSlice) Len() int           { return len(s) }
func (s LogSlice) Less(i, j int) bool { return s[i].cmp(s[j]) < 0 }
func (s LogSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type logExtraData struct {
	Address             cfxaddress.Address `json:"addr,omitempty"`
	BlockHash           *types.Hash        `json:"bh,omitempty"`
	TransactionHash     *types.Hash        `json:"th,omitempty"`
	TransactionIndex    *hexutil.Big       `json:"ti,omitempty"`
	TransactionLogIndex *hexutil.Big       `json:"tli,omitempty"`
	Data                hexutil.Bytes      `json:"data,omitempty"`
	EthExtra            *LogExtra          `json:"eth,omitempty"`
}

func ParseCfxLog(log *types.Log, cid, bn uint64, logExt *LogExtra) *LogV2 {
	convertLogTopicFunc := func(log *types.Log, index int) string {
		if index < 0 || index >= len(log.Topics) {
			return ""
		}

		return log.Topics[index].String()
	}

	return &LogV2{
		ContractID:  cid,
		BlockNumber: bn,
		Epoch:       log.EpochNumber.ToInt().Uint64(),
		Topic0:      convertLogTopicFunc(log, 0),
		Topic1:      convertLogTopicFunc(log, 1),
		Topic2:      convertLogTopicFunc(log, 2),
		Topic3:      convertLogTopicFunc(log, 3),
		LogIndex:    log.LogIndex.ToInt().Uint64(),
		Extra: util.MustMarshalJson(logExtraData{
			Address:             log.Address,
			BlockHash:           log.BlockHash,
			TransactionHash:     log.TransactionHash,
			TransactionIndex:    log.TransactionIndex,
			TransactionLogIndex: log.TransactionLogIndex,
			Data:                log.Data,
			EthExtra:            logExt,
		}),
	}
}

func (log *LogV2) ToCfxLog() (*types.Log, *LogExtra) {
	var extra logExtraData
	if err := json.Unmarshal(log.Extra, &extra); err != nil {
		logrus.WithError(err).Error("Failed to unmarshal cfx log from Extra field")
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
	}, extra.EthExtra
}
