package tracelog

import (
	"math/big"

	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
)

// VirtualLog wraps a types.Log with its compact index identifiers.
type VirtualLog struct {
	*types.Log
	ContractIdx ContractIndex
	EventIdx    EventIndex
}

// EnrichAndConvertLogs assigns log indices, timestamps, and converts types.Log
// into InternalContractLog database models.
func EnrichAndConvertLogs(logs []*VirtualLog, blocks []*types.Block) ([]*mysql.InternalContractLog, error) {
	blockMap := make(map[types.Hash]*types.Block, len(blocks))
	for _, b := range blocks {
		blockMap[b.Hash] = b
	}

	var (
		result         []*mysql.InternalContractLog
		globalLogIndex uint64
	)
	txnLogIndex := make(map[types.Hash]uint64)

	for _, log := range logs {
		if log.BlockHash == nil || log.TransactionHash == nil {
			return nil, errors.New("log missing block hash or transaction hash")
		}

		block, ok := blockMap[*log.BlockHash]
		if !ok {
			return nil, errors.Errorf("block not found for hash %s", log.BlockHash)
		}

		txnHash := *log.TransactionHash

		// Enrich the log in-place
		log.LogIndex = (*hexutil.Big)(new(big.Int).SetUint64(globalLogIndex))
		log.TransactionLogIndex = (*hexutil.Big)(new(big.Int).SetUint64(txnLogIndex[txnHash]))
		log.BlockTimestamp = block.Timestamp

		globalLogIndex++
		txnLogIndex[txnHash]++

		// Convert to DB model
		dbLog := &mysql.InternalContractLog{
			BlockNumber:  block.BlockNumber.ToInt().Uint64(),
			Epoch:        log.EpochNumber.ToInt().Uint64(),
			BlockHash:    log.BlockHash.String(),
			TxHash:       log.TransactionHash.String(),
			TxIndex:      int(log.TransactionIndex.ToInt().Int64()),
			LogIndex:     int(log.LogIndex.ToInt().Int64()),
			AddressIndex: uint8(log.ContractIdx),
			Topic0Index:  uint8(log.EventIdx),
			Data:         log.Data,
		}

		for i, topic := range log.Topics {
			switch i {
			case 1:
				dbLog.Topic1 = topic.String()
			case 2:
				dbLog.Topic2 = topic.String()
			case 3:
				dbLog.Topic3 = topic.String()
			}
		}

		result = append(result, dbLog)
	}

	return result, nil
}
