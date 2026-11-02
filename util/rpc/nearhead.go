package rpc

import (
	"context"
	"fmt"

	"github.com/Conflux-Chain/confura-data-cache/nearhead"
	ct "github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/evm"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/poll"
	"github.com/ethereum/go-ethereum/common"
	wt "github.com/openweb3/web3go/types"
)

// Serialize the dependency's Pop/Put pair with multi-block log snapshots.
// Its individual methods are safe, but do not provide a compound read view.
type nearHeadWriter struct {
	cache  *EthNearHeadCache
	writer *nearhead.Writer
}

func (w *nearHeadWriter) Process(ctx context.Context, data poll.Revertable[evm.BlockData]) {
	w.cache.snapshotMu.Lock()
	defer w.cache.snapshotMu.Unlock()
	w.writer.Process(ctx, data)
}

func (c *EthNearHeadCache) Put(data evm.BlockData) error {
	c.snapshotMu.Lock()
	defer c.snapshotMu.Unlock()
	return c.EthCache.Put(data)
}
func (c *EthNearHeadCache) Pop(number uint64) int {
	c.snapshotMu.Lock()
	defer c.snapshotMu.Unlock()
	return c.EthCache.Pop(number)
}

func (c *EthNearHeadCache) GetLogsByBlockHash(hash common.Hash, filter nearhead.FilterOpt) (*nearhead.EthLogs, error) {
	c.snapshotMu.RLock()
	defer c.snapshotMu.RUnlock()
	ref := ct.BlockHashOrNumberWithHash(hash)
	block := c.EthCache.GetBlock(ref, false).Result
	if block == nil {
		return nil, nil
	}
	receipts := c.EthCache.GetBlockReceipts(ref).Result
	if receipts == nil {
		return nil, nil
	}
	logs, err := c.collectLogs(nil, receipts, filter)
	if err != nil {
		return nil, err
	}
	return &nearhead.EthLogs{FromBlock: block.Number.Uint64(), ToBlock: block.Number.Uint64(), Logs: logs}, nil
}

func (c *EthNearHeadCache) logsSnapshot(from, to uint64, filter nearhead.FilterOpt) (*nearhead.EthLogs, common.Hash, error) {
	c.snapshotMu.RLock()
	defer c.snapshotMu.RUnlock()
	if from > to {
		return nil, common.Hash{}, fmt.Errorf("invalid block range params")
	}
	coverage := c.EthCache.GetBlock(ct.BlockHashOrNumberWithNumber(from), false).Coverage
	if coverage[0] == coverage[1] || to < coverage[0] || from >= coverage[1] {
		return nil, common.Hash{}, nil
	}
	from, to = max(from, coverage[0]), min(to, coverage[1]-1)
	logs := make([]wt.Log, 0)
	for number := from; number <= to; number++ {
		receipts := c.EthCache.GetBlockReceipts(ct.BlockHashOrNumberWithNumber(number)).Result
		if receipts == nil {
			return nil, common.Hash{}, nil
		} // receipts were not collected
		var err error
		logs, err = c.collectLogs(logs, receipts, filter)
		if err != nil {
			return nil, common.Hash{}, err
		}
	}
	last := c.EthCache.GetBlock(ct.BlockHashOrNumberWithNumber(to), false).Result
	if last == nil {
		return nil, common.Hash{}, nil
	}
	return &nearhead.EthLogs{FromBlock: from, ToBlock: to, Logs: logs}, last.Hash, nil
}

func (c *EthNearHeadCache) collectLogs(logs []wt.Log, receipts []*wt.Receipt, filter nearhead.FilterOpt) ([]wt.Log, error) {
	if logs == nil {
		logs = make([]wt.Log, 0)
	}
	for _, receipt := range receipts {
		if receipt == nil {
			return nil, fmt.Errorf("incomplete cached receipt")
		}
		for _, entry := range receipt.Logs {
			if entry == nil {
				return nil, fmt.Errorf("invalid cached log")
			}
			if !util.IncludeEthLogAddrs(entry, filter.Addresses) || !util.MatchEthLogTopics(entry, filter.Topics) {
				continue
			}
			copied := *entry
			copied.Topics = append([]common.Hash(nil), entry.Topics...)
			copied.Data = append([]byte(nil), entry.Data...)
			logs = append(logs, copied)
			if len(logs) > c.maxLogs {
				return nil, fmt.Errorf("the result set exceeds the max limit of %v logs, please narrow down your filter conditions", c.maxLogs)
			}
		}
	}
	return logs, nil
}

// GetLogsByBlockRange returns a snapshot without using the dependency's topic
// matcher. A caller combining this with other sources must fence the chain view.
func (c *EthNearHeadCache) GetLogsByBlockRange(from, to uint64, filter nearhead.FilterOpt) (*nearhead.EthLogs, error) {
	logs, _, err := c.logsSnapshot(from, to, filter)
	return logs, err
}
