package redis

import (
	"context"
	"time"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/metrics"
	"github.com/conflux-chain/conflux-infura/store"
	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	redisCacheExpireDuration = time.Hour * 12 // default expiration duration is 12 hours
)

type redisStore struct {
	ctx context.Context
	rdb *redis.Client
}

func MustNewCacheStore() *redisStore {
	redisUrl := viper.GetViper().GetString("store.redis.url")
	opt, err := redis.ParseURL(redisUrl)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to parse redis url")
	}

	rdb := redis.NewClient(opt)
	ctx := context.Background()

	// Test redis connection
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		logrus.WithError(err).Fatal("Failed to create redis store")
	}

	return &redisStore{rdb: rdb, ctx: ctx}
}

func (rs *redisStore) IsRecordNotFound(err error) bool {
	return err == redis.Nil || err == store.ErrNotFound
}

func (rs *redisStore) GetBlockEpochRange() (uint64, uint64, error) {
	return loadEpochRange(rs.ctx, rs.rdb, store.EpochBlock)
}

func (rs *redisStore) GetTransactionEpochRange() (uint64, uint64, error) {
	return loadEpochRange(rs.ctx, rs.rdb, store.EpochTransaction)
}

func (rs *redisStore) GetLogEpochRange() (uint64, uint64, error) {
	return loadEpochRange(rs.ctx, rs.rdb, store.EpochLog)
}

func (rs *redisStore) GetGlobalEpochRange() (uint64, uint64, error) {
	return loadEpochRange(rs.ctx, rs.rdb, store.EpochDataNil)
}

func (rs *redisStore) GetNumBlocks() uint64 {
	cnt, err := loadEpochDataCount(rs.ctx, rs.rdb, store.EpochBlock)
	if err != nil && !rs.IsRecordNotFound(err) {
		logrus.WithError(err).Error("Failed to get statistics (num of blocks) from redis")
	}

	return cnt
}

func (rs *redisStore) GetNumTransactions() uint64 {
	cnt, err := loadEpochDataCount(rs.ctx, rs.rdb, store.EpochTransaction)
	if err != nil && !rs.IsRecordNotFound(err) {
		logrus.WithError(err).Error("Failed to get statistics (num of transactions) from redis")
	}

	return cnt
}

func (rs *redisStore) GetNumLogs() uint64 {
	cnt, err := loadEpochDataCount(rs.ctx, rs.rdb, store.EpochLog)
	if err != nil && !rs.IsRecordNotFound(err) {
		logrus.WithError(err).Error("Failed to get statistics (num of logs)  from redis")
	}

	return cnt
}

func (rs *redisStore) GetLogs(filter store.LogFilter) (logs []types.Log, err error) {
	// TODO add implementation
	return nil, store.ErrUnsupported
}

func (rs *redisStore) GetTransaction(txHash types.Hash) (*types.Transaction, error) {
	return loadTx(rs.ctx, rs.rdb, txHash)
}

func (rs *redisStore) GetReceipt(txHash types.Hash) (*types.TransactionReceipt, error) {
	return loadTxReceipt(rs.ctx, rs.rdb, txHash)
}

func (rs *redisStore) GetBlocksByEpoch(epochNumber uint64) ([]types.Hash, error) {
	return loadEpochBlocks(rs.ctx, rs.rdb, epochNumber)
}

func (rs *redisStore) GetBlockByEpoch(epochNumber uint64) (*types.Block, error) {
	// TODO Cannot get tx from redis in advance, since only executed txs are saved in store
	return nil, store.ErrUnsupported
}

func (rs *redisStore) GetBlockSummaryByEpoch(epochNumber uint64) (*types.BlockSummary, error) {
	pivotBlock, err := loadEpochPivotBlock(rs.ctx, rs.rdb, epochNumber)
	if err != nil {
		logrus.WithField("epochNumber", epochNumber).WithError(err).Debug("Pivot block missed in cache")
		return nil, err
	}

	return loadBlockByHash(rs.ctx, rs.rdb, pivotBlock)
}

func (rs *redisStore) GetBlockByHash(blockHash types.Hash) (*types.Block, error) {
	return nil, store.ErrUnsupported
}

func (rs *redisStore) GetBlockSummaryByHash(blockHash types.Hash) (*types.BlockSummary, error) {
	return loadBlockByHash(rs.ctx, rs.rdb, blockHash)
}

func (rs *redisStore) GetBlockByBlockNumber(blockNumber uint64) (*types.Block, error) {
	return nil, store.ErrUnsupported
}

func (rs *redisStore) GetBlockSummaryByBlockNumber(blockNumber uint64) (*types.BlockSummary, error) {
	return loadBlockByNumber(rs.ctx, rs.rdb, blockNumber)
}

func (rs *redisStore) Push(data *store.EpochData) error {
	return rs.Pushn([]*store.EpochData{data})
}

func (rs *redisStore) Pushn(dataSlice []*store.EpochData) error {
	if len(dataSlice) == 0 {
		return nil
	}

	updater := metrics.NewTimerUpdaterByName("infura/duration/store/redis/write")
	defer updater.Update()

	_, lastEpoch, err := rs.GetGlobalEpochRange()
	if rs.IsRecordNotFound(err) { // epoch range not found in redis
		lastEpoch = citypes.EpochNumberNil
	} else if err != nil {
		return errors.WithMessage(err, "failed to get global epoch range from redis")
	}

	watchKeys := make([]string, 0, len(dataSlice))
	for _, data := range dataSlice {
		if lastEpoch == citypes.EpochNumberNil {
			lastEpoch = data.Number
		} else {
			lastEpoch++
		}

		if data.Number != lastEpoch { // ensure continous epoch
			return errors.WithMessagef(store.ErrContinousEpochRequired,
				"expected epoch #%v, but #%v got", lastEpoch, data.Number,
			)
		}

		watchKeys = append(watchKeys, getEpochBlocksCacheKey(data.Number))
	}

	return rs.execWithTx(func(tx *redis.Tx) error {
		txOpHistory := store.EpochDataOpAffects{}

		// Operation is commited only if the watched keys remain unchanged.
		_, err = tx.TxPipelined(rs.ctx, func(pipe redis.Pipeliner) error {
			for _, data := range dataSlice {
				opHistory, err := rs.putOneWithTx(pipe, data)
				if err != nil {
					return err
				}

				txOpHistory.Merge(opHistory)
			}

			logrus.WithField("opHistory", txOpHistory).Debug("Pushn db operation history")

			// update epoch data count
			if err := rs.updateEpochDataCount(pipe, txOpHistory); err != nil {
				return err
			}

			// update max of epoch range
			if err := rs.updateEpochRangeMax(pipe, lastEpoch); err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			logrus.WithField("lastEpoch", lastEpoch).WithError(err).Debug("Failed to push epoch data to reids store with pipeline")
		}

		return err

	}, watchKeys...)
}

func (rs *redisStore) Pop() error {
	_, maxEpoch, err := rs.GetGlobalEpochRange()
	if err != nil {
		return errors.WithMessage(err, "Failed to get global epoch range")
	}

	return rs.Popn(maxEpoch)
}

// Popn pops multiple epoch data from redis.
func (rs *redisStore) Popn(epochUntil uint64) error {
	_, maxEpoch, err := rs.GetGlobalEpochRange()
	if err != nil {
		return errors.WithMessage(err, "Failed to get global epoch range")
	}

	if epochUntil > maxEpoch {
		return nil
	}

	return rs.remove(epochUntil, maxEpoch, store.EpochRemoveAll, store.EpochOpPop)
}

func (rs *redisStore) DequeueBlocks(epochUntil uint64) error {
	return rs.dequeueEpochRangeData(store.EpochBlock, epochUntil)
}

func (rs *redisStore) DequeueTransactions(epochUntil uint64) error {
	return rs.dequeueEpochRangeData(store.EpochTransaction, epochUntil)
}

func (rs *redisStore) DequeueLogs(epochUntil uint64) error {
	return rs.dequeueEpochRangeData(store.EpochLog, epochUntil)
}

func (rs *redisStore) Close() error {
	return rs.rdb.Close()
}

func (rs *redisStore) Flush() error {
	return rs.rdb.FlushDBAsync(rs.ctx).Err()
}

func (rs *redisStore) execWithTx(txConsumeFunc func(tx *redis.Tx) error, watchKeys ...string) error {
	for {
		err := rs.rdb.Watch(rs.ctx, func(tx *redis.Tx) error {
			return txConsumeFunc(tx)
		}, watchKeys...)

		if err == nil { // success
			return nil
		}

		if err == redis.TxFailedErr {
			// Optimistic lock lost, retry it
			continue
		}

		return err // any other error
	}
}

func (rs *redisStore) putOneWithTx(rp redis.Pipeliner, data *store.EpochData) (store.EpochDataOpAffects, error) {
	opHistory := store.EpochDataOpAffects{}

	// Epoch blocks & transactions hash collections
	epochBlocks := make([]interface{}, 0, len(data.Blocks))
	epochTxs := make([]interface{}, 0, len(data.Blocks)*2)

	for _, block := range data.Blocks {
		blockHash := block.Hash.String()
		epochBlocks = append(epochBlocks, blockHash)

		// Cache store block number mapping to block hash
		blockNo := block.BlockNumber.ToInt().Uint64()
		blockNo2HashCacheKey := getBlockNumber2HashCacheKey(blockNo)
		if err := rp.Set(rs.ctx, blockNo2HashCacheKey, blockHash, redisCacheExpireDuration).Err(); err != nil {
			return opHistory, err
		}

		// Cache store block summary
		blockSummary := util.GetSummaryOfBlock(block)
		blockRaw := util.MustMarshalRLP(blockSummary)

		blockCacheKey := getBlockCacheKey(block.Hash)
		if err := rp.Set(rs.ctx, blockCacheKey, blockRaw, redisCacheExpireDuration).Err(); err != nil {
			return opHistory, err
		}

		opHistory[store.EpochBlock]++

		for _, btx := range block.Transactions {
			receipt := data.Receipts[btx.Hash]
			// Skip transactions that unexecuted in block.
			// !!! Still need to check BlockHash and Status in case more than one transactions
			// of the same hash appeared in the same epoch.
			if receipt == nil || btx.BlockHash == nil || btx.Status == nil {
				continue
			}

			epochTxs = append(epochTxs, btx.Hash.String())

			// Cache store transactions
			txRaw := util.MustMarshalRLP(btx)
			txCacheKey := getTxCacheKey(btx.Hash)
			if err := rp.Set(rs.ctx, txCacheKey, txRaw, redisCacheExpireDuration).Err(); err != nil {
				return opHistory, err
			}

			// Cache store transaction receipts
			receiptRaw := util.MustMarshalRLP(receipt)
			receiptCacheKey := getTxReceiptCacheKey(btx.Hash)
			if err := rp.Set(rs.ctx, receiptCacheKey, receiptRaw, redisCacheExpireDuration).Err(); err != nil {
				return opHistory, err
			}

			opHistory[store.EpochTransaction]++

			// TODO cache store transaction receipt logs
		}
	}

	// Cache store epoch blocks mapping
	epbCacheKey := getEpochBlocksCacheKey(data.Number)
	if err := rp.RPush(rs.ctx, epbCacheKey, epochBlocks...).Err(); err != nil {
		return opHistory, err
	}

	if len(epochTxs) == 0 {
		return opHistory, nil
	}

	// Cache store epoch transactions mapping
	eptCacheKey := getEpochTxsCacheKey(data.Number)
	return opHistory, rp.RPush(rs.ctx, eptCacheKey, epochTxs...).Err()
}

func (rs *redisStore) remove(epochFrom, epochTo uint64, option store.EpochRemoveOption, rmOpType store.EpochOpType) error {
	if epochFrom > epochTo {
		return errors.Errorf("invalid epoch range (%v,%v) to remove from redis", epochFrom, epochTo)
	}

	updater := metrics.NewTimerUpdaterByName("infura/duration/store/redis/delete")
	defer updater.Update()

	numEpochs := epochTo - epochFrom + 1
	watchKeys := make([]string, 0, numEpochs)

	for i := epochFrom; i <= epochTo; i++ {
		watchKeys = append(watchKeys, getEpochBlocksCacheKey(i))
	}

	removeOpHistory := store.EpochDataOpAffects{}

	return rs.execWithTx(func(tx *redis.Tx) error {
		unlinkKeys := make([]string, 0, 100)

		for i := epochFrom; i <= epochTo; i++ {
			opHistory := store.EpochDataOpAffects{}

			epochNo := i
			if rmOpType == store.EpochOpPop { // pop from back to front
				epochNo = epochFrom + (epochTo - i)
			}

			epbCacheKey := getEpochBlocksCacheKey(epochNo)
			ebtCacheKey := getEpochTxsCacheKey(epochNo)

			// Remove blocks
			if option&store.EpochRemoveBlock != 0 {
				// Load epoch blocks mapping
				blockHashes, err := loadEpochBlocks(rs.ctx, tx, epochNo)
				if err != nil {
					return err
				}

				cacheKeys := make([]string, 0, len(blockHashes)+1)
				for _, blockHash := range blockHashes {
					cacheKeys = append(cacheKeys, getBlockCacheKey(blockHash))
				}

				opHistory[store.EpochBlock] = int64(-len(blockHashes))
				cacheKeys = append(cacheKeys, epbCacheKey)

				unlinkKeys = append(unlinkKeys, cacheKeys...)
			}

			// Remove transactions
			if option&store.EpochRemoveTransaction != 0 {
				// Load epoch transactions mapping
				txHashes, err := loadEpochTxs(rs.ctx, tx, epochNo, 0, -1)
				if err != nil {
					return err
				}

				cacheKeys := make([]string, 0, len(txHashes)*2+1)
				for _, txHash := range txHashes {
					cacheKeys = append(cacheKeys, getTxCacheKey(txHash), getTxReceiptCacheKey(txHash))
				}

				opHistory[store.EpochTransaction] = int64(-len(txHashes))
				cacheKeys = append(cacheKeys, ebtCacheKey)

				unlinkKeys = append(unlinkKeys, cacheKeys...)
			}

			// TODO remove logs

			removeOpHistory.Merge(opHistory)
		}

		// Operation is commited only if the watched keys remain unchanged.
		_, err := tx.TxPipelined(rs.ctx, func(pipe redis.Pipeliner) error {
			// unlink epoch cache keys
			if err := pipe.Unlink(rs.ctx, unlinkKeys...).Err(); err != nil {
				return err
			}

			// update epoch data count
			if err := rs.updateEpochDataCount(pipe, removeOpHistory); err != nil {
				return err
			}

			if rmOpType == store.EpochOpPop {
				// update max of epoch range for pop operation
				return rs.updateEpochRangeMax(pipe, epochFrom-1)
			}

			var edt store.EpochDataType
			switch rmOpType {
			case store.EpochOpDequeueBlock:
				edt = store.EpochBlock
			case store.EpochOpDequeueTx:
				edt = store.EpochTransaction
			case store.EpochOpDequeueLog:
				edt = store.EpochLog
			default:
				logrus.WithField("removeOperationType", rmOpType).Fatal("Invalid remove operation type for redis store")
			}

			// update min of epoch range for dequeue operation
			return rs.updateEpochRangeMin(pipe, epochTo+1, edt)
		})

		if err != nil {
			logrus.WithFields(logrus.Fields{
				"epochFrom": epochFrom, "epochTo": epochTo,
				"rmOption": option, "rmOpType": rmOpType,
			}).WithError(err).Debug("Failed to remove epoch data from reids store with pipeline unlink")
		}

		return err

	}, watchKeys...)
}

func (rs *redisStore) dequeueEpochRangeData(rt store.EpochDataType, epochUntil uint64) error {
	epochFrom, _, err := loadEpochRange(rs.ctx, rs.rdb, rt)
	if err != nil {
		return errors.WithMessage(err, "Failed to get epoch range")
	}

	if epochUntil < epochFrom {
		return nil
	}

	rmOpt := store.EpochDataTypeRemoveOptionMap[rt]
	dqOpt := store.EpochDataTypeDequeueOptionMap[rt]
	return rs.remove(epochFrom, epochUntil, rmOpt, dqOpt)
}

func (rs *redisStore) updateEpochRangeMax(rp redis.Pipeliner, epochNo uint64) error {
	cacheKey := getMetaCacheKey("epoch.ranges")
	batchKVTo := make([]interface{}, 0, 4*2)
	batchKFrom := make([]string, 0, 4)

	opEpochDataTypes := append([]store.EpochDataType{}, store.OpEpochDataTypes...)
	opEpochDataTypes = append(opEpochDataTypes, store.EpochDataNil)
	for _, rt := range opEpochDataTypes {
		if rt == store.EpochLog { // TODO add event logs support
			continue
		}
		fieldFrom, fieldTo := getMetaEpochRangeField(rt)
		batchKVTo = append(batchKVTo, fieldTo, epochNo)
		batchKFrom = append(batchKFrom, fieldFrom)
	}

	// Batch update max of epoch range
	if _, err := rp.HSet(rs.ctx, cacheKey, batchKVTo...).Result(); err != nil {
		logrus.WithField("batchFieldValues", batchKVTo).WithError(err).Error("Failed to update max of epoch range to cache store")
		return errors.WithMessage(err, "failed to update max of epoch range to cache store")
	}

	// Also update min of epoch range if field not set yet.
	for _, fieldFrom := range batchKFrom {
		if err := rp.HSetNX(rs.ctx, cacheKey, fieldFrom, epochNo).Err(); err != nil {
			logrus.WithField("field", fieldFrom).WithError(err).Error("Failed to update min of epoch range to cache store")
			return errors.WithMessage(err, "failed to update min of epoch range to cache store")
		}
	}

	return nil
}

func (rs *redisStore) updateEpochRangeMin(rp redis.Pipeliner, epochNo uint64, rt store.EpochDataType) error {
	cacheKey := getMetaCacheKey("epoch.ranges")

	// Update min epoch for local epoch range
	fieldFrom, _ := getMetaEpochRangeField(rt)
	_, err := rp.HSet(rs.ctx, cacheKey, fieldFrom, epochNo).Result()
	if err != nil {
		logrus.WithField("epochDataType", rt).WithError(err).Error("Failed to update min of epoch range")
		return err
	}

	gFieldFrom, _ := getMetaEpochRangeField(store.EpochDataNil)
	luaKeys := make([]string, 0, len(store.OpEpochDataTypes)+1)
	luaKeys = append(luaKeys, cacheKey)

	for _, rt := range store.OpEpochDataTypes {
		ff, _ := getMetaEpochRangeField(rt)
		luaKeys = append(luaKeys, ff)
	}

	// It's not possible to get key value from redis inside a multi transaction.
	// So we need to update min of global epoch range in lua script to make it atomic.
	// We can run the following command in redis concole to test the lua script:
	// eval "local a,b=redis.call('HMGET',unpack(KEYS)),nil;for c,d in pairs(a)do local e=tonumber(d)if b==nil or e~=nil and b>e then b=e end end;if b~=nil then redis.call('HSET',KEYS[1],ARGV[1],b)redis.log(redis.LOG_WARNING,'update min of epoch range (key='..KEYS[1]..',field='..ARGV[1]..',value='..b..')')end" 4 "metadata:epoch.ranges" "block.epoch.from" "tx.epoch.from" "log.epoch.from" "epoch.from"
	luaScript := `
local vals, minv = redis.call('HMGET', unpack(KEYS)), nil

for k,v in pairs(vals) 
do
	local nv = tonumber(v)
	if minv == nil or (nv ~= nil and minv > nv) 
	then
		minv = nv
	end
end

if minv ~= nil
then
	redis.call('HSET', KEYS[1], ARGV[1], minv)
	redis.log(redis.LOG_DEBUG, 'update min of epoch range (key='..KEYS[1]..',field='..ARGV[1]..',value='..minv..')')
end

return true
`
	if err := rp.Eval(rs.ctx, luaScript, luaKeys, gFieldFrom).Err(); err != nil {
		logrus.WithError(err).Error("Failed to execute lua script to update min of global epoch range")
		return err
	}

	return nil
}

func (rs *redisStore) updateEpochDataCount(rp redis.Pipeliner, opHistory store.EpochDataOpAffects) error {
	// Update epoch totals
	for k, v := range opHistory {
		if v == 0 {
			continue
		}

		if _, err := incrEpochDataCount(rs.ctx, rp, k, v); err != nil {
			logrus.WithField("epochDataType", k).WithError(err).Error("Failed to update epoch data count")
			return err
		}
	}

	return nil
}
