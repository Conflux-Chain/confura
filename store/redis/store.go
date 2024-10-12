package redis

import (
	"context"
	"time"

	"github.com/Conflux-Chain/confura/store"
	citypes "github.com/Conflux-Chain/confura/types"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	_ store.Store = (*RedisStore)(nil) // ensure RedisStore implements Store interface
)

type RedisStoreConfig struct {
	Enabled   bool
	CacheTime time.Duration `default:"12h"`
	Url       string
}

// RedisStore aggregation store to cache block/txn/receipt data in Redis.
// Be noted this would be deprecated in the next few release due to chain data are usually oversized
// (with some of them even more than several MB). It is totally inefficient for Redis to cache big key
// under such circumstance.
type RedisStore struct {
	ctx       context.Context
	rdb       *redis.Client
	cacheTime time.Duration
	disabler  store.ChainDataDisabler
}

func MustNewRedisStoreFromViper(disabler store.ChainDataDisabler) (*RedisStore, bool) {
	var rsconf RedisStoreConfig
	viper.MustUnmarshalKey("store.redis", &rsconf)

	if !rsconf.Enabled {
		return nil, false
	}

	logrus.WithField("config", rsconf).Debug("Creating redis store from viper config")

	rdb := MustNewRedisClient(rsconf.Url)
	ctx := context.Background()

	return &RedisStore{rdb: rdb, ctx: ctx, cacheTime: rsconf.CacheTime, disabler: disabler}, true
}

func MustNewRedisClient(url string) *redis.Client {
	opt, err := redis.ParseURL(url)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to parse redis url")
	}

	client := redis.NewClient(opt)

	// Test connection
	if _, err := client.Ping(context.Background()).Result(); err != nil {
		logrus.WithError(err).Fatal("Failed to ping redis")
	}

	return client
}

func (rs *RedisStore) IsRecordNotFound(err error) bool {
	return errors.Is(err, redis.Nil) || errors.Is(err, store.ErrNotFound)
}

func (rs *RedisStore) GetBlockEpochRange() (uint64, uint64, error) {
	return loadEpochRange(rs.ctx, rs.rdb, store.EpochBlock)
}

func (rs *RedisStore) GetTransactionEpochRange() (uint64, uint64, error) {
	return loadEpochRange(rs.ctx, rs.rdb, store.EpochTransaction)
}

func (rs *RedisStore) GetLogEpochRange() (uint64, uint64, error) {
	return loadEpochRange(rs.ctx, rs.rdb, store.EpochLog)
}

func (rs *RedisStore) GetGlobalEpochRange() (uint64, uint64, error) {
	return loadEpochRange(rs.ctx, rs.rdb, store.EpochDataNil)
}

func (rs *RedisStore) GetNumBlocks() (uint64, error) {
	cnt, err := loadEpochDataCount(rs.ctx, rs.rdb, store.EpochBlock)
	if err != nil && !rs.IsRecordNotFound(err) {
		return 0, err
	}

	return cnt, nil
}

func (rs *RedisStore) GetNumTransactions() (uint64, error) {
	cnt, err := loadEpochDataCount(rs.ctx, rs.rdb, store.EpochTransaction)
	if err != nil && !rs.IsRecordNotFound(err) {
		return 0, err
	}

	return cnt, nil
}

func (rs *RedisStore) GetNumLogs() (uint64, error) {
	cnt, err := loadEpochDataCount(rs.ctx, rs.rdb, store.EpochLog)
	if err != nil && !rs.IsRecordNotFound(err) {
		return 0, err
	}

	return cnt, nil
}

func (rs *RedisStore) GetLogs(ctx context.Context, filter store.LogFilter) (logs []*store.Log, err error) {
	// It's impractical to cache && index event logs in Redis.
	return nil, store.ErrUnsupported
}

func (rs *RedisStore) GetTransaction(ctx context.Context, txHash types.Hash) (*store.Transaction, error) {
	tx, err := loadTx(rs.ctx, rs.rdb, txHash)
	if err != nil {
		return nil, err
	}

	// TODO: return extention field
	return &store.Transaction{CfxTransaction: tx}, nil
}

func (rs *RedisStore) GetReceipt(ctx context.Context, txHash types.Hash) (*store.TransactionReceipt, error) {
	receipt, err := loadTxReceipt(rs.ctx, rs.rdb, txHash)
	if err != nil {
		return nil, err
	}

	// TODO: return extention field
	return &store.TransactionReceipt{CfxReceipt: receipt}, nil
}

func (rs *RedisStore) GetBlocksByEpoch(ctx context.Context, epochNumber uint64) ([]types.Hash, error) {
	return loadEpochBlocks(rs.ctx, rs.rdb, epochNumber)
}

func (rs *RedisStore) GetBlockByEpoch(ctx context.Context, epochNumber uint64) (*store.Block, error) {
	// TODO Cannot get tx from redis in advance, since only executed txs are saved in store
	return nil, store.ErrUnsupported
}

func (rs *RedisStore) GetBlockSummaryByEpoch(ctx context.Context, epochNumber uint64) (*store.BlockSummary, error) {
	pivotBlock, err := loadEpochPivotBlock(rs.ctx, rs.rdb, epochNumber)
	if err != nil {
		logrus.WithField("epochNumber", epochNumber).WithError(err).Debug("Pivot block missed in cache")
		return nil, errors.WithMessage(err, "failed to load epoch pivot block")
	}

	blocksum, err := loadBlockSummaryByHash(rs.ctx, rs.rdb, pivotBlock)
	if err != nil {
		return nil, err
	}

	// TODO: return extention field
	return &store.BlockSummary{CfxBlockSummary: blocksum}, nil
}

func (rs *RedisStore) GetBlockByHash(ctx context.Context, blockHash types.Hash) (*store.Block, error) {
	return nil, store.ErrUnsupported
}

func (rs *RedisStore) GetBlockSummaryByHash(ctx context.Context, blockHash types.Hash) (*store.BlockSummary, error) {
	blocksum, err := loadBlockSummaryByHash(rs.ctx, rs.rdb, blockHash)
	if err != nil {
		return nil, err
	}

	// TODO: return extention field
	return &store.BlockSummary{CfxBlockSummary: blocksum}, nil
}

func (rs *RedisStore) GetBlockByBlockNumber(ctx context.Context, blockNumber uint64) (*store.Block, error) {
	return nil, store.ErrUnsupported
}

func (rs *RedisStore) GetBlockSummaryByBlockNumber(ctx context.Context, blockNumber uint64) (*store.BlockSummary, error) {
	blocksum, err := loadBlockSummaryByNumber(rs.ctx, rs.rdb, blockNumber)
	if err != nil {
		return nil, err
	}

	// TODO: return extention field
	return &store.BlockSummary{CfxBlockSummary: blocksum}, nil
}

func (rs *RedisStore) Push(data *store.EpochData) error {
	return rs.Pushn([]*store.EpochData{data})
}

func (rs *RedisStore) Pushn(dataSlice []*store.EpochData) error {
	if len(dataSlice) == 0 {
		return nil
	}

	startTime := time.Now()
	defer metrics.Registry.Store.Push("redis").UpdateSince(startTime)

	_, lastEpoch, err := rs.GetGlobalEpochRange()
	if rs.IsRecordNotFound(err) {
		// epoch range not found in redis
		lastEpoch = citypes.EpochNumberNil
	} else if err != nil {
		return errors.WithMessage(err, "failed to get global epoch range from redis")
	}

	// ensure continous epoch
	if err := store.RequireContinuous(dataSlice, lastEpoch); err != nil {
		return err
	}

	watchKeys := make([]string, 0, len(dataSlice))
	for _, data := range dataSlice {
		watchKeys = append(watchKeys, getEpochBlocksCacheKey(data.Number))
	}

	return rs.execWithTx(func(tx *redis.Tx) error {
		txOpHistory := store.EpochDataOpAffects{NumAlters: store.EpochDataOpNumAlters{}}

		// Operation is commited only if the watched keys remain unchanged.
		_, err = tx.TxPipelined(rs.ctx, func(pipe redis.Pipeliner) error {
			for _, data := range dataSlice {
				opHistory, err := rs.putOneWithTx(pipe, data)
				if err != nil {
					return err
				}

				txOpHistory.Merge(opHistory)
			}

			logrus.WithField("opHistory", txOpHistory).Debug("Pushn redis operation history")

			// update epoch data count
			if err := rs.updateEpochDataCount(pipe, txOpHistory.NumAlters); err != nil {
				return errors.WithMessage(err, "failed to update statistic on push")
			}

			// update max of epoch range
			if err := rs.updateEpochRangeMax(pipe, lastEpoch, dataSlice[0].Number); err != nil {
				return errors.WithMessage(err, "failed to update epoch range on push")
			}

			return nil
		})

		if err != nil {
			logrus.WithField("lastEpoch", lastEpoch).WithError(err).Debug(
				"Failed to push epoch data to reids store with pipeline",
			)
		}

		return err

	}, watchKeys...)
}

func (rs *RedisStore) Pop() error {
	_, maxEpoch, err := rs.GetGlobalEpochRange()
	if err != nil {
		return errors.WithMessage(err, "failed to get global epoch range")
	}

	return rs.Popn(maxEpoch)
}

// Popn pops multiple epoch data from redis.
func (rs *RedisStore) Popn(epochUntil uint64) error {
	_, maxEpoch, err := rs.GetGlobalEpochRange()
	if err != nil {
		return errors.WithMessage(err, "failed to get global epoch range")
	}

	if epochUntil > maxEpoch {
		return nil
	}

	err = rs.remove(epochUntil, maxEpoch, store.EpochRemoveAll, store.EpochOpPop)

	logrus.WithFields(logrus.Fields{
		"epochUntil": epochUntil, "stackMaxEpoch": maxEpoch,
	}).WithError(err).Info("Popn operation from redis store")

	return err
}

func (rs *RedisStore) DequeueBlocks(epochUntil uint64) error {
	return rs.dequeueEpochRangeData(store.EpochBlock, epochUntil)
}

func (rs *RedisStore) DequeueTransactions(epochUntil uint64) error {
	return rs.dequeueEpochRangeData(store.EpochTransaction, epochUntil)
}

func (rs *RedisStore) DequeueLogs(epochUntil uint64) error {
	return rs.dequeueEpochRangeData(store.EpochLog, epochUntil)
}

func (rs *RedisStore) Close() error {
	return rs.rdb.Close()
}

func (rs *RedisStore) Flush() error {
	return rs.rdb.FlushDBAsync(rs.ctx).Err()
}

func (rs *RedisStore) LoadConfig(confNames ...string) (map[string]interface{}, error) {
	iSlice, err := rs.rdb.HMGet(rs.ctx, "conf", confNames...).Result()
	if err != nil {
		return nil, err
	}

	res := make(map[string]interface{}, len(iSlice))
	for i, v := range iSlice {
		sv, ok := v.(string)
		if !ok { // field not found
			continue
		}

		res[confNames[i]] = sv
	}

	return res, nil
}

func (rs *RedisStore) StoreConfig(confName string, confVal interface{}) error {
	return rs.rdb.HSet(rs.ctx, "conf", confName, confVal).Err()
}

func (rs *RedisStore) execWithTx(txConsumeFunc func(tx *redis.Tx) error, watchKeys ...string) error {
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

// TODO: store extention json field.
func (rs *RedisStore) putOneWithTx(rp redis.Pipeliner, data *store.EpochData) (store.EpochDataOpNumAlters, error) {
	opHistory := store.EpochDataOpNumAlters{}

	// Epoch blocks & transactions hash collections
	epochBlocks := make([]interface{}, 0, len(data.Blocks))
	epochTxs := make([]interface{}, 0, len(data.Blocks)*2)

	for _, block := range data.Blocks {
		blockHash := block.Hash.String()
		epochBlocks = append(epochBlocks, blockHash)

		if !rs.disabler.IsChainBlockDisabled() {
			// Cache store block number mapping to block hash
			blockNo := block.BlockNumber.ToInt().Uint64()
			blockNo2HashCacheKey := getBlockNumber2HashCacheKey(blockNo)
			if err := rp.Set(rs.ctx, blockNo2HashCacheKey, blockHash, rs.cacheTime).Err(); err != nil {
				return opHistory, errors.WithMessage(err, "failed to cache block num to hash mapping")
			}

			// Cache store block summary
			blockSummary := util.GetSummaryOfBlock(block)
			blockRaw := util.MustMarshalRLP(blockSummary)

			blockCacheKey := getBlockCacheKey(block.Hash)
			if err := rp.Set(rs.ctx, blockCacheKey, blockRaw, rs.cacheTime).Err(); err != nil {
				return opHistory, errors.WithMessage(err, "failed to cache block summary")
			}

			opHistory[store.EpochBlock]++
		}

		for _, btx := range block.Transactions {
			receipt := data.Receipts[btx.Hash]
			// Skip transactions that unexecuted in block.
			// !!! Still need to check BlockHash and Status in case more than one transactions
			// of the same hash appeared in the same epoch.
			if receipt == nil || btx.BlockHash == nil || btx.Status == nil {
				continue
			}

			epochTxs = append(epochTxs, btx.Hash.String())

			skipTx := rs.disabler.IsChainTxnDisabled()
			skipRcpt := rs.disabler.IsChainReceiptDisabled()
			if !skipTx {
				// Cache store transactions
				txRaw := util.MustMarshalRLP(btx)
				txCacheKey := getTxCacheKey(btx.Hash)
				if err := rp.Set(rs.ctx, txCacheKey, txRaw, rs.cacheTime).Err(); err != nil {
					return opHistory, errors.WithMessage(err, "failed to cache transaction")
				}
			}

			if !skipRcpt {
				// Cache store transaction receipts
				receiptRaw := util.MustMarshalRLP(receipt)
				receiptCacheKey := getTxReceiptCacheKey(btx.Hash)
				if err := rp.Set(rs.ctx, receiptCacheKey, receiptRaw, rs.cacheTime).Err(); err != nil {
					return opHistory, errors.WithMessage(err, "failed to cache transaction receipt")
				}
			}

			if !skipTx || !skipRcpt {
				opHistory[store.EpochTransaction]++
			}

			// TODO store transaction receipt logs
		}
	}

	if !rs.disabler.IsChainBlockDisabled() {
		// Cache store epoch blocks mapping
		epbCacheKey := getEpochBlocksCacheKey(data.Number)
		if err := rp.RPush(rs.ctx, epbCacheKey, epochBlocks...).Err(); err != nil {
			return opHistory, errors.WithMessage(err, "failed to cache epoch to blocks mapping")
		}

		err := rp.Expire(rs.ctx, epbCacheKey, rs.cacheTime).Err()
		if err != nil {
			logrus.WithField("epbCacheKey", epbCacheKey).Info(
				"Failed to set expiration date for epoch to blocks mapping redis key",
			)
		}
	}

	if len(epochTxs) == 0 {
		return opHistory, nil
	}

	if !rs.disabler.IsChainTxnDisabled() || !rs.disabler.IsChainReceiptDisabled() {
		// Cache store epoch transactions mapping
		eptCacheKey := getEpochTxsCacheKey(data.Number)
		if err := rp.RPush(rs.ctx, eptCacheKey, epochTxs...).Err(); err != nil {
			return opHistory, errors.WithMessage(err, "failed to cache epoch to transactions mapping")
		}

		err := rp.Expire(rs.ctx, eptCacheKey, rs.cacheTime).Err()
		if err != nil {
			logrus.WithField("eptCacheKey", eptCacheKey).Info(
				"Failed to set expiration date for epoch to txs mapping redis key",
			)
		}
	}

	return opHistory, nil
}

func (rs *RedisStore) remove(epochFrom, epochTo uint64, option store.EpochRemoveOption, rmOpType store.EpochOpType) error {
	if epochFrom > epochTo {
		return errors.Errorf("invalid epoch range (%v,%v)", epochFrom, epochTo)
	}

	startTime := time.Now()
	defer metrics.Registry.Store.Pop("redis").UpdateSince(startTime)

	numEpochs := epochTo - epochFrom + 1
	watchKeys := make([]string, 0, numEpochs)

	for i := epochFrom; i <= epochTo; i++ {
		watchKeys = append(watchKeys, getEpochBlocksCacheKey(i))
	}

	removeOpHistory := store.EpochDataOpAffects{NumAlters: store.EpochDataOpNumAlters{}}

	return rs.execWithTx(func(tx *redis.Tx) error {
		unlinkKeys := make([]string, 0, 100)

		for i := epochFrom; i <= epochTo; i++ {
			opHistory := store.EpochDataOpNumAlters{}

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
				if err != nil && !rs.IsRecordNotFound(err) {
					return errors.WithMessage(err, "failed to load epoch to blocks mapping")
				}

				if err == nil {
					cacheKeys := make([]string, 0, len(blockHashes)+1)
					for _, blockHash := range blockHashes {
						cacheKeys = append(cacheKeys, getBlockCacheKey(blockHash))
					}

					opHistory[store.EpochBlock] = int64(-len(blockHashes))
					cacheKeys = append(cacheKeys, epbCacheKey)

					unlinkKeys = append(unlinkKeys, cacheKeys...)
				}
			}

			// Remove transactions
			if option&store.EpochRemoveTransaction != 0 {
				// Load epoch transactions mapping
				txHashes, err := loadEpochTxs(rs.ctx, tx, epochNo, 0, -1)
				if err != nil && !rs.IsRecordNotFound(err) {
					return errors.WithMessage(err, "failed to load epoch to txs mapping")
				}

				if err == nil {
					cacheKeys := make([]string, 0, len(txHashes)*2+1)
					for _, txHash := range txHashes {
						cacheKeys = append(cacheKeys, getTxCacheKey(txHash), getTxReceiptCacheKey(txHash))
					}

					opHistory[store.EpochTransaction] = int64(-len(txHashes))
					cacheKeys = append(cacheKeys, ebtCacheKey)

					unlinkKeys = append(unlinkKeys, cacheKeys...)
				}
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
			if err := rs.updateEpochDataCount(pipe, removeOpHistory.NumAlters); err != nil {
				return errors.WithMessage(err, "failed to update statistics on remove")
			}

			if rmOpType == store.EpochOpPop {
				// update max of epoch range for pop operation
				err := rs.updateEpochRangeMax(pipe, epochFrom-1)
				return errors.WithMessage(err, "failed to update epoch range on remove")
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
				logrus.
					WithField("removeOperationType", rmOpType).
					Fatal("Invalid remove operation type for redis store")
			}

			// update min of epoch range for dequeue operation
			return rs.updateEpochRangeMin(pipe, epochTo+1, edt)
		})

		if err != nil {
			logrus.WithFields(logrus.Fields{
				"epochFrom": epochFrom, "epochTo": epochTo,
				"rmOption": option, "rmOpType": rmOpType,
			}).WithError(err).Info(
				"Failed to remove epoch data from reids store with pipeline unlink",
			)
		}

		return err

	}, watchKeys...)
}

func (rs *RedisStore) dequeueEpochRangeData(rt store.EpochDataType, epochUntil uint64) error {
	epochFrom, _, err := loadEpochRange(rs.ctx, rs.rdb, rt)
	if err != nil {
		return errors.WithMessage(err, "failed to get epoch range")
	}

	if epochUntil < epochFrom {
		return nil
	}

	return rs.remove(epochFrom, epochUntil, rt.ToRemoveOption(), rt.ToDequeOption())
}

func (rs *RedisStore) updateEpochRangeMax(rp redis.Pipeliner, epochNo uint64, growFrom ...uint64) error {
	cacheKey := getMetaCacheKey("epoch.ranges")
	batchKVTo := make([]interface{}, 0, 4*2)
	batchKFrom := make([]string, 0, 4)

	opEpochDataTypes := append([]store.EpochDataType{}, store.OpEpochDataTypes...)
	opEpochDataTypes = append(opEpochDataTypes, store.EpochDataNil)
	for _, rt := range opEpochDataTypes {
		if rs.disabler.IsDisabledForType(rt) {
			continue
		}

		// TODO add event logs support
		if rt == store.EpochLog {
			continue
		}
		fieldFrom, fieldTo := getMetaEpochRangeField(rt)
		batchKVTo = append(batchKVTo, fieldTo, epochNo)
		batchKFrom = append(batchKFrom, fieldFrom)
	}

	// Batch update max of epoch range
	if _, err := rp.HSet(rs.ctx, cacheKey, batchKVTo...).Result(); err != nil {
		logrus.
			WithField("batchFieldValues", batchKVTo).
			WithError(err).Error("Failed to update max of epoch range to cache store")

		return errors.WithMessage(err, "failed to update max of epoch range")
	}

	if len(growFrom) == 0 {
		return nil
	}

	// Also update min of epoch range if field not set yet.
	for _, fieldFrom := range batchKFrom {
		if err := rp.HSetNX(rs.ctx, cacheKey, fieldFrom, growFrom[0]).Err(); err != nil {
			logrus.
				WithField("field", fieldFrom).
				WithError(err).Error("Failed to update min of epoch range to cache store")
			return errors.WithMessage(err, "failed to update min of epoch range")
		}
	}

	if err := rs.refreshEpochRangeExpr(); err != nil {
		logrus.WithError(err).Error("Failed to refresh epoch range cache key expiration when max updated")
	}

	return nil
}

func (rs *RedisStore) updateEpochRangeMin(rp redis.Pipeliner, epochNo uint64, rt store.EpochDataType) error {
	cacheKey := getMetaCacheKey("epoch.ranges")

	// Update min epoch for local epoch range
	fieldFrom, _ := getMetaEpochRangeField(rt)
	_, err := rp.HSet(rs.ctx, cacheKey, fieldFrom, epochNo).Result()
	if err != nil {
		logrus.
			WithField("epochDataType", rt).
			WithError(err).Error("Failed to update min of epoch range to cache store")
		return errors.WithMessage(err, "failed to update min of epoch range")
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
	// 	eval "local a,b=redis.call('HMGET',unpack(KEYS)),nil;for c,d in pairs(a)do \
	// 		local e=tonumber(d)if b==nil or e~=nil and b>e then b=e end end;if b~=nil \
	//		then redis.call('HSET',KEYS[1],ARGV[1],b)redis.log(redis.LOG_WARNING, \
	//		'update min of epoch range (key='..KEYS[1]..',field='..ARGV[1]..',value='..b..')')end" 4 \
	//		"metadata:epoch.ranges" "block.epoch.from" "tx.epoch.from" "log.epoch.from" "epoch.from"
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
		return errors.WithMessage(err, "failed to exec lua to update min of epoch range")
	}

	if err := rs.refreshEpochRangeExpr(); err != nil {
		logrus.WithError(err).Error("Failed to refresh epoch range cache key expiration when min updated")
	}

	return nil
}

func (rs *RedisStore) updateEpochDataCount(rp redis.Pipeliner, opHistory store.EpochDataOpNumAlters) error {
	refreshExpr := false

	// Update epoch totals
	for k, v := range opHistory {
		if v == 0 {
			continue
		}

		if _, err := incrEpochDataCount(rs.ctx, rp, k, v); err != nil {
			logrus.WithFields(logrus.Fields{
				"epochDataType": k,
				"incrCount":     v,
			}).WithError(err).Error("Failed to update epoch data count statistics")
			return errors.WithMessagef(err, "failed to incr count stats key %v with %v", k, v)
		}

		refreshExpr = true
	}

	if refreshExpr {
		err := rs.refreshEpochStatsExpr()
		if err != nil {
			logrus.WithError(err).Error("Failed to refresh epoch statistics cache key expiration")
		}
	}

	return nil
}

func (rs *RedisStore) refreshEpochStatsExpr() error {
	cacheKey := getMetaCacheKey("epoch.statistics")
	return rs.rdb.Expire(rs.ctx, cacheKey, rs.cacheTime).Err()
}

func (rs *RedisStore) refreshEpochRangeExpr() error {
	cacheKey := getMetaCacheKey("epoch.ranges")
	return rs.rdb.Expire(rs.ctx, cacheKey, rs.cacheTime).Err()
}
