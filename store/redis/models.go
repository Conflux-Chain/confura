package redis

import (
	"context"
	"strconv"
	"strings"

	"github.com/Conflux-Chain/confura/store"
	citypes "github.com/Conflux-Chain/confura/types"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

// RedisKey returns a unified redis key sperated by colon
func RedisKey(keyParts ...string) string {
	return strings.Join(keyParts, ":")
}

// Unmarshal RLP raw data read from redis by key
// key: redis key
// v: value for unmarshal, e.g. types.transaction, and use pointer type
func RedisRLPGet(ctx context.Context, rc redis.Cmdable, key string, pVal interface{}) error {
	strBytes, err := rc.Get(ctx, key).Result()

	err = ParseRedisNil(err)
	if err == nil {
		util.MustUnmarshalRLP([]byte(strBytes), pVal)
	}

	return err
}

func StrUint64(v uint64) string {
	return strconv.FormatUint(v, 10)
}

func ParseRedisNil(err error) error {
	if err == redis.Nil {
		return store.ErrNotFound
	}

	return err
}

// Block summary data stored in redis formated as:
// key:		block:{block_hash}
// value: 	RLP encoded types.BlockSummary raw bytes
func getBlockCacheKey(blockHash types.Hash) string {
	return RedisKey("block", blockHash.String())
}

// Transaction data stored in redis formated as:
// key: 	tx:{transaction_hash}
// value: 	RLP encoded types.Transaction raw bytes
func getTxCacheKey(txHash types.Hash) string {
	return RedisKey("tx", txHash.String())
}

// Transaction receipt data stored in redis formated as:
// key: 	receipt:{transaction_hash}
// value: 	RLP encoded types.TransactionReceipt raw bytes
func getTxReceiptCacheKey(txHash types.Hash) string {
	return RedisKey("receipt", txHash.String())
}

// Epoch to block hash collection stored in redis formated as:
// key: 	epoch:{epoch_number}:blocks
// value: 	List of ordered block hashes
func getEpochBlocksCacheKey(epochNo uint64) string {
	return RedisKey("epoch", StrUint64(epochNo), "blocks")
}

// Block number to block hash mapping stored in redis formated as:
// key: 	block#:{block_number}
// value: 	{block_hash}
func getBlockNumber2HashCacheKey(blockNo uint64) string {
	return RedisKey("block#", StrUint64(blockNo))
}

// Epoch to transaction hash collection stored in redis formated as:
// key: 	epoch:{epoch_number}:txs
// value: 	List of transaction hashes
// This can be used to handle epoch transactions indexed by epoch number like epoch data prunning
func getEpochTxsCacheKey(epochNo uint64) string {
	return RedisKey("epoch", StrUint64(epochNo), "txs")
}

// Metadata stored in redis formated as:
// keys: 	metadata:{keys}
// value: 	Variadic data type by key
func getMetaCacheKey(keys ...string) string {
	newKeys := append([]string{"metadata"}, keys...)
	return RedisKey(newKeys...)
}

// Load block summary data by block hash from redis
func loadBlockSummaryByHash(ctx context.Context, rc redis.Cmdable, blockHash types.Hash) (*types.BlockSummary, error) {
	var rpcBlock types.BlockSummary
	err := RedisRLPGet(ctx, rc, getBlockCacheKey(blockHash), &rpcBlock)

	return &rpcBlock, err
}

// Load block summary data by block number from redis
func loadBlockSummaryByNumber(ctx context.Context, rc redis.Cmdable, blockNo uint64) (*types.BlockSummary, error) {
	num2HashCacheKey := getBlockNumber2HashCacheKey(blockNo)

	blockHash, err := rc.Get(ctx, num2HashCacheKey).Result()
	if err = ParseRedisNil(err); err == nil {
		return loadBlockSummaryByHash(ctx, rc, types.Hash(blockHash))
	}

	return nil, err
}

// Load transaction data by transaction hash from redis
func loadTx(ctx context.Context, rc redis.Cmdable, txHash types.Hash) (*types.Transaction, error) {
	var rpcTx types.Transaction
	err := RedisRLPGet(ctx, rc, getTxCacheKey(txHash), &rpcTx)

	return &rpcTx, err
}

// Load transaction receipt data by transaction hash from redis
func loadTxReceipt(ctx context.Context, rc redis.Cmdable, txHash types.Hash) (*types.TransactionReceipt, error) {
	var rpcTxReceipt types.TransactionReceipt
	err := RedisRLPGet(ctx, rc, getTxReceiptCacheKey(txHash), &rpcTxReceipt)

	return &rpcTxReceipt, err
}

// Load epoch block hash collections by epoch number from redis
func loadEpochBlocks(ctx context.Context, rc redis.Cmdable, epochNo uint64) ([]types.Hash, error) {
	return loadEpochBlocksByRange(ctx, rc, epochNo, 0, -1)
}

// Load epoch block hash collections by epoch number from redis
func loadEpochPivotBlock(ctx context.Context, rc redis.Cmdable, epochNo uint64) (types.Hash, error) {
	var emptyHash types.Hash

	epochBlocks, err := loadEpochBlocksByRange(ctx, rc, epochNo, -1, -1)
	if err != nil {
		return emptyHash, errors.WithMessage(err, "failed to get epoch blocks")
	}

	return epochBlocks[0], nil
}

// Load epoch block hash collections by epoch number with range from redis
func loadEpochBlocksByRange(ctx context.Context, rc redis.Cmdable, epochNo uint64, rangeStart, rangeEnd int64) ([]types.Hash, error) {
	cacheKey := getEpochBlocksCacheKey(epochNo)

	strSlice, err := rc.LRange(ctx, cacheKey, rangeStart, rangeEnd).Result()
	if err != nil {
		return nil, err
	}

	if len(strSlice) == 0 { // key not exists since each epoch has at least 1 block (pivot block)
		return nil, store.ErrNotFound
	}

	ret := make([]types.Hash, 0, len(strSlice))
	for _, str := range strSlice {
		ret = append(ret, types.Hash(str))
	}

	return ret, nil
}

// Load epoch transaction hash collections by epoch number from redis
func loadEpochTxs(ctx context.Context, rc redis.Cmdable, epochNo uint64, rangeStart, rangeEnd int64) ([]types.Hash, error) {
	cacheKey := getEpochTxsCacheKey(epochNo)

	strSlice, err := rc.LRange(ctx, cacheKey, rangeStart, rangeEnd).Result()
	if err != nil {
		return nil, err
	}

	ret := make([]types.Hash, 0, len(strSlice))
	for _, str := range strSlice {
		ret = append(ret, types.Hash(str))
	}

	return ret, nil
}

func getMetaEpochRangeField(dt store.EpochDataType) (string, string) {
	var fromKey, toKey string

	switch dt {
	case store.EpochBlock:
		fromKey, toKey = "block.epoch.from", "block.epoch.to"
	case store.EpochTransaction:
		fromKey, toKey = "tx.epoch.from", "tx.epoch.to"
	case store.EpochLog:
		fromKey, toKey = "log.epoch.from", "log.epoch.to"
	default:
		fromKey, toKey = "epoch.from", "epoch.to"
	}

	return fromKey, toKey
}

func loadEpochRange(ctx context.Context, rc redis.Cmdable, dt store.EpochDataType) (uint64, uint64, error) {
	fromField, toField := getMetaEpochRangeField(dt)
	iSlice, err := rc.HMGet(ctx, getMetaCacheKey("epoch.ranges"), fromField, toField).Result()

	if err != nil {
		return 0, 0, err
	}

	epochRanges := [2]uint64{citypes.EpochNumberNil, citypes.EpochNumberNil}
	for i, v := range iSlice {
		sv, ok := v.(string)
		if !ok { // field not found
			continue
		}

		if ev, err := strconv.ParseUint(sv, 10, 64); err == nil {
			epochRanges[i] = ev
		}
	}

	epochRanges[0] = min(epochRanges[0], epochRanges[1])
	if epochRanges[0] == citypes.EpochNumberNil { // both epoch number are uninitialized
		return 0, 0, store.ErrNotFound
	}

	if epochRanges[1] == citypes.EpochNumberNil { // align both end when upper end not set
		epochRanges[1] = epochRanges[0]
	}

	return epochRanges[0], epochRanges[1], nil
}

func getMetaEpochCountField(dt store.EpochDataType) string {
	var key string

	switch dt {
	case store.EpochBlock:
		key = "num.blocks"
	case store.EpochTransaction:
		key = "num.txs"
	case store.EpochLog:
		key = "num.logs"
	default:
		key = "num.epochs"
	}

	return key
}

func loadEpochDataCount(ctx context.Context, rc redis.Cmdable, dt store.EpochDataType) (uint64, error) {
	field := getMetaEpochCountField(dt)
	strBytes, err := rc.HGet(ctx, getMetaCacheKey("epoch.statistics"), field).Result()

	err = ParseRedisNil(err)
	if err != nil {
		return 0, err
	}

	ev, _ := strconv.ParseUint(strBytes, 10, 64)
	return ev, nil
}

func incrEpochDataCount(ctx context.Context, rc redis.Cmdable, dt store.EpochDataType, cnt int64) (int64, error) {
	field := getMetaEpochCountField(dt)

	newCnt, err := rc.HIncrBy(ctx, getMetaCacheKey("epoch.statistics"), field, cnt).Result()
	return newCnt, err
}
