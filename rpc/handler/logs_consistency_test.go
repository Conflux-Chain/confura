package handler

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	web3types "github.com/openweb3/web3go/types"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type mixedEthLogsClient struct{ *fakeEthScanClient }

func (*mixedEthLogsClient) BlockByHash(common.Hash, bool) (*web3types.Block, error) {
	panic("unexpected hash lookup")
}

type mixedCfxLogsClient struct {
	sdk.ClientOperator
	block       func(*cfxtypes.Epoch) (*cfxtypes.BlockSummary, error)
	logs        func(cfxtypes.LogFilter) ([]cfxtypes.Log, error)
	numberBlock func(hexutil.Uint64) (*cfxtypes.BlockSummary, error)
}

func (c *mixedCfxLogsClient) GetBlockSummaryByEpoch(e *cfxtypes.Epoch) (*cfxtypes.BlockSummary, error) {
	return c.block(e)
}
func (c *mixedCfxLogsClient) GetLogs(f cfxtypes.LogFilter) ([]cfxtypes.Log, error) { return c.logs(f) }
func (c *mixedCfxLogsClient) GetBlockSummaryByBlockNumber(n hexutil.Uint64) (*cfxtypes.BlockSummary, error) {
	if c.numberBlock != nil {
		return c.numberBlock(n)
	}
	panic("unexpected numbered lookup")
}
func (*mixedCfxLogsClient) GetBlockSummaryByHash(cfxtypes.Hash) (*cfxtypes.BlockSummary, error) {
	panic("unexpected hash lookup")
}

func createGetLogsTables(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Exec(`CREATE TABLE bn_partitions (id INTEGER PRIMARY KEY, entity TEXT, pi INTEGER, count INTEGER, bn_min INTEGER, bn_max INTEGER, created_at DATETIME, updated_at DATETIME)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO bn_partitions (id,entity,pi,count,bn_min,bn_max) VALUES (1,'logs',0,0,10,10)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE logs_0 (id INTEGER PRIMARY KEY, bn INTEGER, epoch INTEGER, topic0 TEXT, topic1 TEXT, topic2 TEXT, topic3 TEXT, log_index INTEGER, extra BLOB)`).Error)
}

func TestGetLogsMixedConsistency(t *testing.T) {
	oldCheck, oldLimit := checkMixedSourceConsistency, maxGetLogsResponseBytes
	oldBlocks, oldEpochs := store.MaxLogBlockRange, store.MaxLogEpochRange
	store.MaxLogBlockRange, store.MaxLogEpochRange = 1000, 1000
	t.Cleanup(func() { store.MaxLogBlockRange, store.MaxLogEpochRange = oldBlocks, oldEpochs })
	t.Cleanup(func() { checkMixedSourceConsistency = oldCheck; maxGetLogsResponseBytes = oldLimit })
	maxGetLogsResponseBytes = 1 << 20
	for _, space := range []string{"espace", "core"} {
		for _, scenario := range []string{"aligned", "mismatch", "converge", "checkpoint", "disabled", "metadata_error", "missing_checkpoint", "cancel_retry", "db_reorg", "converge_many", "db_reorg_many", "alternating"} {
			t.Run(space+"/"+scenario, func(t *testing.T) {
				checkMixedSourceConsistency = scenario != "disabled"
				db := newScanLogsHandlerTestDB(t)
				createGetLogsTables(t, db)
				hash := common.HexToHash("0x1234")
				other := common.HexToHash("0x5678")
				epoch := uint64(10)
				if space == "core" {
					epoch = 1
				}
				insertScanLogsMapping(t, db, epoch, 10, 10, hash.Hex())
				calls, metadata := 0, 0
				ctx, cancel := context.WithCancel(context.Background())
				if scenario == "mismatch" || scenario == "missing_checkpoint" {
					var deadlineCancel context.CancelFunc
					ctx, deadlineCancel = context.WithTimeout(ctx, 150*time.Millisecond)
					defer deadlineCancel()
				}
				defer cancel()
				nodeErr := errors.New("metadata rpc failed")
				readHash := func(boundary bool) (common.Hash, error) {
					metadata++
					if boundary && calls < 4 && (scenario == "converge_many" || scenario == "alternating" && calls%2 == 1) {
						return other, nil
					}
					if scenario == "metadata_error" {
						return common.Hash{}, nodeErr
					}
					if boundary && (scenario == "mismatch" || scenario == "disabled" || scenario == "cancel_retry" || (scenario == "converge" && calls < 2)) {
						return other, nil
					}
					if !boundary && scenario == "checkpoint" && calls > 0 {
						return other, nil
					}
					return hash, nil
				}
				readLogs := func() {
					calls++
					if scenario == "cancel_retry" {
						cancel()
					}
					if scenario == "db_reorg" && calls == 1 || scenario == "db_reorg_many" && calls < 4 || scenario == "alternating" && calls < 4 && calls%2 == 0 {
						require.NoError(t, db.Exec(`INSERT OR REPLACE INTO configs (name,value) VALUES ('reorg.version',?)`, calls).Error)
					}
				}
				var hit bool
				var err error
				if space == "espace" {
					client := &mixedEthLogsClient{fakeEthScanClient: &fakeEthScanClient{}}
					client.blockFn = func(n int64) (*web3types.Block, error) {
						h, err := readHash(n == 10)
						if err != nil {
							return nil, err
						}
						if scenario == "missing_checkpoint" && n != 10 {
							return nil, nil
						}
						if n == int64(web3types.LatestBlockNumber) {
							n = 12
						}
						return &web3types.Block{Hash: h, Number: big.NewInt(n)}, nil
					}
					client.logsFn = func(f web3types.FilterQuery) ([]web3types.Log, error) { readLogs(); return nil, nil }
					_, hit, err = newTestEthScanLogsHandler(db).GetLogs(ctx, client, &web3types.FilterQuery{FromBlock: blockNumber(10), ToBlock: blockNumber(12)}, "")
				} else {
					client := &mixedCfxLogsClient{}
					client.block = func(e *cfxtypes.Epoch) (*cfxtypes.BlockSummary, error) {
						n, numeric := e.ToInt()
						number := uint64(2)
						if numeric {
							number = n.Uint64()
						}
						h, err := readHash(number == 1)
						if err != nil {
							return nil, err
						}
						if scenario == "missing_checkpoint" && number != 1 {
							return nil, nil
						}
						return cfxSummary(cfxtypes.Hash(h.Hex()), number, number+9), nil
					}
					client.logs = func(f cfxtypes.LogFilter) ([]cfxtypes.Log, error) { readLogs(); return nil, nil }
					_, hit, err = newTestCfxScanLogsHandler(db).GetLogs(ctx, client, &cfxtypes.LogFilter{FromEpoch: epochNumber(1), ToEpoch: epochNumber(2)}, "")
				}
				switch scenario {
				case "mismatch", "missing_checkpoint":
					require.ErrorIs(t, err, store.ErrGetLogsTimeout)
				case "metadata_error":
					require.ErrorIs(t, err, nodeErr)
				case "cancel_retry":
					require.ErrorIs(t, err, store.ErrGetLogsTimeout)
				default:
					require.NoError(t, err)
					require.True(t, hit)
				}
				switch scenario {
				case "converge", "checkpoint", "db_reorg":
					require.Equal(t, 2, calls)
				case "mismatch":
					require.GreaterOrEqual(t, calls, 2)
				case "converge_many", "db_reorg_many", "alternating":
					require.Equal(t, 4, calls)
				case "missing_checkpoint", "metadata_error":
					require.Zero(t, calls)
				default:
					require.Equal(t, 1, calls)
				}
				if scenario == "disabled" {
					require.Zero(t, metadata)
				}
			})
		}
	}
}

func TestGetLogsFenceExcludesArchiveOnlyReads(t *testing.T) {
	// No successful DB segment means archive+regular-FN results stay outside the fence.
	fence, err := (&CfxLogsApiHandler{}).newLogsFence(nil, nil, &cfxtypes.LogFilter{})
	require.NoError(t, err)
	require.Nil(t, fence)
}

func TestGetLogsConsistencyConfiguration(t *testing.T) {
	oldCheck, oldBytes, oldErr := checkMixedSourceConsistency, maxGetLogsResponseBytes, errResponseBodySizeTooLarge
	oldLimit, oldWindow := maxScanLogsLimit, defaultScanLogsFNWindow
	t.Cleanup(func() {
		viper.Reset()
		checkMixedSourceConsistency, maxGetLogsResponseBytes, errResponseBodySizeTooLarge = oldCheck, oldBytes, oldErr
		maxScanLogsLimit, defaultScanLogsFNWindow = oldLimit, oldWindow
	})
	viper.Reset()
	MustInitFromViper()
	require.True(t, checkMixedSourceConsistency)
	viper.Set("requestControl.getLogs.checkMixedSourceConsistency", false)
	MustInitFromViper()
	require.False(t, checkMixedSourceConsistency)
	viper.Set("requestControl.getLogs.checkMixedSourceConsistency", true)
	MustInitFromViper()
	require.True(t, checkMixedSourceConsistency)
}

func TestGetLogsCheckpointDoesNotChangeRange(t *testing.T) {
	oldCheck, oldBlocks, oldEpochs, oldBytes := checkMixedSourceConsistency, store.MaxLogBlockRange, store.MaxLogEpochRange, maxGetLogsResponseBytes
	checkMixedSourceConsistency = true
	store.MaxLogBlockRange = 1000
	store.MaxLogEpochRange = 1000
	maxGetLogsResponseBytes = 1 << 20
	t.Cleanup(func() {
		checkMixedSourceConsistency, store.MaxLogBlockRange, store.MaxLogEpochRange, maxGetLogsResponseBytes = oldCheck, oldBlocks, oldEpochs, oldBytes
	})
	hash := common.HexToHash("0x1234")
	for _, upper := range []uint64{12, 30} {
		db := newScanLogsHandlerTestDB(t)
		createGetLogsTables(t, db)
		insertScanLogsMapping(t, db, 10, 10, 10, hash.Hex())
		var numbers []uint64
		client := &mixedEthLogsClient{fakeEthScanClient: &fakeEthScanClient{}}
		client.blockFn = func(n int64) (*web3types.Block, error) {
			if n == int64(web3types.LatestBlockNumber) {
				n = 20
			}
			numbers = append(numbers, uint64(n))
			return &web3types.Block{Number: big.NewInt(n), Hash: hash}, nil
		}
		client.logsFn = func(f web3types.FilterQuery) ([]web3types.Log, error) {
			require.EqualValues(t, 11, *f.FromBlock)
			require.EqualValues(t, upper, *f.ToBlock)
			return nil, nil
		}
		_, _, err := newTestEthScanLogsHandler(db).GetLogs(context.Background(), client, &web3types.FilterQuery{FromBlock: blockNumber(10), ToBlock: blockNumber(web3types.BlockNumber(upper))}, "")
		require.NoError(t, err)
		require.Equal(t, min(upper, uint64(20)), numbers[len(numbers)-1])
	}
	for _, mode := range []string{"epoch", "block", "future"} {
		db := newScanLogsHandlerTestDB(t)
		createGetLogsTables(t, db)
		insertScanLogsMapping(t, db, 1, 10, 10, hash.Hex())
		var epochs []uint64
		filter := &cfxtypes.LogFilter{FromEpoch: epochNumber(1), ToEpoch: epochNumber(2)}
		if mode == "block" {
			filter = &cfxtypes.LogFilter{FromBlock: cfxtypes.NewBigInt(10), ToBlock: cfxtypes.NewBigInt(12)}
		}
		if mode == "future" {
			filter.ToEpoch = epochNumber(4)
		}
		client := &mixedCfxLogsClient{}
		client.block = func(e *cfxtypes.Epoch) (*cfxtypes.BlockSummary, error) {
			n, numeric := e.ToInt()
			number := uint64(3)
			if numeric {
				number = n.Uint64()
			}
			epochs = append(epochs, number)
			blockNumber := number + 9
			if number == 2 {
				blockNumber = 12
			}
			if number == 3 {
				blockNumber = 20
			}
			return cfxSummary(cfxtypes.Hash(hash.Hex()), number, blockNumber), nil
		}
		client.numberBlock = func(n hexutil.Uint64) (*cfxtypes.BlockSummary, error) {
			require.EqualValues(t, 12, n)
			return cfxSummary(cfxtypes.Hash(hash.Hex()), 2, 12), nil
		}
		client.logs = func(f cfxtypes.LogFilter) ([]cfxtypes.Log, error) {
			if mode == "block" {
				require.EqualValues(t, 12, f.ToBlock.ToInt().Uint64())
			} else {
				require.True(t, filter.ToEpoch.Equals(f.ToEpoch))
			}
			return nil, nil
		}
		_, _, err := newTestCfxScanLogsHandler(db).GetLogs(context.Background(), client, filter, "")
		require.NoError(t, err)
		checkpoint := uint64(2)
		if mode == "future" {
			checkpoint = 3
		}
		require.Equal(t, checkpoint, epochs[len(epochs)-1])
	}
}

func TestGetLogsCheckpointSelection(t *testing.T) {
	oldCheck, oldBytes := checkMixedSourceConsistency, maxGetLogsResponseBytes
	oldBlocks, oldEpochs := store.MaxLogBlockRange, store.MaxLogEpochRange
	maxGetLogsResponseBytes = 1 << 20
	store.MaxLogBlockRange, store.MaxLogEpochRange = 1000, 1000
	t.Cleanup(func() {
		checkMixedSourceConsistency, maxGetLogsResponseBytes = oldCheck, oldBytes
		store.MaxLogBlockRange, store.MaxLogEpochRange = oldBlocks, oldEpochs
	})
	for _, space := range []string{"espace", "core"} {
		for _, scenario := range []string{"numeric", "reorg", "missing", "disabled", "db_only", "fn_only"} {
			t.Run(space+"/"+scenario, func(t *testing.T) {
				checkMixedSourceConsistency = scenario != "disabled"
				db := newScanLogsHandlerTestDB(t)
				createGetLogsTables(t, db)
				hash, other := common.HexToHash("0x1234"), common.HexToHash("0x5678")
				boundary, upper, head := uint64(10), uint64(12), uint64(20)
				if space == "core" {
					boundary, upper, head = 1, 2, 3
				}
				insertScanLogsMapping(t, db, boundary, 10, 10, hash.Hex())
				var reads []uint64
				calls, targetReads := 0, 0
				read := func(n uint64, latest bool) (uint64, common.Hash, bool) {
					if latest {
						n = head
					}
					reads = append(reads, n)
					if n == upper {
						targetReads++
						if scenario == "missing" && targetReads == 1 {
							return n, hash, true
						}
						if scenario == "reorg" && calls > 0 {
							return n, other, false
						}
					}
					return n, hash, false
				}
				if space == "espace" {
					filter := &web3types.FilterQuery{FromBlock: blockNumber(10), ToBlock: blockNumber(web3types.BlockNumber(upper))}
					if scenario == "db_only" {
						filter.ToBlock = blockNumber(10)
					}
					if scenario == "fn_only" {
						filter.FromBlock = blockNumber(11)
					}
					client := &mixedEthLogsClient{fakeEthScanClient: &fakeEthScanClient{}}
					client.blockFn = func(n int64) (*web3types.Block, error) {
						number, h, missing := read(uint64(n), n == int64(web3types.LatestBlockNumber))
						if missing {
							return nil, nil
						}
						return &web3types.Block{Number: new(big.Int).SetUint64(number), Hash: h}, nil
					}
					client.logsFn = func(f web3types.FilterQuery) ([]web3types.Log, error) {
						calls++
						require.Equal(t, *filter.ToBlock, *f.ToBlock)
						return nil, nil // Empty reads must still validate chain identities.
					}
					_, _, err := newTestEthScanLogsHandler(db).GetLogs(context.Background(), client, filter, "")
					require.NoError(t, err)
				} else {
					filter := &cfxtypes.LogFilter{FromEpoch: epochNumber(1), ToEpoch: epochNumber(upper)}
					if scenario == "db_only" {
						filter.ToEpoch = epochNumber(1)
					}
					if scenario == "fn_only" {
						filter.FromEpoch = epochNumber(2)
					}
					client := &mixedCfxLogsClient{}
					client.block = func(e *cfxtypes.Epoch) (*cfxtypes.BlockSummary, error) {
						n, numeric := e.ToInt()
						number := uint64(0)
						if numeric {
							number = n.Uint64()
						}
						number, h, missing := read(number, !numeric)
						if missing {
							return nil, nil
						}
						return cfxSummary(cfxtypes.Hash(h.Hex()), number, number+9), nil
					}
					client.logs = func(f cfxtypes.LogFilter) ([]cfxtypes.Log, error) {
						calls++
						require.True(t, filter.ToEpoch.Equals(f.ToEpoch))
						return nil, nil
					}
					_, _, err := newTestCfxScanLogsHandler(db).GetLogs(context.Background(), client, filter, "")
					require.NoError(t, err)
				}
				switch scenario {
				case "numeric":
					require.Equal(t, []uint64{head, upper, boundary, upper}, reads)
				case "reorg":
					require.Equal(t, []uint64{head, upper, boundary, upper, head, upper, boundary, upper}, reads)
					require.Equal(t, 2, calls)
				case "missing":
					require.Equal(t, []uint64{head, upper, head, upper, boundary, upper}, reads)
				case "disabled", "db_only", "fn_only":
					require.Empty(t, reads)
				}
			})
		}
	}
}

func TestLogsFenceShortCircuitsMetadataReads(t *testing.T) {
	for _, scenario := range []string{"same_height", "bad_boundary", "bad_checkpoint", "rpc_error"} {
		t.Run(scenario, func(t *testing.T) {
			boundary := logBlockIdentity{10, "block-a"}
			checkpoint := logBlockIdentity{12, "block-b"}
			if scenario == "same_height" {
				checkpoint = boundary
			}
			calls := 0
			rpcErr := errors.New("metadata unavailable")
			fence := &logsConsistencyFence{boundary: boundary, checkpoint: checkpoint, readHash: func(n uint64) (string, error) {
				calls++
				if scenario == "rpc_error" {
					return "", rpcErr
				}
				if n == boundary.number {
					if scenario == "bad_boundary" {
						return "different", nil
					}
					return boundary.hash, nil
				}
				return "different", nil
			}}
			err := fence.check()
			switch scenario {
			case "same_height":
				require.NoError(t, err)
			case "rpc_error":
				require.ErrorIs(t, err, rpcErr)
			default:
				require.ErrorIs(t, err, ErrGetLogsConsistency)
			}
			if scenario == "bad_checkpoint" {
				require.Equal(t, 2, calls)
			} else {
				require.Equal(t, 1, calls)
			}
		})
	}
}
