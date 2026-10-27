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
	block func(*cfxtypes.Epoch) (*cfxtypes.BlockSummary, error)
	logs  func(cfxtypes.LogFilter) ([]cfxtypes.Log, error)
}

func (c *mixedCfxLogsClient) GetBlockSummaryByEpoch(e *cfxtypes.Epoch) (*cfxtypes.BlockSummary, error) {
	return c.block(e)
}
func (c *mixedCfxLogsClient) GetLogs(f cfxtypes.LogFilter) ([]cfxtypes.Log, error) { return c.logs(f) }
func (c *mixedCfxLogsClient) GetBlockSummaryByBlockNumber(n hexutil.Uint64) (*cfxtypes.BlockSummary, error) {
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
	checkMixedSourceConsistency = true
	maxGetLogsResponseBytes = 1 << 20
	store.MaxLogBlockRange, store.MaxLogEpochRange = 1000, 1000
	t.Cleanup(func() {
		checkMixedSourceConsistency, maxGetLogsResponseBytes = oldCheck, oldLimit
		store.MaxLogBlockRange, store.MaxLogEpochRange = oldBlocks, oldEpochs
	})
	for _, space := range []string{"espace", "core"} {
		for _, test := range []struct {
			name               string
			attempts, logCalls int
		}{
			{"aligned", 1, 1}, {"empty", 1, 1}, {"suffix_reorg", 1, 1},
			{"pre_mismatch", 2, 1}, {"post_mismatch", 2, 2},
			{"pre_missing", 2, 1}, {"post_missing", 2, 2},
			{"persistent_pre_mismatch", -1, 0}, {"persistent_post_mismatch", -1, -1},
			{"pre_error", 1, 0}, {"post_error", 1, 1}, {"logs_error", 1, 1},
			{"cancel_retry", 1, 0}, {"db_reorg", 2, 2}, {"db_boundary_refresh", 2, 2},
			{"converge_many", 4, 1}, {"post_converge_many", 4, 4},
			{"db_reorg_many", 4, 4}, {"alternating", 4, 3},
		} {
			t.Run(space+"/"+test.name, func(t *testing.T) {
				db := newScanLogsHandlerTestDB(t)
				createGetLogsTables(t, db)
				hash, other := common.HexToHash("0x1234"), common.HexToHash("0x5678")
				boundary := uint64(10)
				if space == "core" {
					boundary = 1
				}
				insertScanLogsMapping(t, db, boundary, 10, 10, hash.Hex())
				if test.name != "empty" {
					require.NoError(t, db.Exec(`INSERT INTO logs_0 (id,bn,epoch,log_index,extra) VALUES (1,10,?,0,'{"data":"0x00"}')`, boundary).Error)
					require.NoError(t, db.Exec(`UPDATE bn_partitions SET count=1`).Error)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if test.attempts < 0 {
					var deadlineCancel context.CancelFunc
					ctx, deadlineCancel = context.WithTimeout(ctx, 150*time.Millisecond)
					defer deadlineCancel()
				}
				nodeErr := errors.New("node read failed")
				attempts, logCalls := 0, 0
				var phases [][]string
				awaitingPost := false
				currentHash := hash
				readHash := func() (h common.Hash, missing bool, err error) {
					post := awaitingPost
					awaitingPost = false
					if !post {
						attempts++
						phases = append(phases, []string{"pre"})
					} else {
						phases[attempts-1] = append(phases[attempts-1], "post")
					}
					h = currentHash
					// Failed pre-checks must advance the attempt without a log RPC.
					defer func() { awaitingPost = !post && err == nil && !missing && h == currentHash }()
					if !post {
						switch test.name {
						case "pre_error":
							return h, false, nodeErr
						case "pre_missing":
							return h, attempts == 1, nil
						case "pre_mismatch":
							if attempts == 1 {
								h = other
							}
						case "converge_many":
							if attempts < 4 {
								h = other
							}
						case "alternating":
							if attempts == 1 {
								h = other
							}
						case "persistent_pre_mismatch", "cancel_retry":
							h = other
							if test.name == "cancel_retry" {
								cancel()
							}
						}
					} else {
						switch test.name {
						case "post_error":
							return h, false, nodeErr
						case "post_missing":
							return h, attempts == 1, nil
						case "post_mismatch":
							if attempts == 1 {
								h = other
							}
						case "post_converge_many":
							if attempts < 4 {
								h = other
							}
						case "alternating":
							if attempts == 3 {
								h = other
							}
						case "persistent_post_mismatch":
							h = other
						}
					}
					return h, false, nil
				}
				readLogs := func() error {
					logCalls++
					require.True(t, awaitingPost, "log query must follow a successful pre-check")
					phases[attempts-1] = append(phases[attempts-1], "logs")
					if test.name == "logs_error" {
						return nodeErr
					}
					if test.name == "db_reorg" && attempts == 1 || test.name == "db_reorg_many" && attempts < 4 ||
						test.name == "alternating" && attempts == 2 || test.name == "db_boundary_refresh" && attempts == 1 {
						require.NoError(t, db.Exec(`INSERT OR REPLACE INTO configs (name,value) VALUES ('reorg.version',?)`, attempts).Error)
					}
					if test.name == "db_boundary_refresh" && attempts == 1 {
						currentHash = other
						require.NoError(t, db.Exec(`UPDATE epoch_block_map SET pivot_hash=?`, other.Hex()).Error)
						require.NoError(t, db.Exec(`UPDATE logs_0 SET extra='{"data":"0x2a"}'`).Error)
					}
					return nil
				}
				var data [][]byte
				var hit bool
				var err error
				if space == "espace" {
					client := &mixedEthLogsClient{fakeEthScanClient: &fakeEthScanClient{}}
					client.blockFn = func(n int64) (*web3types.Block, error) {
						require.EqualValues(t, boundary, n, "only the fixed DB boundary may be queried")
						h, missing, err := readHash()
						if err != nil || missing {
							return nil, err
						}
						return &web3types.Block{Number: big.NewInt(n), Hash: h}, nil
					}
					client.logsFn = func(f web3types.FilterQuery) ([]web3types.Log, error) {
						if err := readLogs(); err != nil {
							return nil, err
						}
						if test.name == "empty" {
							return nil, nil
						}
						log := web3types.Log{BlockNumber: 11, Data: []byte{byte(attempts)}}
						if test.name == "suffix_reorg" {
							log.BlockHash = other
						}
						return []web3types.Log{log}, nil
					}
					var logs []web3types.Log
					logs, hit, err = newTestEthScanLogsHandler(db).GetLogs(ctx, client, &web3types.FilterQuery{FromBlock: blockNumber(10), ToBlock: blockNumber(12)}, "")
					for _, log := range logs {
						data = append(data, log.Data)
					}
				} else {
					client := &mixedCfxLogsClient{}
					client.block = func(e *cfxtypes.Epoch) (*cfxtypes.BlockSummary, error) {
						n, numeric := e.ToInt()
						require.True(t, numeric, "head labels must not be queried")
						require.EqualValues(t, boundary, n.Uint64())
						h, missing, err := readHash()
						if err != nil || missing {
							return nil, err
						}
						return cfxSummary(cfxtypes.Hash(h.Hex()), boundary, 10), nil
					}
					client.logs = func(f cfxtypes.LogFilter) ([]cfxtypes.Log, error) {
						if err := readLogs(); err != nil {
							return nil, err
						}
						if test.name == "empty" {
							return nil, nil
						}
						log := cfxtypes.Log{Data: []byte{byte(attempts)}}
						if test.name == "suffix_reorg" {
							h := cfxtypes.Hash(other.Hex())
							log.BlockHash = &h
						}
						return []cfxtypes.Log{log}, nil
					}
					var logs []cfxtypes.Log
					logs, hit, err = newTestCfxScanLogsHandler(db).GetLogs(ctx, client, &cfxtypes.LogFilter{FromEpoch: epochNumber(1), ToEpoch: epochNumber(2)}, "")
					for _, log := range logs {
						data = append(data, log.Data)
					}
				}
				switch test.name {
				case "persistent_pre_mismatch", "persistent_post_mismatch", "cancel_retry":
					require.ErrorIs(t, err, store.ErrGetLogsTimeout)
				case "pre_error", "post_error", "logs_error":
					require.ErrorIs(t, err, nodeErr)
				default:
					require.NoError(t, err)
					require.True(t, hit)
					if test.name == "empty" {
						require.Empty(t, data)
					} else {
						dbData := byte(0)
						if test.name == "db_boundary_refresh" {
							dbData = 42
						}
						require.Equal(t, [][]byte{{dbData}, {byte(attempts)}}, data, "only the final complete attempt may be returned")
					}
				}
				if err != nil {
					require.Empty(t, data, "failed queries must not publish partial results")
				}
				if test.attempts < 0 {
					require.GreaterOrEqual(t, attempts, 2)
				} else {
					require.Equal(t, test.attempts, attempts)
				}
				if test.logCalls < 0 {
					require.GreaterOrEqual(t, logCalls, 2)
				} else {
					require.Equal(t, test.logCalls, logCalls)
				}
				for _, phase := range phases {
					if len(phase) == 1 {
						require.Equal(t, []string{"pre"}, phase)
					} else if test.name == "logs_error" {
						require.Equal(t, []string{"pre", "logs"}, phase)
					} else {
						require.Equal(t, []string{"pre", "logs", "post"}, phase)
					}
				}
			})
		}
	}
}

func TestGetLogsFenceExcludesArchiveOnlyReads(t *testing.T) {
	// No successful DB segment means archive+regular-FN results stay outside the fence.
	fence, err := (&CfxLogsApiHandler{}).newLogsFence(nil, nil)
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

func TestGetLogsBoundaryDoesNotChangeRange(t *testing.T) {
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
		require.Equal(t, []uint64{10, 10}, numbers)
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
			require.True(t, numeric)
			number := n.Uint64()
			epochs = append(epochs, number)
			return cfxSummary(cfxtypes.Hash(hash.Hex()), number, number+9), nil
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
		require.Equal(t, []uint64{1, 1}, epochs)
	}
}

func TestGetLogsBoundaryCheckRoutes(t *testing.T) {
	oldCheck, oldBytes := checkMixedSourceConsistency, maxGetLogsResponseBytes
	oldBlocks, oldEpochs := store.MaxLogBlockRange, store.MaxLogEpochRange
	maxGetLogsResponseBytes = 1 << 20
	store.MaxLogBlockRange, store.MaxLogEpochRange = 1000, 1000
	t.Cleanup(func() {
		checkMixedSourceConsistency, maxGetLogsResponseBytes = oldCheck, oldBytes
		store.MaxLogBlockRange, store.MaxLogEpochRange = oldBlocks, oldEpochs
	})
	for _, space := range []string{"espace", "core"} {
		for _, scenario := range []string{"mixed", "disabled", "db_only", "fn_only"} {
			t.Run(space+"/"+scenario, func(t *testing.T) {
				checkMixedSourceConsistency = scenario != "disabled"
				db := newScanLogsHandlerTestDB(t)
				createGetLogsTables(t, db)
				hash := common.HexToHash("0x1234")
				boundary := uint64(10)
				if space == "core" {
					boundary = 1
				}
				insertScanLogsMapping(t, db, boundary, 10, 10, hash.Hex())
				metadata, logs := 0, 0
				if space == "espace" {
					filter := &web3types.FilterQuery{FromBlock: blockNumber(10), ToBlock: blockNumber(12)}
					if scenario == "db_only" {
						filter.ToBlock = blockNumber(10)
					}
					if scenario == "fn_only" {
						filter.FromBlock = blockNumber(11)
					}
					client := &mixedEthLogsClient{fakeEthScanClient: &fakeEthScanClient{}}
					client.blockFn = func(n int64) (*web3types.Block, error) {
						metadata++
						require.EqualValues(t, boundary, n)
						return &web3types.Block{Number: big.NewInt(n), Hash: hash}, nil
					}
					client.logsFn = func(web3types.FilterQuery) ([]web3types.Log, error) { logs++; return nil, nil }
					_, _, err := newTestEthScanLogsHandler(db).GetLogs(context.Background(), client, filter, "")
					require.NoError(t, err)
				} else {
					filter := &cfxtypes.LogFilter{FromEpoch: epochNumber(1), ToEpoch: epochNumber(2)}
					if scenario == "db_only" {
						filter.ToEpoch = epochNumber(1)
					}
					if scenario == "fn_only" {
						filter.FromEpoch = epochNumber(2)
					}
					client := &mixedCfxLogsClient{}
					client.block = func(e *cfxtypes.Epoch) (*cfxtypes.BlockSummary, error) {
						metadata++
						require.True(t, epochNumber(boundary).Equals(e))
						return cfxSummary(cfxtypes.Hash(hash.Hex()), boundary, 10), nil
					}
					client.logs = func(cfxtypes.LogFilter) ([]cfxtypes.Log, error) { logs++; return nil, nil }
					_, _, err := newTestCfxScanLogsHandler(db).GetLogs(context.Background(), client, filter, "")
					require.NoError(t, err)
				}
				if scenario == "mixed" {
					require.Equal(t, 2, metadata)
				} else {
					require.Zero(t, metadata)
				}
				if scenario == "db_only" {
					require.Zero(t, logs)
				} else {
					require.Equal(t, 1, logs)
				}
			})
		}
	}
}

func TestLogsBoundaryMetadataValidation(t *testing.T) {
	for _, mode := range []string{"nil_block", "nil_height", "negative_height", "wrong_height", "empty_hash"} {
		t.Run(mode, func(t *testing.T) {
			hash := common.HexToHash("0x1234")
			ethBlock := &web3types.Block{Number: big.NewInt(10), Hash: hash}
			cfxBlock := cfxSummary(cfxtypes.Hash(hash.Hex()), 1, 10)
			switch mode {
			case "nil_block":
				ethBlock, cfxBlock = nil, nil
			case "nil_height":
				ethBlock.Number, cfxBlock.EpochNumber = nil, nil
			case "negative_height":
				ethBlock.Number, cfxBlock.EpochNumber = big.NewInt(-1), (*hexutil.Big)(big.NewInt(-1))
			case "wrong_height":
				ethBlock.Number, cfxBlock.EpochNumber = big.NewInt(11), cfxtypes.NewBigInt(2)
			case "empty_hash":
				ethBlock.Hash, cfxBlock.Hash = common.Hash{}, ""
			}
			eth := &mixedEthLogsClient{fakeEthScanClient: &fakeEthScanClient{}}
			eth.blockFn = func(int64) (*web3types.Block, error) { return ethBlock, nil }
			_, err := ethLogBoundaryHash(eth, 10)
			require.ErrorIs(t, err, ErrGetLogsConsistency)
			cfx := &mixedCfxLogsClient{block: func(*cfxtypes.Epoch) (*cfxtypes.BlockSummary, error) { return cfxBlock, nil }}
			_, err = cfxLogBoundaryHash(cfx, 1)
			require.ErrorIs(t, err, ErrGetLogsConsistency)
		})
	}
}
