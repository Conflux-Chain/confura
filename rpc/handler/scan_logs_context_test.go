package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
	"time"

	citypes "github.com/Conflux-Chain/confura/types"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/go-rpc-provider/interfaces"
	"github.com/openweb3/web3go"
	wt "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

type scanContextProvider struct {
	interfaces.Provider
	call func(context.Context, interface{}, string, ...interface{}) error
}

func (p *scanContextProvider) CallContext(ctx context.Context, result interface{}, method string, args ...interface{}) error {
	return p.call(ctx, result, method, args...)
}
func scanAssign(result, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, result)
}

func TestScanLogsCancelsFullnodeReads(t *testing.T) {
	for _, space := range []string{"cfx", "eth"} {
		for _, stage := range []string{"before", "logs", "after"} {
			for _, ignore := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/ignore=%v", space, stage, ignore), func(t *testing.T) {
					db := newScanLogsHandlerTestDB(t)
					sqlDB, err := db.DB()
					require.NoError(t, err)
					defer sqlDB.Close()
					entered := make(chan context.Context, 1)
					release := make(chan struct{})
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					blockCalls := 0
					provider := &scanContextProvider{call: func(c context.Context, result interface{}, method string, args ...interface{}) error {
						isLogs := method == space+"_getLogs"
						current := "other"
						if isLogs {
							current = "logs"
						}
						if !isLogs && (space == "eth" || fmt.Sprint(args[0]) == "0xa") {
							blockCalls++
							current = "before"
							if blockCalls > 1 {
								current = "after"
							}
						}
						if current == stage {
							entered <- c
							if ignore {
								<-release
							} else {
								<-c.Done()
								return c.Err()
							}
						}
						if isLogs {
							return scanAssign(result, []interface{}{})
						}
						if space == "eth" {
							return scanAssign(result, &wt.Block{Number: big.NewInt(10), Difficulty: big.NewInt(0), Hash: common.HexToHash("0xa")})
						}
						return scanAssign(result, map[string]interface{}{"epochNumber": fmt.Sprint(args[0]), "blockNumber": fmt.Sprint(args[0]), "hash": common.HexToHash(fmt.Sprint(args[0])).Hex()})
					}}
					eth := web3go.NewClientWithProvider(provider)
					cfx, err := sdk.NewClientWithProvider(provider)
					require.NoError(t, err)
					cfx.SetNetworkId(1029)
					cfx.SetChainId(1029)
					done := make(chan error, 1)
					go func() {
						if space == "eth" {
							result, err := newTestEthScanLogsHandler(db).ScanLogs(ctx, eth.Eth, EthScanLogParams{EthScanLogRequest: &EthScanLogRequest{Limit: 1}, BlockRange: citypes.RangeUint64{From: 10, To: 10}}, nil)
							if result != nil {
								done <- fmt.Errorf("published canceled page")
								return
							}
							done <- err
						} else {
							result, err := newTestCfxScanLogsHandler(db).ScanLogs(ctx, rpcutil.NewCfxCoreClient(cfx), CfxScanLogParams{CfxScanLogRequest: &CfxScanLogRequest{Limit: 1}, EpochRange: citypes.RangeUint64{From: 10, To: 10}}, nil)
							if result != nil {
								done <- fmt.Errorf("published canceled page")
								return
							}
							done <- err
						}
					}()
					select {
					case upstream := <-entered:
						cancel()
						require.ErrorIs(t, upstream.Err(), context.Canceled)
						close(release)
					case err := <-done:
						t.Fatalf("returned before %s: %v", stage, err)
					case <-time.After(3 * time.Second):
						t.Fatal("read not entered")
					}
					select {
					case err := <-done:
						require.Error(t, err)
					case <-time.After(3 * time.Second):
						t.Fatal("cancellation did not finish")
					}
				})
			}
		}
	}
}

func TestScanLogsCancelsMetadataWait(t *testing.T) {
	for _, space := range []string{"cfx", "eth"} {
		t.Run(space, func(t *testing.T) {
			db := newScanLogsHandlerTestDB(t)
			sqlDB, err := db.DB()
			require.NoError(t, err)
			defer sqlDB.Close()
			conn, err := sqlDB.Conn(context.Background())
			require.NoError(t, err)
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if space == "eth" {
					_, err := newTestEthScanLogsHandler(db).ScanLogs(ctx, &fakeEthScanClient{}, EthScanLogParams{EthScanLogRequest: &EthScanLogRequest{Limit: 1}, BlockRange: citypes.RangeUint64{From: 10, To: 10}}, nil)
					done <- err
				} else {
					_, err := newTestCfxScanLogsHandler(db).ScanLogs(ctx, &fakeCfxScanClient{}, CfxScanLogParams{CfxScanLogRequest: &CfxScanLogRequest{Limit: 1}, EpochRange: citypes.RangeUint64{From: 10, To: 10}}, nil)
					done <- err
				}
			}()
			select {
			case err := <-done:
				require.Error(t, err)
			case <-time.After(time.Second):
				t.Fatal("metadata ignored deadline while pool was held")
			}
			require.NoError(t, conn.Close())
			// Cancellation must not persist on the store shared by other requests.
			if space == "eth" {
				_, err = newTestEthScanLogsHandler(db).es.GetReorgVersion()
			} else {
				_, err = newTestCfxScanLogsHandler(db).ms.GetReorgVersion()
			}
			require.NoError(t, err)
		})
	}
}

func TestScanNormalizeCancellation(t *testing.T) {
	for _, space := range []string{"cfx", "eth"} {
		t.Run(space, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan struct{})
			p := &scanContextProvider{call: func(c context.Context, _ interface{}, _ string, _ ...interface{}) error {
				close(entered)
				<-c.Done()
				return c.Err()
			}}
			done := make(chan error, 1)
			go func() {
				if space == "eth" {
					c := web3go.NewClientWithProvider(p)
					_, err := NormalizeEthScanLogRequest(c.WithContext(ctx).Eth, 0, EthScanLogRequest{}, false)
					done <- err
				} else {
					c, err := sdk.NewClientWithProvider(p)
					if err != nil {
						done <- err
						return
					}
					c.SetNetworkId(1029)
					c.SetChainId(1029)
					_, err = NormalizeCfxScanLogRequest(rpcutil.CfxWithContext(ctx, c), CfxScanLogRequest{}, false)
					done <- err
				}
			}()
			select {
			case <-entered:
				cancel()
			case <-time.After(time.Second):
				t.Fatal("normalize did not start")
			}
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("normalize ignored cancellation")
			}
		})
	}
}

func TestScanMetadataReadsHonorDeadline(t *testing.T) {
	db := newScanLogsHandlerTestDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	defer sqlDB.Close()
	ms := newTestCfxScanLogsHandler(db).ms
	reads := map[string]func(context.Context) error{
		"version":  func(ctx context.Context) error { _, err := ms.GetReorgVersionContext(ctx); return err },
		"earliest": func(ctx context.Context) error { _, _, err := ms.EarliestBlockMappingContext(ctx); return err },
		"latest":   func(ctx context.Context) error { _, _, err := ms.LatestBlockMappingContext(ctx); return err },
		"mapping":  func(ctx context.Context) error { _, _, err := ms.BlockMappingContext(ctx, 10); return err },
		"pivot":    func(ctx context.Context) error { _, _, err := ms.PivotHashContext(ctx, 10); return err },
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			conn, err := sqlDB.Conn(context.Background())
			require.NoError(t, err)
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- read(ctx) }()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.DeadlineExceeded)
			case <-time.After(time.Second):
				t.Fatal("metadata wait ignored deadline")
			}
			require.NoError(t, conn.Close())
			require.NoError(t, read(context.Background()))
		})
	}
}
