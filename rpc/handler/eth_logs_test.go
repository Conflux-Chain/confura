package handler

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/ethereum/go-ethereum/common"
	web3types "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

type ethHashLogsClient struct {
	block  *web3types.Block
	err    error
	filter web3types.FilterQuery
	logs   []web3types.Log
	calls  int
	onHash func()
}

func (c *ethHashLogsClient) BlockByHash(common.Hash, bool) (*web3types.Block, error) {
	if c.onHash != nil {
		c.onHash()
	}
	return c.block, c.err
}
func (c *ethHashLogsClient) BlockByNumber(web3types.BlockNumber, bool) (*web3types.Block, error) {
	panic("unexpected block lookup")
}
func (c *ethHashLogsClient) Logs(f web3types.FilterQuery) ([]web3types.Log, error) {
	c.filter = f
	c.calls++
	return c.logs, c.err
}

func TestEthHashLogIdentity(t *testing.T) {
	hash := common.HexToHash("0x1234")
	for _, scenario := range []string{"match", "mismatch", "missing", "pruned", "ahead", "unknown", "invalid", "node_error", "db_error"} {
		t.Run(scenario, func(t *testing.T) {
			db := newScanLogsHandlerTestDB(t)
			handler := newTestEthScanLogsHandler(db)
			insertScanLogsMapping(t, db, 10, 10, 10, hash.Hex())
			client := &ethHashLogsClient{block: &web3types.Block{Number: big.NewInt(10)}}
			filter := &web3types.FilterQuery{BlockHash: &hash, Addresses: []common.Address{common.HexToAddress("0x1234")}, Topics: [][]common.Hash{{hash}}}
			switch scenario {
			case "mismatch":
				require.NoError(t, db.Exec("UPDATE epoch_block_map SET pivot_hash = ?", common.Hash{}.Hex()).Error)
			case "missing":
				require.NoError(t, db.Exec("DELETE FROM epoch_block_map").Error)
			case "pruned":
				client.block.Number = big.NewInt(9)
			case "ahead":
				client.block.Number = big.NewInt(11)
			case "unknown":
				client.block = nil
			case "invalid":
				client.block.Number = nil
			case "node_error":
				client.err = errors.New("rpc failed")
			case "db_error":
				require.NoError(t, db.Exec("DROP TABLE epoch_block_map").Error)
			}
			dbFilter, fnFilter, err := handler.splitLogFilterByBlockHash(client, filter, 10, 10)
			switch scenario {
			case "match":
				require.NoError(t, err)
				require.NotNil(t, dbFilter)
				require.Nil(t, fnFilter)
			case "pruned":
				require.ErrorIs(t, err, store.ErrAlreadyPruned)
			case "invalid", "node_error", "db_error":
				require.Error(t, err)
				require.Nil(t, fnFilter)
			default:
				require.NoError(t, err)
				require.Nil(t, dbFilter)
				require.Same(t, filter, fnFilter)
			}
		})
	}
}

func TestEthHashLogsKeepDatabaseErrors(t *testing.T) {
	db := newScanLogsHandlerTestDB(t)
	handler := newTestEthScanLogsHandler(db)
	require.NoError(t, db.Exec("DROP TABLE configs").Error)
	hash := common.HexToHash("0x1234")
	client := &ethHashLogsClient{}
	_, _, err := handler.GetLogs(context.Background(), client, &web3types.FilterQuery{BlockHash: &hash}, "")
	require.Error(t, err)
	require.Zero(t, client.calls)
}

func TestEthHashLogsFallbackKeepsFilter(t *testing.T) {
	db := newScanLogsHandlerTestDB(t)
	insertScanLogsMapping(t, db, 10, 10, 10, common.HexToHash("0x5678").Hex())
	hash := common.HexToHash("0x1234")
	client := &ethHashLogsClient{block: &web3types.Block{Number: big.NewInt(10)}}
	filter := &web3types.FilterQuery{BlockHash: &hash, Addresses: []common.Address{common.HexToAddress("0x1111")}, Topics: [][]common.Hash{{hash}}}
	logs, hit, err := newTestEthScanLogsHandler(db).GetLogs(context.Background(), client, filter, "")
	require.NoError(t, err)
	require.False(t, hit)
	require.Empty(t, logs)
	require.Equal(t, *filter, client.filter)
}

func TestEthHashLogsDatabaseEmptyOrFailure(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "query_error"}[failed], func(t *testing.T) {
			db := newScanLogsHandlerTestDB(t)
			createGetLogsTables(t, db)
			hash := common.HexToHash("0x1234")
			insertScanLogsMapping(t, db, 10, 10, 10, hash.Hex())
			if failed {
				require.NoError(t, db.Exec("DROP TABLE logs_0").Error)
			}
			client := &ethHashLogsClient{block: &web3types.Block{Number: big.NewInt(10)}}
			logs, hit, err := newTestEthScanLogsHandler(db).GetLogs(context.Background(), client, &web3types.FilterQuery{BlockHash: &hash}, "")
			if failed {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.True(t, hit)
				require.Empty(t, logs)
			}
			require.Zero(t, client.calls)
		})
	}
}
