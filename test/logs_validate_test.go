package test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	cfx "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
	eth "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type validationDBStub struct {
	read    func(store.LogFilter) ([]*store.Log, error)
	version int
	calls   int
}

func (db *validationDBStub) GetLogs(_ context.Context, filter store.LogFilter) ([]*store.Log, error) {
	db.calls++
	return db.read(filter)
}

func (db *validationDBStub) GetReorgVersion() (int, error) { return db.version, nil }

func validationRPC(t *testing.T, respond func(string, []json.RawMessage) interface{}) *gethrpc.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": request.ID, "result": respond(request.Method, request.Params)})
	}))
	t.Cleanup(server.Close)
	client, err := gethrpc.Dial(server.URL)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client
}

func validationSource(t *testing.T, db *validationDBStub, latest *uint64) *logValidationSource {
	return &logValidationSource{
		space: "espace", db: db, next: 10, endpoint: "http://fullnode.invalid",
		bounds: func() (uint64, uint64, bool, error) { return 1, *latest, true, nil },
		rpc:    validationRPC(t, func(string, []json.RawMessage) interface{} { return []eth.Log{} }),
	}
}

func TestLogsValidationDynamicDBHeight(t *testing.T) {
	latest := uint64(11)
	var ranges [][2]uint64
	db := &validationDBStub{read: func(filter store.LogFilter) ([]*store.Log, error) {
		ranges = append(ranges, [2]uint64{filter.BlockFrom, filter.BlockTo})
		return nil, nil
	}}
	src := validationSource(t, db, &latest)
	var statuses []string
	err := runLogValidation(context.Background(), LogValidationConfig{BatchSize: 100}, []*logValidationSource{src}, func(value interface{}) error {
		result := value.(logValidationResult)
		statuses = append(statuses, result.Status)
		if result.Status == "matched" {
			require.LessOrEqual(t, result.To, latest)
			if result.To == 11 {
				latest = 14
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, [][2]uint64{{10, 11}, {12, 14}}, ranges)
	require.Equal(t, []string{"matched", "matched", "caught_up"}, statuses)
	require.EqualValues(t, 15, src.next)
	require.True(t, src.done)
}

func TestLogsValidationFailFastWithContext(t *testing.T) {
	latest := uint64(20)
	log := eth.Log{BlockNumber: 10, Address: common.HexToAddress("0x1"), Data: []byte{1}}
	db := &validationDBStub{read: func(store.LogFilter) ([]*store.Log, error) { return []*store.Log{store.ParseEthLog(&log)}, nil }}
	src := validationSource(t, db, &latest)
	otherDB := &validationDBStub{read: func(store.LogFilter) ([]*store.Log, error) { return nil, nil }}
	other := validationSource(t, otherDB, &latest)
	var saved logValidationResult
	err := runLogValidation(context.Background(), LogValidationConfig{BatchSize: 2}, []*logValidationSource{src, other}, func(value interface{}) error {
		saved = value.(logValidationResult)
		return nil
	})
	require.Error(t, err)
	require.Equal(t, "mismatched", saved.Status)
	require.Len(t, saved.DBRows, 1)
	require.Len(t, saved.DBLogs, 1)
	require.Len(t, saved.DBOnly, 1)
	require.EqualValues(t, 20, saved.Latest)
	require.EqualValues(t, 10, saved.From)
	require.EqualValues(t, 11, saved.To)
	require.NotNil(t, saved.Filter)
	require.Equal(t, 1, db.calls)
	require.Zero(t, otherDB.calls)
	require.EqualValues(t, 10, src.next)
}

func TestLogsValidationOversizedSplitsRange(t *testing.T) {
	latest := uint64(20)
	var ranges [][2]uint64
	db := &validationDBStub{read: func(filter store.LogFilter) ([]*store.Log, error) {
		ranges = append(ranges, [2]uint64{filter.BlockFrom, filter.BlockTo})
		if filter.BlockTo-filter.BlockFrom+1 > 2 {
			return nil, store.ErrFilterResultSetTooLarge
		}
		return nil, nil
	}}
	src := validationSource(t, db, &latest)
	result, err := src.validateNext(context.Background(), 8)
	require.NoError(t, err)
	require.Equal(t, "matched", result.Status)
	require.Equal(t, [][2]uint64{{10, 17}, {10, 13}, {10, 11}}, ranges)
}

func TestLogsValidationCoreEpochBlockRange(t *testing.T) {
	latest := uint64(20)
	var filter store.LogFilter
	db := &validationDBStub{read: func(f store.LogFilter) ([]*store.Log, error) { filter = f; return nil, nil }}
	src := validationSource(t, db, &latest)
	src.space = "core"
	src.rpc = validationRPC(t, func(method string, params []json.RawMessage) interface{} {
		switch method {
		case "cfx_getBlocksByEpoch":
			return []cfx.Hash{"0x01", "0x02"}
		case "cfx_getBlockByHash":
			return map[string]string{"blockNumber": "0x1f4"}
		case "cfx_getBlockByEpochNumber":
			return map[string]string{"blockNumber": "0x1f9"}
		case "cfx_getLogs":
			var f map[string]string
			if err := json.Unmarshal(params[0], &f); err != nil {
				t.Error(err)
			}
			if f["fromEpoch"] != "0xa" || f["toEpoch"] != "0xb" {
				t.Errorf("unexpected filter %v", f)
			}
			return []cfx.Log{}
		default:
			t.Errorf("unexpected method %s", method)
			return nil
		}
	})
	result, err := src.validateNext(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, "matched", result.Status)
	require.EqualValues(t, 500, filter.BlockFrom)
	require.EqualValues(t, 505, filter.BlockTo)
}

func TestLogsValidationRejectsChangingOrUnavailableRange(t *testing.T) {
	for _, scenario := range []string{"reorg", "null", "pruned", "empty_db"} {
		t.Run(scenario, func(t *testing.T) {
			latest := uint64(20)
			db := &validationDBStub{}
			db.read = func(store.LogFilter) ([]*store.Log, error) {
				if scenario == "reorg" {
					db.version++
				}
				return nil, nil
			}
			src := validationSource(t, db, &latest)
			if scenario == "null" {
				src.rpc = validationRPC(t, func(string, []json.RawMessage) interface{} { return nil })
			}
			if scenario == "pruned" {
				src.bounds = func() (uint64, uint64, bool, error) { return 11, 20, true, nil }
			}
			if scenario == "empty_db" {
				src.bounds = func() (uint64, uint64, bool, error) { return 0, 0, false, nil }
			}
			result := logValidationResult{From: 10}
			err := src.validateRange(context.Background(), 2, &result)
			require.Error(t, err)
			require.False(t, errors.Is(err, errResultNotMatched))
			require.NotEqual(t, "matched", result.Status)
		})
	}
}

func TestLogsValidationComparesDuplicatesAndFields(t *testing.T) {
	log := eth.Log{BlockNumber: 10, Data: []byte{1}}
	a, err := canonicalValidationLog(log)
	require.NoError(t, err)
	log.Data = []byte{2}
	b, err := canonicalValidationLog(log)
	require.NoError(t, err)
	dbOnly, fnOnly := diffLogValidation([]json.RawMessage{a, b, a}, []json.RawMessage{b, a})
	require.Equal(t, []json.RawMessage{a}, dbOnly)
	require.Empty(t, fnOnly)
	dbOnly, fnOnly = diffLogValidation([]json.RawMessage{a}, []json.RawMessage{b})
	require.Len(t, dbOnly, 1)
	require.Len(t, fnOnly, 1)
	log.Topics = []common.Hash{}
	c, err := canonicalValidationLog(log)
	require.NoError(t, err)
	require.Equal(t, b, c)
}

func TestLogsValidationCoreKeepsTransactionIndex(t *testing.T) {
	log := cfx.Log{TransactionIndex: cfx.NewBigInt(1)}
	// A real address is required by the SDK's JSON encoder.
	require.NoError(t, log.Address.UnmarshalJSON([]byte(`"cfxtest:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa6f0vrcsw"`)))
	a, err := canonicalValidationLog(normalizeCoreValidationLog(&log, 100))
	require.NoError(t, err)
	log.TransactionIndex = cfx.NewBigInt(2)
	b, err := canonicalValidationLog(normalizeCoreValidationLog(&log, 100))
	require.NoError(t, err)
	require.NotEqual(t, a, b)
	c, err := canonicalValidationLog(normalizeCoreValidationLog(&log, 101))
	require.NoError(t, err)
	require.NotEqual(t, b, c)
}

func TestLogsValidationCoreDetectsWrongBlockNumber(t *testing.T) {
	for _, bn := range []uint64{500, 501} {
		latest := uint64(20)
		hash := cfx.Hash(common.HexToHash("0x1").Hex())
		log := cfx.Log{BlockHash: &hash, EpochNumber: cfx.NewBigInt(10), LogIndex: cfx.NewBigInt(0)}
		require.NoError(t, log.Address.UnmarshalJSON([]byte(`"cfxtest:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa6f0vrcsw"`)))
		db := &validationDBStub{read: func(store.LogFilter) ([]*store.Log, error) {
			return []*store.Log{store.ParseCfxLog(&log, bn)}, nil
		}}
		src := validationSource(t, db, &latest)
		src.space = "core"
		src.rpc = validationRPC(t, func(method string, _ []json.RawMessage) interface{} {
			switch method {
			case "cfx_getBlocksByEpoch":
				return []cfx.Hash{hash}
			case "cfx_getBlockByHash":
				return map[string]string{"blockNumber": "0x1f4"}
			case "cfx_getBlockByEpochNumber":
				return map[string]string{"blockNumber": "0x1f9"}
			case "cfx_getLogs":
				return []cfx.Log{log}
			default:
				t.Errorf("unexpected method %s", method)
				return nil
			}
		})
		result, err := src.validateNext(context.Background(), 2)
		if bn == 500 {
			require.NoError(t, err)
			require.Equal(t, "matched", result.Status)
		} else {
			require.ErrorIs(t, err, errResultNotMatched)
			require.Len(t, result.DBOnly, 1)
			require.Len(t, result.FullnodeOnly, 1)
		}
	}
}
