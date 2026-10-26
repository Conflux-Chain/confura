package test

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util/blacklist"
	cfx "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
	eth "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// LogValidationConfig uses Core epoch numbers for both spaces.
type LogValidationConfig struct {
	Space, CoreEndpoint, EspaceEndpoint, Report string
	FromEpoch, BatchSize                        uint64
}

type logValidationDB interface {
	GetLogs(context.Context, store.LogFilter) ([]*store.Log, error)
	GetReorgVersion() (int, error)
}

type logValidationSource struct {
	space    string
	db       logValidationDB
	bounds   func() (first, latest uint64, exists bool, err error)
	close    func() error
	rpc      *gethrpc.Client
	endpoint string
	next     uint64
	done     bool
	batches  uint64
	logs     uint64
}

// logValidationResult deliberately keeps complete inputs on failure, so the
// first mismatch can be investigated without re-running the affected range.
type logValidationResult struct {
	Status        string            `json:"status"`
	Space         string            `json:"space"`
	From          uint64            `json:"fromEpoch"`
	To            uint64            `json:"toEpoch"`
	Latest        uint64            `json:"dbLatest"`
	BlockFrom     uint64            `json:"blockFrom"`
	BlockTo       uint64            `json:"blockTo"`
	Filter        interface{}       `json:"fullnodeFilter,omitempty"`
	DBCount       int               `json:"dbCount"`
	FullnodeCount int               `json:"fullnodeCount"`
	Excluded      int               `json:"excludedFullnodeLogs"`
	DBRows        []*store.Log      `json:"dbRows,omitempty"`
	DBLogs        []json.RawMessage `json:"dbLogs,omitempty"`
	FullnodeLogs  []json.RawMessage `json:"fullnodeLogs,omitempty"`
	DBOnly        []json.RawMessage `json:"dbOnly,omitempty"`
	FullnodeOnly  []json.RawMessage `json:"fullnodeOnly,omitempty"`
	Error         string            `json:"error,omitempty"`
}

func openLogValidationSource[T store.ChainData](space string, cfg *mysql.Config, filter store.ChainDataFilter) (*logValidationSource, error) {
	if !cfg.Enabled {
		return nil, errors.Errorf("%s mysql store is not enabled", space)
	}
	conn, err := cfg.Open()
	if err != nil {
		return nil, errors.WithMessage(err, "open "+space+" database")
	}
	db := mysql.NewMysqlStore[T](conn, cfg, filter)
	return &logValidationSource{
		space: space, db: db, close: db.Close,
		bounds: func() (uint64, uint64, bool, error) {
			first, exists, err := db.EarliestBlockMapping()
			if err != nil || !exists {
				return 0, 0, exists, err
			}
			latest, exists, err := db.LatestBlockMapping()
			return first.Epoch, latest.Epoch, exists, err
		},
	}, nil
}

// ValidateLogs scans each space independently, refreshing DB progress before
// every batch. It stops at the first mismatch or unrecoverable query error.
func ValidateLogs(ctx context.Context, conf LogValidationConfig) (err error) {
	if conf.BatchSize == 0 {
		return errors.New("test.logs.batchSize must be positive")
	}
	if conf.Space != "core" && conf.Space != "espace" && conf.Space != "both" {
		return errors.New("space must be core, espace or both")
	}
	if conf.Report == "" || (conf.Space != "espace" && conf.CoreEndpoint == "") ||
		(conf.Space != "core" && conf.EspaceEndpoint == "") {
		return errors.New("report and the selected spaces' fullnode endpoints are required")
	}
	report, err := os.OpenFile(conf.Report, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return errors.WithMessage(err, "create log validation report")
	}
	defer func() { err = stderrors.Join(err, report.Close()) }()
	write := func(value interface{}) error {
		if err := json.NewEncoder(report).Encode(value); err != nil {
			return errors.WithMessage(err, "write log validation report")
		}
		return report.Sync()
	}

	var sources []*logValidationSource
	defer func() {
		for _, src := range sources {
			if src.rpc != nil {
				src.rpc.Close()
			}
			err = stderrors.Join(err, src.close())
			logrus.WithFields(logrus.Fields{"space": src.space, "fromEpoch": conf.FromEpoch,
				"nextEpoch": src.next, "batches": src.batches, "logs": src.logs,
				"caughtUp": src.done, "report": conf.Report}).Info("Event logs validation summary")
		}
	}()
	for _, space := range []string{"core", "espace"} {
		if conf.Space != "both" && conf.Space != space {
			continue
		}
		var src *logValidationSource
		if space == "core" {
			src, err = openLogValidationSource[*store.EpochData](space, mysql.MustNewConfigFromViper(), store.StoreConfig())
		} else {
			src, err = openLogValidationSource[store.EthData](space, mysql.MustNewEthStoreConfigFromViper(), store.EthStoreConfig())
		}
		if err != nil {
			return err
		}
		sources = append(sources, src)
		src.next, src.endpoint = conf.FromEpoch, conf.CoreEndpoint
		if space == "espace" {
			src.endpoint = conf.EspaceEndpoint
		}
		src.rpc, err = gethrpc.DialContext(ctx, src.endpoint)
		if err != nil {
			return errors.New(strings.ReplaceAll(err.Error(), src.endpoint, "[fullnode]"))
		}
	}
	return runLogValidation(ctx, conf, sources, write)
}

func runLogValidation(ctx context.Context, conf LogValidationConfig, sources []*logValidationSource, write func(interface{}) error) error {
	for {
		active := false
		for _, src := range sources {
			if src.done {
				continue
			}
			active = true
			result, checkErr := src.validateNext(ctx, conf.BatchSize)
			if checkErr != nil {
				result.Status = "error"
				if errors.Is(checkErr, errResultNotMatched) {
					result.Status = "mismatched"
				}
				result.Error = strings.ReplaceAll(checkErr.Error(), src.endpoint, "[fullnode]")
				logrus.WithFields(logrus.Fields{"space": src.space, "fromEpoch": result.From,
					"toEpoch": result.To, "status": result.Status, "report": conf.Report}).Error("Event logs validation stopped")
				return stderrors.Join(errors.New(result.Error), write(result))
			}
			if result.Status == "caught_up" {
				src.done = true
			} else {
				src.batches++
				src.logs += uint64(result.DBCount)
				if result.To == math.MaxUint64 {
					src.done = true
				} else {
					src.next = result.To + 1
				}
			}
			// Full payloads are needed only on failure.
			result.DBRows, result.DBLogs, result.FullnodeLogs = nil, nil, nil
			if err := write(result); err != nil {
				return err
			}
			logrus.WithField("result", result).Info("Event logs validation")
		}
		if !active {
			return nil
		}
		if err := waitLogValidation(ctx, 200*time.Millisecond); err != nil {
			return err
		}
	}
}

func (src *logValidationSource) validateNext(ctx context.Context, size uint64) (result logValidationResult, err error) {
	for attempt := 0; attempt < 3; attempt++ {
		result = logValidationResult{Space: src.space, From: src.next, To: src.next}
		err = src.validateRange(ctx, size, &result)
		if err == nil || errors.Is(err, errResultNotMatched) || ctx.Err() != nil {
			return result, err
		}
		if isLogValidationOversized(err) && result.To > result.From {
			size = (result.To - result.From + 1) / 2
			attempt = -1 // splitting does not consume the transient retry budget
			continue
		}
		if attempt < 2 {
			if waitErr := waitLogValidation(ctx, time.Second); waitErr != nil {
				return result, waitErr
			}
		}
	}
	return result, err
}

func (src *logValidationSource) validateRange(ctx context.Context, size uint64, result *logValidationResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	version, err := src.db.GetReorgVersion()
	if err != nil {
		return err
	}
	first, latest, exists, err := src.bounds()
	if err != nil {
		return err
	}
	result.Latest = latest
	if !exists {
		return errors.New("database has no synchronized range")
	}
	if result.From < first {
		return errors.Errorf("epoch %d precedes retained database range starting at %d", result.From, first)
	}
	if result.From > latest {
		result.Status = "caught_up"
		return nil
	}
	result.To = result.From + min(size-1, latest-result.From)
	result.BlockFrom, result.BlockTo = result.From, result.To
	if src.space == "core" {
		if err := src.coreBlockRange(ctx, result); err != nil {
			return err
		}
		result.Filter = map[string]string{"fromEpoch": hexutil.EncodeUint64(result.From), "toEpoch": hexutil.EncodeUint64(result.To)}
	} else {
		result.Filter = map[string]string{"fromBlock": hexutil.EncodeUint64(result.From), "toBlock": hexutil.EncodeUint64(result.To)}
	}
	result.DBRows, err = src.db.GetLogs(ctx, store.LogFilter{BlockFrom: result.BlockFrom, BlockTo: result.BlockTo})
	if err != nil {
		return errors.WithMessage(err, "read database logs")
	}
	result.DBLogs, err = src.databaseLogs(result.DBRows)
	if err != nil {
		return err
	}
	result.DBCount = len(result.DBLogs)
	if err := src.fullnodeLogs(ctx, result); err != nil {
		return errors.WithMessage(err, "read fullnode logs")
	}
	result.FullnodeCount = len(result.FullnodeLogs)
	// Detect rollback/pruning while reading; never label a moving range as corrupt.
	first, latest, exists, err = src.bounds()
	if err != nil {
		return err
	}
	currentVersion, err := src.db.GetReorgVersion()
	if err != nil {
		return err
	}
	if version != currentVersion || !exists || first > result.From || latest < result.To {
		return errors.New("database range changed during validation; retrying")
	}
	result.DBOnly, result.FullnodeOnly = diffLogValidation(result.DBLogs, result.FullnodeLogs)
	if len(result.DBOnly)+len(result.FullnodeOnly) != 0 {
		return errResultNotMatched
	}
	result.Status = "matched"
	return nil
}

func (src *logValidationSource) coreBlockRange(ctx context.Context, result *logValidationResult) error {
	var hashes []cfx.Hash
	if err := src.rpc.CallContext(ctx, &hashes, "cfx_getBlocksByEpoch", hexutil.EncodeUint64(result.From)); err != nil {
		return err
	}
	if len(hashes) == 0 {
		return errors.New("fullnode returned no blocks for Core epoch")
	}
	var first, last *struct {
		BlockNumber *hexutil.Uint64 `json:"blockNumber"`
	}
	if err := src.rpc.CallContext(ctx, &first, "cfx_getBlockByHash", hashes[0], false); err != nil {
		return err
	}
	if err := src.rpc.CallContext(ctx, &last, "cfx_getBlockByEpochNumber", hexutil.EncodeUint64(result.To), false); err != nil {
		return err
	}
	if first == nil || last == nil || first.BlockNumber == nil || last.BlockNumber == nil || *first.BlockNumber > *last.BlockNumber {
		return errors.New("fullnode returned invalid Core block range")
	}
	result.BlockFrom, result.BlockTo = uint64(*first.BlockNumber), uint64(*last.BlockNumber)
	return nil
}

func (src *logValidationSource) databaseLogs(rows []*store.Log) ([]json.RawMessage, error) {
	logs := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		var value interface{}
		if src.space == "core" {
			log, err := row.ToCfxLog()
			if err != nil {
				return logs, err
			}
			value = normalizeCoreValidationLog(log, row.BlockNumber)
		} else {
			log, err := row.ToEthLog()
			if err != nil {
				return logs, err
			}
			value = log
		}
		encoded, err := canonicalValidationLog(value)
		if err != nil {
			return logs, err
		}
		logs = append(logs, encoded)
	}
	return logs, nil
}

func (src *logValidationSource) fullnodeLogs(ctx context.Context, result *logValidationResult) error {
	if src.space == "core" {
		var logs []cfx.Log
		if err := src.rpc.CallContext(ctx, &logs, "cfx_getLogs", result.Filter); err != nil {
			return err
		}
		if logs == nil {
			return errors.New("fullnode returned null logs")
		}
		blockNumbers := make(map[cfx.Hash]uint64)
		for _, log := range logs {
			if log.EpochNumber == nil || log.BlockHash == nil {
				return errors.New("fullnode log is missing epochNumber or blockHash")
			}
			if blacklist.IsAddressBlacklisted(&log.Address, log.EpochNumber.ToInt().Uint64()) {
				result.Excluded++
				continue
			}
			bn, exists := blockNumbers[*log.BlockHash]
			if !exists {
				var block *struct {
					BlockNumber *hexutil.Uint64 `json:"blockNumber"`
				}
				if err := src.rpc.CallContext(ctx, &block, "cfx_getBlockByHash", *log.BlockHash, false); err != nil {
					return err
				}
				if block == nil || block.BlockNumber == nil {
					return errors.New("fullnode returned no block number for Core log")
				}
				bn = uint64(*block.BlockNumber)
				blockNumbers[*log.BlockHash] = bn
			}
			encoded, err := canonicalValidationLog(normalizeCoreValidationLog(&log, bn))
			if err != nil {
				return err
			}
			result.FullnodeLogs = append(result.FullnodeLogs, encoded)
		}
	} else {
		var logs []eth.Log
		if err := src.rpc.CallContext(ctx, &logs, "eth_getLogs", result.Filter); err != nil {
			return err
		}
		if logs == nil {
			return errors.New("fullnode returned null logs")
		}
		for _, log := range logs {
			encoded, err := canonicalValidationLog(log)
			if err != nil {
				return err
			}
			result.FullnodeLogs = append(result.FullnodeLogs, encoded)
		}
	}
	return nil
}

func normalizeCoreValidationLog(log *cfx.Log, blockNumber uint64) interface{} {
	// Core getLogs can return a transactionIndex inconsistent with the receipt.
	// Ignore it as the existing Core validator does. Space is not persisted.
	normalized := *log
	normalized.TransactionIndex = nil
	normalized.Space = nil
	return struct {
		*cfx.Log
		Address     string         `json:"address"`
		BlockNumber hexutil.Uint64 `json:"blockNumber"`
	}{&normalized, log.Address.GetHexAddress(), hexutil.Uint64(blockNumber)}
}

func canonicalValidationLog(value interface{}) (json.RawMessage, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var fields map[string]interface{}
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	if fields["topics"] == nil {
		fields["topics"] = []interface{}{}
	}
	for key, value := range fields {
		if str, ok := value.(string); ok && strings.HasPrefix(strings.ToLower(str), "0x") {
			fields[key] = strings.ToLower(str)
		}
	}
	if topics, ok := fields["topics"].([]interface{}); ok {
		for i, topic := range topics {
			if str, ok := topic.(string); ok {
				topics[i] = strings.ToLower(str)
			}
		}
	}
	return json.Marshal(fields)
}

// Compare multisets rather than maps: repeated rows must not disappear.
func diffLogValidation(db, fn []json.RawMessage) (dbOnly, fnOnly []json.RawMessage) {
	db = append([]json.RawMessage(nil), db...)
	fn = append([]json.RawMessage(nil), fn...)
	sort.Slice(db, func(i, j int) bool { return string(db[i]) < string(db[j]) })
	sort.Slice(fn, func(i, j int) bool { return string(fn[i]) < string(fn[j]) })
	i, j := 0, 0
	for i < len(db) && j < len(fn) {
		switch strings.Compare(string(db[i]), string(fn[j])) {
		case -1:
			dbOnly = append(dbOnly, db[i])
			i++
		case 1:
			fnOnly = append(fnOnly, fn[j])
			j++
		default:
			i++
			j++
		}
	}
	return append(dbOnly, db[i:]...), append(fnOnly, fn[j:]...)
}

func isLogValidationOversized(err error) bool {
	if errors.Is(err, store.ErrFilterQuerySetTooLarge) || errors.Is(err, store.ErrFilterResultSetTooLarge) {
		return true
	}
	message := strings.ToLower(err.Error())
	for _, fragment := range []string{"too many", "too large", "exceeds", "exceed the", "response size", "block range", "epoch range"} {
		if strings.Contains(message, fragment) {
			return true
		}
	}
	return false
}

func waitLogValidation(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
