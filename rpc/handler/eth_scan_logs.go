package handler

import (
	"context"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	web3types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

type EthBlockRange struct {
	From *web3types.BlockNumber `json:"fromBlock,omitempty"`
	To   *web3types.BlockNumber `json:"toBlock,omitempty"`
}

func (r *EthBlockRange) UnmarshalJSON(data []byte) error {
	type plain EthBlockRange
	var decoded plain
	if err := unmarshalStrictJSONObject(data, &decoded); err != nil {
		return errors.WithMessage(err, "invalid eSpace block range")
	}
	if err := validateJSONObjectFields(data, nil, []string{"fromBlock", "toBlock"}); err != nil {
		return err
	}
	*r = EthBlockRange(decoded)
	return nil
}

type EthScanLogFilter struct {
	BlockRange *EthBlockRange  `json:"blockRange,omitempty"`
	Address    *common.Address `json:"address,omitempty"`
	Topic0     *common.Hash    `json:"topic0,omitempty"`
}

func (f *EthScanLogFilter) UnmarshalJSON(data []byte) error {
	type plain EthScanLogFilter
	var decoded plain
	if err := unmarshalStrictJSONObject(data, &decoded); err != nil {
		return errors.WithMessage(err, "invalid eSpace scan filter")
	}
	if err := validateJSONObjectFields(data, nil, []string{"blockRange", "address", "topic0"}); err != nil {
		return err
	}
	*f = EthScanLogFilter(decoded)
	return nil
}

type EthScanLogRequest struct {
	Filter  EthScanLogFilter `json:"filter"`
	Limit   hexutil.Uint64   `json:"limit"`
	Cursor  *ScanLogCursor   `json:"cursor,omitempty"`
	Reverse bool             `json:"reverse,omitempty"`
}

func (r EthScanLogRequest) ContractAddress() (string, bool) {
	if r.Filter.Address == nil {
		return "", false
	}
	return r.Filter.Address.String(), true
}

func (r *EthScanLogRequest) UnmarshalJSON(data []byte) error {
	type plain EthScanLogRequest
	var decoded plain
	if err := unmarshalStrictJSONObject(data, &decoded); err != nil {
		return errors.WithMessage(err, "invalid eSpace scan request")
	}
	if err := validateJSONObjectFields(
		data,
		[]string{"filter"},
		[]string{"filter", "limit", "cursor", "reverse"},
	); err != nil {
		return err
	}
	*r = EthScanLogRequest(decoded)
	return nil
}

type EthScanLogParams struct {
	*EthScanLogRequest
	BlockRange          types.RangeUint64
	WithPivotAssumption bool
}

// EthPivotAssumption identifies one canonical eSpace block and doubles as the
// output guard shape.
type EthPivotAssumption struct {
	BlockNumber hexutil.Uint64 `json:"blockNumber"`
	BlockHash   common.Hash    `json:"blockHash"`
}

func (a *EthPivotAssumption) UnmarshalJSON(data []byte) error {
	type plain EthPivotAssumption
	var decoded plain
	if err := unmarshalStrictJSONObject(data, &decoded); err != nil {
		return errors.WithMessage(err, "invalid eSpace pivot assumption")
	}
	if err := validateJSONObjectFields(
		data,
		[]string{"blockNumber", "blockHash"},
		[]string{"blockNumber", "blockHash"},
	); err != nil {
		return err
	}
	*a = EthPivotAssumption(decoded)
	return nil
}

type EthPivotGuard EthPivotAssumption

// EthScanLogResult keeps native web3go logs. The unexported usage field tracks
// the internal canonical read-set and is never serialized in the RPC response.
type EthScanLogResult struct {
	Logs       []web3types.Log `json:"logs"`
	NextCursor *ScanLogCursor  `json:"nextCursor,omitempty"`
	PivotGuard *EthPivotGuard  `json:"pivotGuard,omitempty"`
	usage      ethScanUsage
}

// ethScanClient is the minimal node surface used by Reader and consistency
// orchestration.
type ethScanClient interface {
	BlockByNumber(web3types.BlockNumber, bool) (*web3types.Block, error)
	Logs(web3types.FilterQuery) ([]web3types.Log, error)
}

type ethBlockNumberResolver interface {
	BlockByNumber(web3types.BlockNumber, bool) (*web3types.Block, error)
}

func NormalizeEthScanLogRequest(
	resolver ethBlockNumberResolver,
	hardfork web3types.BlockNumber,
	req EthScanLogRequest,
	withPivotAssumption bool,
) (EthScanLogParams, error) {
	effectiveLimit, err := normalizeScanLogsLimit(req.Limit)
	if err != nil {
		return EthScanLogParams{}, err
	}
	req.Limit = effectiveLimit

	from, to := web3types.LatestBlockNumber, web3types.LatestBlockNumber
	if req.Filter.BlockRange != nil {
		if req.Filter.BlockRange.From != nil {
			from = *req.Filter.BlockRange.From
		}
		if req.Filter.BlockRange.To != nil {
			to = *req.Filter.BlockRange.To
		}
	}

	resolvedTags := make(map[web3types.BlockNumber]uint64)
	resolveBlock := func(blockNum web3types.BlockNumber) (uint64, error) {
		if blockNum > 0 {
			return uint64(blockNum), nil
		}

		if resolved, ok := resolvedTags[blockNum]; ok {
			return resolved, nil
		}

		block, err := resolver.BlockByNumber(blockNum, false)
		if err != nil {
			return 0, errors.WithMessagef(err, "failed to resolve block by tag %s", blockNum)
		}
		if block == nil || block.Number == nil {
			return 0, errors.WithMessagef(
				ErrScanLogsInvalidParams,
				"unavailable block by tag %s", blockNum,
			)
		}
		resolved := block.Number.Uint64()
		resolvedTags[blockNum] = resolved
		return resolved, nil
	}

	fromNumber, err := resolveBlock(from)
	if err != nil {
		return EthScanLogParams{}, err
	}
	toNumber, err := resolveBlock(to)
	if err != nil {
		return EthScanLogParams{}, err
	}
	if fromNumber > toNumber {
		return EthScanLogParams{}, errors.WithMessagef(
			ErrScanLogsInvalidParams,
			"invalid block range: from block %s exceeds to block %s", from, to,
		)
	}

	// A numeric upper bound is explicit caller input. Compare it with one frozen
	// latest head and fail instead of truncating the requested range.
	if to >= 0 {
		latest, err := resolveBlock(web3types.LatestBlockNumber)
		if err != nil {
			return EthScanLogParams{}, err
		}
		if toNumber > latest {
			return EthScanLogParams{}, errors.WithMessagef(
				ErrScanLogsInvalidParams,
				"explicit toBlock %d exceeds frozen latest %d", toNumber, latest,
			)
		}
	}

	// eSpace has no logs before the transition height. Preserve a wholly
	// pre-transition request as empty instead of moving it onto the hardfork block.
	var effectiveBlockRange scanRange
	if hardforkNumber := uint64(hardfork); toNumber <= hardforkNumber {
		effectiveBlockRange = emptyScanRange
	} else {
		effectiveBlockRange = scanRange{
			From: max(fromNumber, hardforkNumber),
			To:   toNumber,
		}
	}

	if req.Cursor != nil && !effectiveBlockRange.contains(uint64(req.Cursor.BlockNumber)) {
		return EthScanLogParams{}, errors.WithMessage(
			ErrScanLogsInvalidCursor, "cursor is outside the normalized block range",
		)
	}

	return EthScanLogParams{
		EthScanLogRequest:   &req,
		BlockRange:          types.RangeUint64(effectiveBlockRange),
		WithPivotAssumption: withPivotAssumption,
	}, nil
}

type ethScanGeneration struct {
	// All fields are captured under one DB v0. dbPivot is the split identity
	// compared with FN when a candidate actually consumes both canonical views.
	dbAvailable   bool
	dbBlocks      scanRange
	fnBlocks      scanRange
	requestBlocks scanRange
	dbMinBlock    uint64
	dbMaxBlock    uint64
	dbPivot       common.Hash
	plan          scanPlan
	owner         cursorOwner
}

type ethScanUsage struct {
	db bool
	fn bool
}

func (r *EthScanLogResult) canonicalUsageDB() bool { return r != nil && r.usage.db }
func (r *EthScanLogResult) canonicalUsageFN() bool { return r != nil && r.usage.fn }

// ethFNFilter adapts the public scan filter to FilterQuery only at the FN
// Reader boundary. The Reader owns FromBlock/ToBlock for each physical window.
func ethFNFilter(scanFilter EthScanLogFilter) web3types.FilterQuery {
	filter := web3types.FilterQuery{}

	if scanFilter.Address != nil {
		filter.Addresses = []common.Address{*scanFilter.Address}
	}
	if scanFilter.Topic0 != nil {
		filter.Topics = [][]common.Hash{{*scanFilter.Topic0}}
	}
	return filter
}

// buildEthGeneration captures one DB-version-specific block split. The RPC
// adapter has already resolved tags and rejected a future numeric upper bound;
// this layer must not resolve latest again because doing so would change the
// meaning of a normalized safe/finalized request.
func (handler *EthLogsApiHandler) buildEthGeneration(req EthScanLogParams) (ethScanGeneration, error) {
	cursor := req.Cursor.toStoreCursor()
	if scanRange(req.BlockRange).empty() {
		if cursor != nil {
			return ethScanGeneration{}, errors.WithMessage(
				ErrScanLogsInvalidCursor, "cursor is outside of the empty request range",
			)
		}
		return newEthFullnodeGeneration(emptyScanRange, nil, req.Reverse), nil
	}

	earliest, ok, err := handler.es.EarliestBlockMapping()
	if err != nil {
		return ethScanGeneration{}, err
	}
	if !ok {
		// Without an earliest watermark Store cannot prove that it owns any
		// subrange. Keep the already-normalized request intact and let FN serve the
		// whole range; do not invent a DB split from zero-value mappings.
		gen := newEthFullnodeGeneration(scanRange(req.BlockRange), cursor, req.Reverse)
		if cursor != nil && gen.fnBlocks.empty() {
			return ethScanGeneration{}, errors.WithMessage(
				ErrScanLogsInvalidCursor, "cursor is outside of the request range",
			)
		}
		return gen, nil
	}

	latest, ok, err := handler.es.LatestBlockMapping()
	if err != nil {
		return ethScanGeneration{}, err
	}
	if !ok {
		// Both endpoints come from the same mapping table. Earliest-without-latest
		// is not an ordinary empty Store; it is a cross-generation read or a broken
		// Store invariant and must pass through the outer v0/v1 error gate.
		return ethScanGeneration{}, errors.WithMessage(
			ErrScanLogsConsistency, "latest mapping is unavailable while earliest mapping exists",
		)
	}

	if req.BlockRange.From < earliest.BnMin {
		// Never silently clip pruned history: a short clipped page is
		// indistinguishable from true exhaustion to a cursor client.
		return ethScanGeneration{}, store.ErrAlreadyPruned
	}

	gen := ethScanGeneration{
		dbAvailable: true,
		dbBlocks: scanRange{
			From: max(req.BlockRange.From, earliest.BnMin),
			To:   min(req.BlockRange.To, latest.BnMax),
		},
		dbMinBlock:    earliest.BnMin,
		dbMaxBlock:    latest.BnMax,
		requestBlocks: scanRange(req.BlockRange),
		dbPivot:       common.HexToHash(latest.PivotHash),
	}

	// Block tags and future numeric bounds are normalized/validated by RPC before
	// this call. Handler planning only splits the frozen effective range at the
	// DB watermark and must not read a newer latest head of its own.
	if latest.BnMax >= req.BlockRange.To {
		gen.fnBlocks = emptyScanRange
	} else {
		gen.fnBlocks = scanRange{
			From: max(req.BlockRange.From, latest.BnMax+1),
			To:   req.BlockRange.To,
		}
	}

	owner, err := classifyCursorOwner(cursor, gen.dbBlocks, latest.BnMax)
	if err != nil {
		return ethScanGeneration{}, errors.WithMessage(err, "invalid cursor placement")
	}
	if owner == cursorOwnerFN && gen.fnBlocks.empty() {
		return ethScanGeneration{}, errors.WithMessage(
			ErrScanLogsInvalidCursor, "cursor is outside of the segment range",
		)
	}
	gen.owner = owner

	gen.plan = buildScanPlan(!gen.dbBlocks.empty(), !gen.fnBlocks.empty(), req.Reverse, owner, cursor)
	return gen, nil
}

func newEthFullnodeGeneration(
	requestBlockRange scanRange, cursor *store.ScanCursor, reverse bool,
) ethScanGeneration {
	owner := cursorOwnerNone
	if cursor != nil {
		owner = cursorOwnerFN
	}

	return ethScanGeneration{
		dbBlocks:      emptyScanRange,
		fnBlocks:      requestBlockRange,
		requestBlocks: requestBlockRange,
		owner:         owner,
		plan:          buildScanPlan(false, !requestBlockRange.empty(), reverse, owner, cursor),
	}
}

// newEthDBCache converts Store rows to native web3go logs immediately. Parallel
// cursor keys let the generic cache return the tail of the prefix consumed by
// the current attempt, not necessarily the end of all previously cached rows.
func (handler *EthLogsApiHandler) newEthDBCache(req EthScanLogParams, gen ethScanGeneration) *dbScanCache[web3types.Log] {
	return &dbScanCache[web3types.Log]{
		initial: gen.plan.cursorFor(scanSourceDB),
		scan: func(ctx context.Context, cursor *store.ScanCursor, limit int) ([]web3types.Log, []store.ScanCursor, error) {
			filter := store.ScanLogFilter{BlockFrom: gen.dbBlocks.From, BlockTo: gen.dbBlocks.To}
			if req.Filter.Address != nil {
				filter.Contract = req.Filter.Address.String()
			}
			if req.Filter.Topic0 != nil {
				filter.Topic0 = req.Filter.Topic0.String()
			}

			raw, err := handler.es.ScanLogs(ctx, store.ScanLogParams{Filter: filter, Cursor: cursor, Reverse: req.Reverse, Limit: limit})
			if err != nil {
				return nil, nil, err
			}

			logs := make([]web3types.Log, 0, len(raw))
			keys := make([]store.ScanCursor, 0, len(raw))
			for _, item := range raw {
				log, err := item.ToEthLog()
				if err != nil {
					return nil, nil, errors.WithMessage(err, "failed to convert DB log to sdk log")
				}

				logs = append(logs, *log)
				keys = append(keys, store.ScanCursor{BlockNumber: item.BlockNumber, LogIndex: item.LogIndex})
			}
			return logs, keys, nil
		},
	}
}

// runEthPlan appends already ordered, non-overlapping segments and records
// actual canonical usage. A segment skipped because remaining == 0 does not
// make the page mixed; an executed empty scan does.
func (handler *EthLogsApiHandler) runEthPlan(
	ctx context.Context, eth ethScanClient, req EthScanLogParams, gen ethScanGeneration,
	dbCache *dbScanCache[web3types.Log],
) (*EthScanLogResult, error) {
	result := &EthScanLogResult{}
	fnFilter := ethFNFilter(req.Filter)

	for _, segment := range gen.plan.segments {
		remaining := int(req.Limit) - len(result.Logs)
		if remaining == 0 {
			break
		}

		switch segment.source {
		case scanSourceDB:
			result.usage.db = true

			if err := dbCache.Ensure(ctx, remaining); err != nil {
				return result, err
			}

			logs := dbCache.Prefix(remaining)
			result.Logs = append(result.Logs, logs...)
			if len(logs) > 0 {
				result.NextCursor = newScanCursor(dbCache.Tail(len(logs)))
			}
		case scanSourceFN:
			result.usage.fn = true

			reader := &ethFNReader{client: eth, spec: ethFNReaderSpec{
				blocks: gen.fnBlocks, filter: fnFilter, cursor: segment.cursor,
				reverse: req.Reverse, windowSize: defaultScanLogsFNWindow,
			}}

			batch, err := reader.Scan(ctx, remaining)
			if err != nil {
				return result, err
			}

			result.Logs = append(result.Logs, batch.Logs...)
			if batch.TailPosition != nil {
				result.NextCursor = newScanCursor(batch.TailPosition)
			}
		}
	}
	return result, nil
}

// finishEthCandidate applies PivotGuard direction rules without additional RPC:
// eSpace logs already contain blockNumber and blockHash. Forward uses the last
// log; reverse first page uses the first/highest log; reverse continuations and
// empty pages preserve the assumption established by the owning DB/FN path.
func finishEthCandidate(
	req EthScanLogParams, assumption *EthPivotAssumption, logs []web3types.Log,
	tail *ScanLogCursor, usage ethScanUsage,
) *EthScanLogResult {
	result := &EthScanLogResult{Logs: logs, NextCursor: tail.clone(), usage: usage}
	if assumption == nil && (!req.WithPivotAssumption || len(logs) == 0) {
		return result
	}

	if len(logs) == 0 || req.Reverse && req.Cursor != nil {
		guard := EthPivotGuard(*assumption)
		result.PivotGuard = &guard
		return result
	}

	log := logs[len(logs)-1]
	if req.Reverse {
		log = logs[0]
	}

	result.PivotGuard = &EthPivotGuard{BlockNumber: hexutil.Uint64(log.BlockNumber), BlockHash: log.BlockHash}
	return result
}

// ScanLogs executes an eSpace scan over an already frozen numeric request.
// It uses the same two-level protocol as Core:
//
//	outer: v0 -> DB coverage/assumption/cache -> v1
//	inner: blockHash(H) before -> FN dependencies/boundary -> blockHash(H) after
//
// FN checkpoint changes rebuild only the inner candidate; DB version changes
// rebuild the generation and invalidate its cache. Success and canonical errors
// share the same commit gate.
func (handler *EthLogsApiHandler) ScanLogs(
	ctx context.Context,
	eth ethScanClient,
	req EthScanLogParams,
	assumption *EthPivotAssumption,
) (result *EthScanLogResult, err error) {
	if req.WithPivotAssumption && req.Cursor != nil && assumption == nil {
		return nil, errors.WithMessage(
			ErrScanLogsInvalidParams, "pivot assumption is required when cursor is provided",
		)
	}
	recorder := newScanLogsMetrics("eth", req.WithPivotAssumption)
	ctx = withScanLogsMetrics(ctx, recorder)
	started := time.Now()
	recorder.Percentage("direction/reverse", req.Reverse)
	recorder.Histogram("limit", int64(req.Limit))
	defer func() {
		recorder.Duration("duration", started)
		recorder.Percentage("stale", errors.Is(err, ErrScanLogsStaleCursor))
		if result == nil {
			return
		}
		recorder.Histogram("result", int64(len(result.Logs)))
		db, fn := result.canonicalUsageDB(), result.canonicalUsageFN()
		recorder.Percentage("source/db", db && !fn)
		recorder.Percentage("source/fn", fn && !db)
		recorder.Percentage("source/mixed", db && fn)
	}()
	return handler.scanLogs(ctx, eth, req, assumption)
}

func (handler *EthLogsApiHandler) scanLogs(
	ctx context.Context,
	eth ethScanClient,
	req EthScanLogParams,
	assumption *EthPivotAssumption,
) (*EthScanLogResult, error) {
	if req.Limit == 0 || uint64(req.Limit) > maxScanLogsLimit {
		return nil, errors.WithMessagef(ErrScanLogsInvalidParams, "scan limit must be in [1, %d]", maxScanLogsLimit)
	}

	ctx, cancel := context.WithTimeout(ctx, store.TimeoutGetLogs)
	defer cancel()

	for {
		if err := checkTimeout(ctx); err != nil {
			return nil, err
		}

		// Every Store-derived fact below belongs to this DB generation. An
		// outer retry rebuilds the split and discards its cache as one unit.
		v0, err := handler.es.GetReorgVersion()
		if err != nil {
			return nil, err
		}

		gen, err := handler.buildEthGeneration(req)
		if err != nil {
			if errors.Is(err, ErrScanLogsConsistency) {
				_, retryOuter, commitErr := handler.commitEthDBGeneration(v0, nil, err)
				if retryOuter {
					markScanLogsMetric(ctx, "retry/db_outer")
					continue
				}
				return nil, commitErr
			}
			return nil, err
		}
		recordScanLogsHistogram(ctx, "plan/segments", int64(len(gen.plan.segments)))
		recordScanLogsHistogram(ctx, "plan/cursor_owner", int64(gen.owner))

		// Cache lifetime is exactly one outer generation. It can survive FN-only
		// retries, but a plan without a DB segment does not allocate one.
		var dbCache *dbScanCache[web3types.Log]
		if gen.plan.contains(scanSourceDB) {
			dbCache = handler.newEthDBCache(req, gen)
		}

		dbAssumption, assumptionErr, err := handler.checkEthDBAssumption(gen, assumption)
		if err != nil {
			return nil, err
		}
		if assumptionErr != nil {
			_, retryOuter, commitErr := handler.commitEthDBGeneration(v0, nil, assumptionErr)
			if retryOuter {
				markScanLogsMetric(ctx, "retry/db_outer")
				continue
			}
			return nil, commitErr
		}
		fnAssumption := assumption != nil && !dbAssumption

		// A full DB-first prefix needs no FN view unless the assumption itself is
		// FN-owned. Materializing it here also makes the prefix reusable by later
		// inner attempts when the page is mixed.
		if dbCache != nil && gen.plan.startsWith(scanSourceDB) {
			if err := dbCache.Ensure(ctx, int(req.Limit)); err != nil {
				return nil, err
			}

			if len(dbCache.logs) == int(req.Limit) && !fnAssumption {
				result := finishEthCandidate(
					req,
					assumption,
					dbCache.logs,
					newScanCursor(dbCache.Tail(len(dbCache.logs))),
					ethScanUsage{db: true},
				)
				result, retryOuter, commitErr := handler.commitEthDBGeneration(v0, result, nil)
				if retryOuter {
					markScanLogsMetric(ctx, "retry/db_outer")
					continue
				}
				return result, commitErr
			}
		}

		// An FN-owned cursor must enter the FN path so its block-range membership
		// can be checked. A deterministic invalid cursor returns immediately and
		// does not consume checkpoint or boundary retries.
		if !gen.plan.contains(scanSourceFN) && !fnAssumption {
			result, provisionalErr := handler.runEthPlan(ctx, eth, req, gen, dbCache)
			if provisionalErr == nil {
				result = finishEthCandidate(req, assumption, result.Logs, result.NextCursor, result.usage)
			}

			result, retryOuter, commitErr := handler.commitEthDBGeneration(v0, result, provisionalErr)
			if retryOuter {
				markScanLogsMetric(ctx, "retry/db_outer")
				continue
			}
			return result, commitErr
		}

		outer := ethOuterState{
			version:      v0,
			gen:          gen,
			dbCache:      dbCache,
			dbAssumption: dbAssumption,
			fnAssumption: fnAssumption,
		}
		result, retryOuter, err := handler.scanEthFullnodeGeneration(
			ctx, eth, req, assumption, outer,
		)
		if retryOuter {
			markScanLogsMetric(ctx, "retry/db_outer")
			continue
		}
		return result, err
	}
}

// ethOuterState is immutable for one DB generation. The cache is the only
// mutable member: it may grow across FN retries, but it is discarded whenever
// version changes and the outer loop rebuilds the generation.
type ethOuterState struct {
	version      int
	gen          ethScanGeneration
	dbCache      *dbScanCache[web3types.Log]
	dbAssumption bool
	fnAssumption bool
}

// ethInnerCandidate keeps a result or canonical-dependent error provisional
// until the FN checkpoint and DB generation have both been closed. Its usage
// is based on reads actually performed, including empty scans and assumptions.
type ethInnerCandidate struct {
	result *EthScanLogResult
	err    error
	usage  ethScanUsage
}

// checkEthDBAssumption validates a DB-owned assumption exactly once in the
// captured generation. A mismatch is a provisional outcome, not an immediate
// return: commitEthDBGeneration must first prove the DB version stayed stable.
func (handler *EthLogsApiHandler) checkEthDBAssumption(
	gen ethScanGeneration,
	assumption *EthPivotAssumption,
) (belongsToDB bool, provisionalErr error, err error) {
	if assumption == nil || !gen.dbAvailable ||
		uint64(assumption.BlockNumber) < gen.dbMinBlock ||
		uint64(assumption.BlockNumber) > gen.dbMaxBlock {
		return false, nil, nil
	}

	pivot, ok, err := handler.es.PivotHash(uint64(assumption.BlockNumber))
	if err != nil {
		return true, nil, errors.WithMessage(err, "failed to get pivot hash")
	}
	if !ok {
		// The captured watermarks advertised this block as covered. Missing its
		// identity is therefore a Store fault, not a stale client assumption.
		return true, errors.WithMessage(
			ErrScanLogsConsistency,
			"pivot mapping is unavailable within captured coverage",
		), nil
	}

	if common.HexToHash(pivot) != assumption.BlockHash {
		return true, errors.WithMessagef(
			ErrScanLogsAssumptionNotMet,
			"expected pivot %s got %s for block %d",
			assumption.BlockHash, pivot, assumption.BlockNumber,
		), nil
	}
	return true, nil, nil
}

// commitEthDBGeneration is the only publication point for a candidate that
// depends solely on Store. It applies the same v0/v1 rule to successes and
// provisional errors so neither can escape from a generation that changed.
func (handler *EthLogsApiHandler) commitEthDBGeneration(
	v0 int,
	result *EthScanLogResult,
	provisionalErr error,
) (*EthScanLogResult, bool, error) {
	v1, err := handler.es.GetReorgVersion()
	if err != nil {
		return nil, false, err
	}
	if v1 != v0 {
		return nil, true, nil
	}

	if provisionalErr != nil {
		return nil, false, provisionalErr
	}
	return result, false, nil
}

// scanEthFullnodeGeneration owns the inner retry loop for one immutable outer
// state. Every pass opens a fresh FN view at the same numeric checkpoint H,
// rebuilds FN-derived data, aligns mixed views, and then either commits the
// candidate or selects the narrowest valid retry scope.
func (handler *EthLogsApiHandler) scanEthFullnodeGeneration(
	ctx context.Context,
	eth ethScanClient,
	req EthScanLogParams,
	assumption *EthPivotAssumption,
	outer ethOuterState,
) (*EthScanLogResult, bool, error) {
	checkpoint := outer.gen.fnBlocks.To
	if outer.gen.fnBlocks.empty() {
		checkpoint = outer.gen.requestBlocks.To
	}
	if outer.fnAssumption && uint64(assumption.BlockNumber) > checkpoint {
		checkpoint = uint64(assumption.BlockNumber)
	}

	boundaryRetries := 0
	for {
		if err := checkTimeout(ctx); err != nil {
			return nil, false, err
		}

		before, err := eth.BlockByNumber(web3types.BlockNumber(checkpoint), false)
		if err != nil {
			return nil, false, errors.WithMessage(err, "failed to get pre-checkpoint block")
		}
		if before == nil {
			if outer.fnAssumption && uint64(assumption.BlockNumber) == checkpoint {
				return nil, false, errors.WithMessagef(
					ErrScanLogsAssumptionNotMet,
					"assumption block %d is unavailable", assumption.BlockNumber,
				)
			}
			return nil, false, errors.WithMessage(
				ErrScanLogsInvalidFilter, "pre-checkpoint block is unavailable",
			)
		}

		candidate, err := handler.buildEthInnerCandidate(
			ctx, eth, req, assumption, outer, checkpoint,
		)
		if err != nil {
			return nil, false, err
		}

		boundaryMismatch := false
		if candidate.usage.db && candidate.usage.fn {
			// Empty scans and auxiliary reads are canonical facts too. Align the
			// DB watermark whenever the candidate consumed both read views.
			boundary, err := eth.BlockByNumber(web3types.BlockNumber(outer.gen.dbMaxBlock), false)
			if err != nil {
				return nil, false, errors.WithMessage(err, "failed to get boundary block")
			}
			boundaryMismatch = (boundary == nil || boundary.Hash != outer.gen.dbPivot)
		}

		// This endpoint fence is optimistic, not a transaction or immutable FN
		// snapshot. before=A/after=B detects a lasting canonical switch, but an
		// intermediate query served from B followed by after=A is an undetectable
		// A->B->A (ABA) window. Boundary alignment has the same limitation.
		// confirmed/safe reduces probability and a caller-selected finalized range
		// is safest; silently capping latest would break short-page exhaustion
		// semantics. Strict prevention needs a node view token or atomic range RPC,
		// which JSON-RPC batch does not provide.
		after, err := eth.BlockByNumber(web3types.BlockNumber(checkpoint), false)
		if err != nil {
			return nil, false, errors.WithMessage(err, "failed to get post-checkpoint block")
		}

		dbStable := true
		if outer.gen.dbAvailable {
			v1, err := handler.es.GetReorgVersion()
			if err != nil {
				return nil, false, errors.WithMessage(err, "failed to get reorg version")
			}
			dbStable = (v1 == outer.version)
		}

		checkpointStable := (after != nil && after.Hash == before.Hash)
		decision := decideCanonicalCommit(dbStable, checkpointStable, !boundaryMismatch)

		switch decision {
		case canonicalRetryOuter:
			return nil, true, nil
		case canonicalRetryInner:
			markScanLogsMetric(ctx, "retry/fn_inner")
			if boundaryMismatch && checkpointStable {
				markScanLogsMetric(ctx, "boundary/mismatch")
				if boundaryRetries >= maxBoundaryInnerRetries {
					markScanLogsMetric(ctx, "boundary/convergence_failure")
					return nil, false, errors.WithMessagef(
						ErrScanLogsConsistency, "mixed boundary mismatch after %d retry", boundaryRetries,
					)
				}
				boundaryRetries++
				if err := waitBoundaryRetry(ctx); err != nil {
					return nil, false, err
				}
			}
			continue
		case canonicalCommit:
			if candidate.err != nil {
				return nil, false, candidate.err
			}
			return candidate.result, false, nil
		default:
			return nil, false, errors.New("unknown canonical commit decision")
		}
	}
}

// buildEthInnerCandidate performs every FN-dependent read between the
// checkpoint's before/after calls. Canonical-dependent outcomes stay in the
// returned candidate; deterministic request, transport and execution errors
// are returned directly.
func (handler *EthLogsApiHandler) buildEthInnerCandidate(
	ctx context.Context,
	eth ethScanClient,
	req EthScanLogParams,
	assumption *EthPivotAssumption,
	outer ethOuterState,
	checkpoint uint64,
) (ethInnerCandidate, error) {
	candidate := ethInnerCandidate{usage: ethScanUsage{db: outer.dbAssumption}}

	if outer.gen.owner == cursorOwnerFN && req.Cursor != nil {
		// eSpace is block-native, so range membership is enough here. The fixed
		// higher checkpoint protects cursor ancestry without another hash lookup.
		candidate.usage.fn = true
		cursorBlock := uint64(req.Cursor.BlockNumber)
		if cursorBlock > checkpoint || !outer.gen.fnBlocks.contains(cursorBlock) {
			return ethInnerCandidate{}, errors.WithMessage(
				ErrScanLogsInvalidCursor, "cursor is outside the request segment",
			)
		}
	}

	if candidate.err == nil {
		result, provisionalErr := handler.runEthPlan(ctx, eth, req, outer.gen, outer.dbCache)
		candidate.result = result
		candidate.err = provisionalErr
		if result != nil {
			candidate.usage.db = candidate.usage.db || result.canonicalUsageDB()
			candidate.usage.fn = candidate.usage.fn || result.canonicalUsageFN()
		}
		if provisionalErr != nil && !isCanonicalDependentError(provisionalErr) {
			return ethInnerCandidate{}, provisionalErr
		}
	}

	if outer.fnAssumption {
		candidate.usage.fn = true
		block, err := eth.BlockByNumber(web3types.BlockNumber(assumption.BlockNumber), false)
		if err != nil {
			return ethInnerCandidate{}, errors.WithMessage(err, "failed to get pivot assumption block")
		}
		if candidate.err == nil && block == nil {
			candidate.err = newCanonicalDependentError(
				ErrScanLogsAssumptionNotMet,
				"pivot assumption block %d is unavailable",
				uint64(assumption.BlockNumber),
			)
		} else if candidate.err == nil && block.Hash != assumption.BlockHash {
			candidate.err = newCanonicalDependentError(
				ErrScanLogsAssumptionNotMet, "pivot assumption does not match",
			)
		}
	}

	if candidate.err == nil {
		candidate.result = finishEthCandidate(
			req,
			assumption,
			candidate.result.Logs,
			candidate.result.NextCursor,
			candidate.usage,
		)
		candidate.usage.db = candidate.result.canonicalUsageDB()
		candidate.usage.fn = candidate.result.canonicalUsageFN()
	}

	return candidate, nil
}

type ethFNReaderSpec struct {
	// blocks and predicate are immutable for one replayable inner attempt.
	blocks     scanRange
	filter     web3types.FilterQuery
	cursor     *store.ScanCursor
	reverse    bool
	windowSize uint64
}

type ethFNReader struct {
	client ethScanClient
	spec   ethFNReaderSpec
}

// Scan walks one eSpace block segment. eth_getLogs is ascending; reverse mode
// visits physical windows high-to-low and reverses each complete response.
// Only a whitelisted oversized error may shrink and retry a window—transport,
// execution and arbitrary JSON-RPC failures are returned unchanged.
func (r *ethFNReader) Scan(ctx context.Context, remaining int) (fnSegmentBatch[web3types.Log], error) {
	if remaining <= 0 || r.spec.blocks.empty() {
		return fnSegmentBatch[web3types.Log]{}, nil
	}

	blocks, err := clipBlockRangeAtCursor(r.spec.blocks, r.spec.cursor, r.spec.reverse)
	if err != nil {
		return fnSegmentBatch[web3types.Log]{}, errors.WithMessage(err, "invalid cursor range")
	}

	var filterFirstWindow func([]web3types.Log) []web3types.Log
	if r.spec.cursor != nil {
		filterFirstWindow = r.applyCursor
	}
	logs, err := scanFNBlockWindows(
		ctx,
		blocks,
		r.spec.reverse,
		r.spec.windowSize,
		remaining,
		r.readWindow,
		filterFirstWindow,
	)
	if err != nil {
		return fnSegmentBatch[web3types.Log]{}, err
	}

	batch := fnSegmentBatch[web3types.Log]{Logs: logs}
	if len(logs) > 0 {
		last := logs[len(logs)-1]
		batch.TailPosition = &store.ScanCursor{
			BlockNumber: last.BlockNumber, LogIndex: uint64(last.Index),
		}
	}

	return batch, validateFnBatch(batch, remaining)
}

// readWindow replaces all range selectors with the planned numeric block
// window. Clearing BlockHash prevents an accidentally mixed FilterQuery from
// escaping the segment boundary.
func (r *ethFNReader) readWindow(ctx context.Context, from, to uint64) ([]web3types.Log, error) {
	if err := checkTimeout(ctx); err != nil {
		return nil, err
	}

	filter := r.spec.filter
	filter.BlockHash = nil
	fromBlock, toBlock := web3types.BlockNumber(from), web3types.BlockNumber(to)
	filter.FromBlock, filter.ToBlock = &fromBlock, &toBlock
	return r.client.Logs(filter)
}

// applyCursor compares logIndex only within cursor.bn. Log indices restart in
// every block, so applying cursor.li globally would drop valid later blocks
// such as (5010,0) after cursor (5009,2). The physical query boundary already
// excludes blocks on the wrong side of cursor.bn.
//
// eSpace does not need a separate BlockByNumber(cursor.bn) hash lookup here.
// The fixed higher checkpoint H is read before and after the entire attempt;
// under the same ancestry assumption used by the fence, a stable hash at H
// protects every canonical block <= H. A low cursor-hash read would therefore
// duplicate work without closing the accepted A->B->A window. Core resolves a
// cursor hash only because its native Log omits blockNumber and therefore needs
// that identity to recognize the cursor block in a block-range response.
func (r *ethFNReader) applyCursor(logs []web3types.Log) []web3types.Log {
	if r.spec.cursor == nil {
		return logs
	}

	kept := make([]web3types.Log, 0, len(logs))
	for i := range logs {
		log := logs[i]
		if log.BlockNumber != r.spec.cursor.BlockNumber {
			kept = append(kept, log)
			continue
		}

		if (!r.spec.reverse && uint64(log.Index) > r.spec.cursor.LogIndex) ||
			(r.spec.reverse && uint64(log.Index) < r.spec.cursor.LogIndex) {
			kept = append(kept, log)
		}
	}
	return kept
}

var _ fnSegmentReader[web3types.Log] = (*ethFNReader)(nil)
