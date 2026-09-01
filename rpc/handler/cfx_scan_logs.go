package handler

import (
	"context"
	"math/big"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/types"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
)

type CfxEpochRange struct {
	From *cfxtypes.Epoch `json:"fromEpoch,omitempty"`
	To   *cfxtypes.Epoch `json:"toEpoch,omitempty"`
}

func (r *CfxEpochRange) UnmarshalJSON(data []byte) error {
	type plain CfxEpochRange
	var decoded plain

	if err := unmarshalStrictJSONObject(data, &decoded); err != nil {
		return errors.WithMessage(err, "invalid epoch range")
	}

	if err := validateJSONObjectFields(data, nil, []string{"fromEpoch", "toEpoch"}); err != nil {
		return err
	}

	*r = CfxEpochRange(decoded)
	return nil
}

type CfxScanLogFilter struct {
	EpochRange *CfxEpochRange      `json:"epochRange,omitempty"`
	Address    *cfxaddress.Address `json:"address,omitempty"`
	Topic0     *cfxtypes.Hash      `json:"topic0,omitempty"`
}

func (f *CfxScanLogFilter) UnmarshalJSON(data []byte) error {
	type plain CfxScanLogFilter
	var decoded plain

	if err := unmarshalStrictJSONObject(data, &decoded); err != nil {
		return errors.WithMessage(err, "invalid scan filter")
	}
	if err := validateJSONObjectFields(data, nil, []string{"epochRange", "address", "topic0"}); err != nil {
		return err
	}
	*f = CfxScanLogFilter(decoded)
	return nil
}

// CfxScanLogRequest is the JSON-RPC request shape. EpochRange may still contain
// tags and must be normalized before entering the Handler.
type CfxScanLogRequest struct {
	Filter  CfxScanLogFilter `json:"filter"`
	Limit   hexutil.Uint64   `json:"limit"`
	Cursor  *ScanLogCursor   `json:"cursor,omitempty"`
	Reverse bool             `json:"reverse,omitempty"`
}

func (r CfxScanLogRequest) ContractAddress() (string, bool) {
	if r.Filter.Address == nil {
		return "", false
	}
	return r.Filter.Address.MustGetBase32Address(), true
}

func (r *CfxScanLogRequest) UnmarshalJSON(data []byte) error {
	type plain CfxScanLogRequest
	var decoded plain
	if err := unmarshalStrictJSONObject(data, &decoded); err != nil {
		return errors.WithMessage(err, "invalid scan request")
	}
	if err := validateJSONObjectFields(
		data,
		[]string{"filter"},
		[]string{"filter", "limit", "cursor", "reverse"},
	); err != nil {
		return err
	}
	*r = CfxScanLogRequest(decoded)
	return nil
}

// CfxScanLogParams is the Handler input produced by the RPC normalization
// layer. EpochRange is numeric, frozen and validated for the entire request.
type CfxScanLogParams struct {
	*CfxScanLogRequest
	EpochRange          types.RangeUint64
	WithPivotAssumption bool
}

// `CfxPivotAssumption` identifies the canonical pivot block for an epoch.
// The same shape is used for the output guard.
type CfxPivotAssumption struct {
	EpochNumber    hexutil.Uint64 `json:"epochNumber"`
	PivotBlockHash cfxtypes.Hash  `json:"pivotBlockHash"`
}

func (a *CfxPivotAssumption) UnmarshalJSON(data []byte) error {
	type plain CfxPivotAssumption
	var decoded plain
	if err := unmarshalStrictJSONObject(data, &decoded); err != nil {
		return errors.WithMessage(err, "invalid pivot assumption")
	}
	if err := validateJSONObjectFields(
		data,
		[]string{"epochNumber", "pivotBlockHash"},
		[]string{"epochNumber", "pivotBlockHash"},
	); err != nil {
		return err
	}
	*a = CfxPivotAssumption(decoded)
	return nil
}

type CfxPivotGuard CfxPivotAssumption

type CfxScanLogResult struct {
	Logs       []cfxtypes.Log `json:"logs"`
	NextCursor *ScanLogCursor `json:"nextCursor,omitempty"`
	PivotGuard *CfxPivotGuard `json:"pivotGuard,omitempty"`
	usage      cfxScanUsage
}

// `cfxScanClient` allows Reader behavior to be tested without
// mocking the much larger SDK ClientOperator interface.
type cfxScanClient interface {
	GetBlockSummaryByHash(cfxtypes.Hash) (*cfxtypes.BlockSummary, error)
	GetBlockSummaryByEpoch(*cfxtypes.Epoch) (*cfxtypes.BlockSummary, error)
	GetBlockSummaryByBlockNumber(hexutil.Uint64) (*cfxtypes.BlockSummary, error)
	GetLogs(cfxtypes.LogFilter) ([]cfxtypes.Log, error)
}

type cfxEpochNumberResolver interface {
	GetEpochNumber(...*cfxtypes.Epoch) (*hexutil.Big, error)
}

func NormalizeCfxScanLogRequest(
	resolver cfxEpochNumberResolver,
	req CfxScanLogRequest,
	withPivotAssumption bool,
) (CfxScanLogParams, error) {
	// Normalize log limit
	effectiveLimit, err := normalizeScanLogsLimit(req.Limit)
	if err != nil {
		return CfxScanLogParams{}, errors.WithMessage(err, "failed to normalize scan log limit")
	}
	req.Limit = effectiveLimit

	// Normalize epoch range
	from, to := cfxtypes.EpochLatestState, cfxtypes.EpochLatestState
	if req.Filter.EpochRange != nil {
		if req.Filter.EpochRange.From != nil {
			from = req.Filter.EpochRange.From
		}
		if req.Filter.EpochRange.To != nil {
			to = req.Filter.EpochRange.To
		}
	}

	resolvedTags := make(map[string]uint64)
	resolveEpoch := func(epoch *cfxtypes.Epoch) (uint64, error) {
		if epochNum, ok := epoch.ToInt(); ok {
			return epochNum.Uint64(), nil
		}

		key := epoch.String()
		if epochNum, ok := resolvedTags[key]; ok {
			return epochNum, nil
		}

		epochNum, err := resolver.GetEpochNumber(epoch)
		if err != nil {
			return 0, errors.WithMessagef(err, "failed to resolve epoch tag %s", epoch)
		}
		if epochNum == nil {
			return 0, errors.Errorf("failed to resolve epoch tag %s: empty epoch number", epoch)
		}

		resolved := epochNum.ToInt().Uint64()
		resolvedTags[key] = resolved
		return resolved, nil
	}

	fromNumber, err := resolveEpoch(from)
	if err != nil {
		return CfxScanLogParams{}, err
	}
	toNumber, err := resolveEpoch(to)
	if err != nil {
		return CfxScanLogParams{}, err
	}
	if fromNumber > toNumber {
		return CfxScanLogParams{}, errors.WithMessagef(
			ErrScanLogsInvalidParams,
			"invalid epoch range: from epoch %s exceeds to epoch %s", from, to,
		)
	}

	// Numeric upper bounds are caller assertions rather than dynamic tags. Freeze
	// latest_state once at the request boundary and reject a future assertion;
	// silently truncating it would make the client mistake a short page for EOF.
	if _, explicitUpper := to.ToInt(); explicitUpper {
		latestState, err := resolveEpoch(cfxtypes.EpochLatestState)
		if err != nil {
			return CfxScanLogParams{}, err
		}

		if toNumber > latestState {
			return CfxScanLogParams{}, errors.WithMessagef(
				ErrScanLogsInvalidParams,
				"explicit toEpoch %d exceeds frozen latest_state %d", toNumber, latestState,
			)
		}
	}

	return CfxScanLogParams{
		CfxScanLogRequest: &req,
		EpochRange: types.RangeUint64{
			From: fromNumber, To: toNumber,
		},
		WithPivotAssumption: withPivotAssumption,
	}, nil
}

type cfxScanGeneration struct {
	// `dbAvailable` distinguishes a real DB coverage interval from the zero values
	// used by a pure-FN fallback when mapping tables are not initialized yet.
	dbAvailable bool

	// The outer generation stays epoch-native because DB coverage and the public
	// Core API are expressed in epochs. dbEpochs is mapped from stable Store data;
	// fnEpochs is materialized into attempt-local blocks only inside the FN fence.
	dbEpochs scanRange
	dbBlocks scanRange
	fnEpochs scanRange

	// requestEpochs is the original RPC-normalized request epoch range.
	requestEpochs scanRange

	// DB coverage is captured within the outer v0/v1 generation.
	dbMinEpoch uint64
	dbMaxEpoch uint64
	dbMaxBlock uint64

	// `dbPivot` is the DB identity at the split watermark used for mixed-view
	// boundary alignment in every inner attempt.
	dbPivot cfxtypes.Hash

	// `plan` captures the DB/FN split and the one-time cursor placement.
	plan scanPlan

	// `owner` is the source that owns the cursor.
	owner cursorOwner
}

// cfxScanUsage records canonical views actually consumed by one candidate.
// Empty scans and auxiliary pivot reads count; merely planned segments do not.
type cfxScanUsage struct {
	db bool
	fn bool
}

func (r *CfxScanLogResult) canonicalUsageDB() bool { return r != nil && r.usage.db }
func (r *CfxScanLogResult) canonicalUsageFN() bool { return r != nil && r.usage.fn }

// cfxFNFilter adapts the public scan filter to the broader SDK request only at
// the FN boundary. Range/window fields are supplied by the Reader.
func cfxFNFilter(scanFilter CfxScanLogFilter) cfxtypes.LogFilter {
	filter := cfxtypes.LogFilter{}

	if scanFilter.Address != nil {
		filter.Address = []cfxtypes.Address{*scanFilter.Address}
	}
	if scanFilter.Topic0 != nil {
		filter.Topics = [][]cfxtypes.Hash{{*scanFilter.Topic0}}
	}
	return filter
}

// buildCfxGeneration captures one DB-version-specific epoch split and produces
// a pure ordered source Plan. It deliberately does not persist FN block bounds:
// those bounds are canonical node data and must be rebuilt in every inner
// before/after attempt.
func (handler *CfxLogsApiHandler) buildCfxGeneration(req CfxScanLogParams) (cfxScanGeneration, error) {
	cursor := req.Cursor.toStoreCursor()

	earliest, ok, err := handler.ms.EarliestBlockMapping()
	if err != nil {
		return cfxScanGeneration{}, errors.WithMessage(err, "failed to load earliest block mappings")
	}
	if !ok {
		// No earliest mapping means Store cannot prove coverage for any part of
		// this request. Treat the whole normalized range as FN-owned instead of
		// failing or guessing a DB split.
		gen := newCfxFullnodeGeneration(scanRange(req.EpochRange), cursor, req.Reverse)
		if cursor != nil && gen.fnEpochs.empty() {
			return cfxScanGeneration{}, errors.WithMessage(
				ErrScanLogsInvalidCursor, "cursor is outside of the request range",
			)
		}
		return gen, nil
	}

	if req.EpochRange.From < earliest.Epoch {
		// Dropping the low end would silently omit pruned history and could make a
		// short page look exhausted to the client.
		return cfxScanGeneration{}, store.ErrAlreadyPruned
	}

	latest, ok, err := handler.ms.LatestBlockMapping()
	if err != nil {
		return cfxScanGeneration{}, errors.WithMessage(err, "failed to load latest block mappings")
	}

	if !ok {
		// Earliest and latest query the same mapping table. Once earliest exists,
		// latest must also exist in a stable Store view. Missing only this endpoint
		// means the two non-transactional reads crossed a Store change, or the table
		// is inconsistent; let the outer v0/v1 gate retry or publish the fault.
		return cfxScanGeneration{}, errors.WithMessage(
			ErrScanLogsConsistency, "latest mapping is unavailable while earliest mapping exists",
		)
	}

	gen := cfxScanGeneration{
		dbAvailable: true,
		dbEpochs: scanRange{
			From: max(req.EpochRange.From, earliest.Epoch),
			To:   min(req.EpochRange.To, latest.Epoch),
		},
		dbMinEpoch:    earliest.Epoch,
		dbMaxEpoch:    latest.Epoch,
		dbMaxBlock:    latest.BnMax,
		dbPivot:       cfxtypes.Hash(latest.PivotHash),
		requestEpochs: scanRange(req.EpochRange),
	}

	if gen.dbEpochs.empty() {
		gen.dbBlocks = emptyScanRange
	} else {
		// Both DB segment endpoints must have exact mappings. A missing endpoint
		// contradicts the continuous DB coverage claimed by the captured watermarks.
		fromMapping, fromOK, err := handler.ms.BlockMapping(gen.dbEpochs.From)
		if err != nil {
			return cfxScanGeneration{}, errors.WithMessage(err, "failed to load block mapping")
		}

		toMapping, toOK, err := handler.ms.BlockMapping(gen.dbEpochs.To)
		if err != nil {
			return cfxScanGeneration{}, errors.WithMessage(err, "failed to load block mapping")
		}

		if !fromOK || !toOK ||
			fromMapping.BnMin > fromMapping.BnMax ||
			toMapping.BnMin > toMapping.BnMax ||
			fromMapping.BnMin > toMapping.BnMax {
			return cfxScanGeneration{}, errors.WithMessage(
				ErrScanLogsConsistency, "inconsistent block mappings",
			)
		}

		gen.dbBlocks = scanRange{From: fromMapping.BnMin, To: toMapping.BnMax}
	}

	// The RPC layer has already resolved tags to numeric epochs and
	// frozen and validated the effective numeric range. The Handler only intersects that
	// range with the DB watermark.
	if latest.Epoch >= req.EpochRange.To {
		gen.fnEpochs = emptyScanRange
	} else {
		gen.fnEpochs = scanRange{
			From: max(req.EpochRange.From, latest.Epoch+1),
			To:   req.EpochRange.To,
		}
	}

	owner, err := classifyCursorOwner(cursor, gen.dbBlocks, latest.BnMax)
	if err != nil {
		return cfxScanGeneration{}, errors.WithMessage(err, "invalid cursor placement")
	}
	if owner == cursorOwnerFN && gen.fnEpochs.empty() {
		return cfxScanGeneration{}, errors.WithMessage(
			ErrScanLogsInvalidCursor, "cursor is outside of the segment range",
		)
	}
	gen.owner = owner

	gen.plan = buildScanPlan(!gen.dbBlocks.empty(), !gen.fnEpochs.empty(), req.Reverse, owner, cursor)
	return gen, nil
}

func newCfxFullnodeGeneration(
	requestEpochRange scanRange, cursor *store.ScanCursor, reverse bool,
) cfxScanGeneration {
	owner := cursorOwnerNone
	if cursor != nil {
		owner = cursorOwnerFN
	}

	plan := buildScanPlan(false, !requestEpochRange.empty(), reverse, owner, cursor)

	return cfxScanGeneration{
		dbEpochs:      emptyScanRange,
		dbBlocks:      emptyScanRange,
		fnEpochs:      requestEpochRange,
		requestEpochs: requestEpochRange,
		owner:         owner,
		plan:          plan,
	}
}

// newCfxDBCache adapts Store's BN-native keyset scan to native Core logs. Keys
// remain a parallel sidecar because Core Log does not expose blockNumber; only
// DB pagination and final TailPosition need them.
func (handler *CfxLogsApiHandler) newCfxDBCache(req CfxScanLogParams, gen cfxScanGeneration) *dbScanCache[cfxtypes.Log] {
	return &dbScanCache[cfxtypes.Log]{
		initial: gen.plan.cursorFor(scanSourceDB),
		scan: func(ctx context.Context, cursor *store.ScanCursor, limit int) ([]cfxtypes.Log, []store.ScanCursor, error) {
			filter := store.ScanLogFilter{
				BlockFrom: gen.dbBlocks.From, BlockTo: gen.dbBlocks.To,
			}
			if req.Filter.Address != nil {
				filter.Contract = req.Filter.Address.String()
			}
			if req.Filter.Topic0 != nil {
				filter.Topic0 = req.Filter.Topic0.String()
			}

			params := store.ScanLogParams{Filter: filter, Cursor: cursor, Reverse: req.Reverse, Limit: limit}
			raw, err := handler.ms.ScanLogs(ctx, params)
			if err != nil {
				return nil, nil, err
			}

			logs := make([]cfxtypes.Log, 0, len(raw))
			keys := make([]store.ScanCursor, 0, len(raw))

			for _, item := range raw {
				log, err := item.ToCfxLog()
				if err != nil {
					return nil, nil, errors.WithMessage(err, "failed to convert DB log to sdk log")
				}

				cursor := store.ScanCursor{BlockNumber: item.BlockNumber, LogIndex: item.LogIndex}
				logs = append(logs, *log)
				keys = append(keys, cursor)
			}
			return logs, keys, nil
		},
	}
}

// runCfxPlan is intentionally a sequential append-only runner. It records
// canonical usage when a source is consulted, even if that source returns no
// logs. Merely having a segment in the Plan is not usage: a preceding segment
// may fill limit and prevent the later source from being touched.
func (handler *CfxLogsApiHandler) runCfxPlan(
	ctx context.Context, cfx cfxScanClient, req CfxScanLogParams, gen cfxScanGeneration,
	dbCache *dbScanCache[cfxtypes.Log], attempt *cfxFNAttemptView,
) (*CfxScanLogResult, error) {
	result := &CfxScanLogResult{}
	usage := cfxScanUsage{}

	for _, segment := range gen.plan.segments {
		remaining := int(req.Limit) - len(result.Logs)
		if remaining == 0 {
			break
		}

		switch segment.source {
		case scanSourceDB:
			// Ensure's argument is the desired DB prefix length for this candidate.
			// In reverse scans this can vary between FN retries.
			usage.db = true
			if err := dbCache.Ensure(ctx, remaining); err != nil {
				return result, errors.WithMessagef(err, "failed to ensure DB cache")
			}

			logs := dbCache.Prefix(remaining)
			result.Logs = append(result.Logs, logs...)
			if len(logs) > 0 {
				result.NextCursor = newScanCursor(dbCache.Tail(len(logs)))
			}
		case scanSourceFN:
			usage.fn = true
			if attempt == nil {
				return result, errors.New("attempt view is not open")
			}

			blockPlan, err := attempt.resolveBlockPlan(gen, segment, req.Reverse)
			if err != nil {
				result.usage = usage
				return result, errors.WithMessage(err, "failed to resolve block segment")
			}

			reader := &cfxFNReader{
				client:  cfx,
				attempt: attempt,
				spec: cfxFNReaderSpec{
					blocks: blockPlan.blocks, filter: cfxFNFilter(req.Filter), cursor: blockPlan.cursor,
					cursorHash: blockPlan.cursorHash, reverse: req.Reverse, windowSize: defaultScanLogsFNWindow,
				},
			}
			batch, err := reader.Scan(ctx, remaining)
			if err != nil {
				result.usage = usage
				return result, errors.WithMessage(err, "failed to scan full node logs")
			}

			result.Logs = append(result.Logs, batch.Logs...)
			if batch.TailPosition != nil {
				result.NextCursor = newScanCursor(batch.TailPosition)
			}
		}
	}
	result.usage = usage
	return result, nil
}

// finishCfxCandidate performs page-level work that may introduce additional
// canonical dependencies. It must run while the FN fence is open and before
// DB v1. NextCursor itself is already materialized; PivotGuard may need a DB
// pivot mapping or an FN pivot summary.
func (handler *CfxLogsApiHandler) finishCfxCandidate(
	req CfxScanLogParams, assumption *CfxPivotAssumption, gen cfxScanGeneration,
	logs []cfxtypes.Log, tail *ScanLogCursor, dbUsed, fnUsed bool, attempt *cfxFNAttemptView,
) (*CfxScanLogResult, error) {
	result := &CfxScanLogResult{Logs: logs, NextCursor: tail.clone()}
	usage := cfxScanUsage{db: dbUsed, fn: fnUsed}

	if assumption == nil && (!req.WithPivotAssumption || len(logs) == 0) {
		result.usage = usage
		return result, nil
	}

	if len(logs) == 0 || (req.Reverse && req.Cursor != nil) {
		// Empty pages cannot derive a new guard. Reverse continuations must keep
		// the first page's fixed upper guard, so both cases echo the input
		// assumption established by the owning DB/FN path.
		guard := CfxPivotGuard(*assumption)
		result.PivotGuard = &guard
		result.usage = usage
		return result, nil
	}

	guardLog := logs[len(logs)-1]
	if req.Reverse {
		// On the reverse first page, logs[0] is the highest returned log. Fixing
		// the guard there prevents later pages from drifting their upper view.
		guardLog = logs[0]
	}
	if guardLog.EpochNumber == nil {
		return result, errors.New("incomplete guard log: missing epoch number")
	}

	epoch := guardLog.EpochNumber.ToInt().Uint64()
	guard := &CfxPivotGuard{EpochNumber: hexutil.Uint64(epoch)}

	if gen.dbAvailable && epoch <= gen.dbMaxEpoch {
		usage.db = true

		pivot, ok, err := handler.ms.PivotHash(epoch)
		if err != nil {
			return result, errors.WithMessagef(err, "failed to get db pivot hash for epoch %d", epoch)
		}
		if !ok {
			return result, newCanonicalDependentError(
				ErrScanLogsConsistency, "guard epoch %d is missing from block mapping", epoch,
			)
		}
		guard.PivotBlockHash = cfxtypes.Hash(pivot)
	} else {
		if !fnUsed {
			return result, newCanonicalDependentError(
				ErrScanLogsConsistency,
				"candidate produced guard epoch %d above DB watermark %d", epoch, gen.dbMaxEpoch,
			)
		}

		usage.fn = true

		if attempt == nil {
			return result, newCanonicalDependentError(
				ErrScanLogsConsistency, "missing open attempt view",
			)
		}
		pivot, err := attempt.pivot(epoch)
		if err != nil {
			return result, errors.WithMessagef(
				err, "failed to get pivot block for epoch %d", epoch,
			)
		}
		guard.PivotBlockHash = pivot.hash
	}

	result.PivotGuard = guard
	result.usage = usage
	return result, nil
}

func equalCfxHash(a, b cfxtypes.Hash) bool {
	return common.HexToHash(a.String()) == common.HexToHash(b.String())
}

// ScanLogs executes a scan whose epoch range was already normalized and
// frozen by the RPC layer.
//
// Consistency uses two nested optimistic scopes:
//
//	outer DB generation:
//	  v0 -> capture watermarks/mappings -> DB assumption -> DB cache
//	  inner FN attempt (only if reached):
//	    before(H) -> logs/cursor/guard/boundary -> after(H)
//	  v1 -> common success/error commit gate
//
// A changed FN checkpoint retries only the inner scope and reuses DB results.
// A changed DB reorgVersion invalidates mappings, plan, cache and all candidate
// outcomes. Canonical-dependent errors pass through the same gates as success.
func (handler *CfxLogsApiHandler) ScanLogs(
	ctx context.Context,
	cfx cfxScanClient,
	req CfxScanLogParams,
	assumption *CfxPivotAssumption,
) (result *CfxScanLogResult, err error) {
	if req.WithPivotAssumption && req.Cursor != nil && assumption == nil {
		return nil, errors.WithMessage(
			ErrScanLogsInvalidParams, "pivot assumption is required when cursor is provided",
		)
	}

	recorder := newScanLogsMetrics("cfx", req.WithPivotAssumption)
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

	return handler.scanLogs(ctx, cfx, req, assumption)
}

func (handler *CfxLogsApiHandler) scanLogs(
	ctx context.Context,
	cfx cfxScanClient,
	req CfxScanLogParams,
	assumption *CfxPivotAssumption,
) (*CfxScanLogResult, error) {
	if req.Limit == 0 || uint64(req.Limit) > maxScanLogsLimit {
		return nil, errors.WithMessagef(ErrScanLogsInvalidParams, "scan limit must be in [1, %d]", maxScanLogsLimit)
	}

	ctx, cancel := context.WithTimeout(ctx, store.TimeoutGetLogs)
	defer cancel()

	for {
		if err := checkTimeout(ctx); err != nil {
			return nil, err
		}

		// v0 must precede every DB-derived canonical fact: coverage, mappings,
		// assumption, guard and both positive and negative scan results.
		v0, err := handler.ms.GetReorgVersion()
		if err != nil {
			return nil, errors.WithMessage(err, "failed to load reorg version")
		}

		gen, err := handler.buildCfxGeneration(req)
		if err != nil {
			if errors.Is(err, ErrScanLogsConsistency) {
				// Mapping invariants were evaluated from the v0 generation. If Store
				// changed while they were being read, rebuild before publishing the
				// consistency error. Invalid cursors are deterministic request errors
				// and return directly without entering a commit fence.
				_, retryOuter, commitErr := handler.commitCfxDBGeneration(v0, nil, err)
				if retryOuter {
					markScanLogsMetric(ctx, "retry/db_outer")
					continue
				}
				return nil, commitErr
			}
			return nil, errors.WithMessage(err, "failed to build scan generation")
		}

		recordScanLogsHistogram(ctx, "plan/segments", int64(len(gen.plan.segments)))
		recordScanLogsCursorOwner(ctx, gen.owner)

		// Cache lifetime is exactly this outer generation. FN-only plans do not
		// allocate one; a cache survives inner retries but never an outer restart.
		var dbCache *dbScanCache[cfxtypes.Log]
		if gen.plan.contains(scanSourceDB) {
			dbCache = handler.newCfxDBCache(req, gen)
		}

		// Check DB assumptions if the provided pivot is within the captured DB coverage.
		dbAssumption, assumptionErr, err := handler.checkCfxDBAssumption(gen, assumption)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to check DB assumption")
		}
		if assumptionErr != nil {
			_, retryOuter, err := handler.commitCfxDBGeneration(v0, nil, assumptionErr)
			if retryOuter {
				markScanLogsMetric(ctx, "retry/db_outer")
				continue
			}
			return nil, err
		}
		fnAssumption := assumption != nil && !dbAssumption

		// In the common forward DB-first case, materialize the immutable DB
		// prefix once before opening a fullnode view. If it fills the page and
		// no FN assumption is needed, the request remains a pure DB read-set.
		if gen.plan.startsWith(scanSourceDB) {
			if err := dbCache.Ensure(ctx, int(req.Limit)); err != nil {
				return nil, errors.WithMessage(err, "failed to ensure DB cache")
			}

			if len(dbCache.logs) == int(req.Limit) && !fnAssumption {
				scanCursor := newScanCursor(dbCache.Tail(len(dbCache.logs)))
				result, provisionalErr := handler.finishCfxCandidate(
					req, assumption, gen, dbCache.logs, scanCursor, true, false, nil,
				)

				result, retryOuter, err := handler.commitCfxDBGeneration(v0, result, provisionalErr)
				if retryOuter {
					markScanLogsMetric(ctx, "retry/db_outer")
					continue
				}
				return result, err
			}
		}

		// Quick path: if the plan is pure DB only and no FN assumption is needed, run the plan and commit the result.
		if !gen.plan.contains(scanSourceFN) && !fnAssumption {
			result, provisionalErr := handler.runCfxPlan(ctx, cfx, req, gen, dbCache, nil)
			if provisionalErr == nil {
				result, provisionalErr = handler.finishCfxCandidate(
					req, assumption, gen, result.Logs, result.NextCursor, true, false, nil,
				)
			}
			result, retryOuter, err := handler.commitCfxDBGeneration(v0, result, provisionalErr)
			if retryOuter {
				markScanLogsMetric(ctx, "retry/db_outer")
				continue
			}
			return result, err
		}

		// Otherwise, run the fullnode plan.
		outer := cfxOuterState{
			version:      v0,
			gen:          gen,
			dbCache:      dbCache,
			dbAssumption: dbAssumption,
			fnAssumption: fnAssumption,
		}
		result, retryOuter, err := handler.scanCfxFullnodeGeneration(
			ctx, cfx, req, assumption, outer,
		)
		if retryOuter {
			markScanLogsMetric(ctx, "retry/db_outer")
			continue
		}
		return result, err
	}
}

// checkCfxDBAssumption resolves an assumption that belongs to the captured DB
// coverage. A mismatch is returned as a provisional generation result; callers
// must close the v0/v1 fence before publishing it.
func (handler *CfxLogsApiHandler) checkCfxDBAssumption(
	gen cfxScanGeneration, assumption *CfxPivotAssumption,
) (belongsToDB bool, provisionalErr error, err error) {
	if assumption == nil || !gen.dbAvailable ||
		uint64(assumption.EpochNumber) < gen.dbMinEpoch || uint64(assumption.EpochNumber) > gen.dbMaxEpoch {
		return false, nil, nil
	}

	pivot, ok, err := handler.ms.PivotHash(uint64(assumption.EpochNumber))
	if err != nil {
		return true, nil, errors.WithMessage(err, "failed to load pivot hash")
	}
	if !ok {
		return true, errors.WithMessage(
			ErrScanLogsConsistency, "pivot mapping is unavailable within captured coverage",
		), nil
	}

	if !equalCfxHash(cfxtypes.Hash(pivot), assumption.PivotBlockHash) {
		return true, errors.WithMessagef(
			ErrScanLogsAssumptionNotMet,
			"expected pivot %s got %s for epoch %d",
			assumption.PivotBlockHash, pivot, assumption.EpochNumber,
		), nil
	}
	return true, nil, nil
}

// commitCfxDBGeneration is the single commit gate for results and provisional
// errors that depend only on the captured DB generation.
func (handler *CfxLogsApiHandler) commitCfxDBGeneration(
	v0 int, result *CfxScanLogResult, provisionalErr error,
) (res *CfxScanLogResult, shouldRetry bool, err error) {
	v1, err := handler.ms.GetReorgVersion()
	if err != nil {
		return nil, false, errors.WithMessage(err, "failed to load reorg version")
	}

	if v1 != v0 {
		return nil, true, nil
	}

	if provisionalErr != nil {
		return nil, false, provisionalErr
	}
	return result, false, nil
}

// cfxInnerCandidate holds either a result or a canonical-dependent provisional
// error until the FN checkpoint and DB generation fences have both closed.
type cfxInnerCandidate struct {
	result *CfxScanLogResult
	err    error
	usage  cfxScanUsage
}

// cfxOuterState groups everything whose lifetime is one DB generation. Keeping
// it explicit makes the boundary between outer retries and FN-only retries
// visible in the function signatures.
type cfxOuterState struct {
	version      int
	gen          cfxScanGeneration
	dbCache      *dbScanCache[cfxtypes.Log]
	dbAssumption bool
	fnAssumption bool
}

// scanCfxFullnodeGeneration owns the inner retry loop. Each pass opens one FN
// fence at a fixed H, rebuilds the FN portion of the candidate, aligns a mixed
// DB/FN boundary, and then decides whether to commit or retry.
func (handler *CfxLogsApiHandler) scanCfxFullnodeGeneration(
	ctx context.Context,
	cfx cfxScanClient,
	req CfxScanLogParams,
	assumption *CfxPivotAssumption,
	outer cfxOuterState,
) (*CfxScanLogResult, bool, error) {
	h := outer.gen.fnEpochs.To
	if outer.gen.fnEpochs.empty() {
		h = outer.gen.requestEpochs.To
	}

	if outer.fnAssumption && uint64(assumption.EpochNumber) > h {
		h = uint64(assumption.EpochNumber)
	}

	boundaryRetries := 0
	for {
		if err := checkTimeout(ctx); err != nil {
			return nil, false, err
		}

		// The before summary both identifies the FN view and bounds all cursor BN
		// lookups that may occur while building this candidate.
		before, err := cfx.GetBlockSummaryByEpoch(cfxtypes.NewEpochNumberUint64(h))
		markScanLogsMetric(ctx, "checkpoint_before")
		if err != nil {
			return nil, false, errors.WithMessage(err, "failed to get pivot block summary")
		}

		attempt, err := newCfxFNAttemptView(
			cfx, h, before, scanLogsMetricsFromContext(ctx),
		)
		if err != nil {
			// A checkpoint without the fields needed by the algorithm cannot define
			// a fence, so fail this node call directly.
			return nil, false, err
		}

		candidate, err := handler.buildCfxInnerCandidate(ctx, cfx, req, assumption, outer, attempt)
		if err != nil {
			return nil, false, err
		}

		boundaryMismatch := false
		if candidate.usage.db && candidate.usage.fn {
			boundary, err := attempt.pivot(outer.gen.dbMaxEpoch)
			if err != nil {
				if !isCanonicalDependentError(err) {
					return nil, false, errors.WithMessagef(
						err, "failed to get boundary summary for epoch %d", outer.gen.dbMaxEpoch,
					)
				}
				candidate.err = errors.WithMessagef(
					err, "failed to get boundary summary for epoch %d", outer.gen.dbMaxEpoch,
				)
			} else {
				boundaryMismatch = !equalCfxHash(boundary.hash, outer.gen.dbPivot)
			}
		}

		// This fence is optimistic, not a transaction or immutable FN snapshot.
		// before=A/after=B detects a lasting canonical switch and discards every
		// dependent range/cursor/guard read. It cannot detect before=A, an
		// intermediate query served from B, then after=A: that A->B->A (ABA)
		// attempt will be accepted. The mixed DB/FN boundary check has the same
		// limitation. confirmed/safe lowers the probability and finalized is the
		// safest caller-selected range, but the server must not silently cap a
		// latest request because a short page would falsely signal exhaustion.
		// Eliminating ABA requires a node-provided immutable view token or atomic
		// range RPC; JSON-RPC batch does not provide that guarantee.
		after, err := cfx.GetBlockSummaryByEpoch(cfxtypes.NewEpochNumberUint64(h))
		markScanLogsMetric(ctx, "checkpoint_after")
		if err != nil {
			return nil, false, errors.WithMessagef(err, "failed to get block summary for epoch %d", h)
		}
		afterRef, err := newCfxBlockRef(after)
		if err != nil {
			return nil, false, errors.WithMessage(err, "invalid post-checkpoint block summary")
		}

		dbStable := true
		if outer.gen.dbAvailable {
			v1, err := handler.ms.GetReorgVersion()
			if err != nil {
				return nil, false, errors.WithMessage(err, "failed to get reorg version")
			}
			dbStable = (v1 == outer.version)
		}

		checkpointStable := equalCfxHash(afterRef.hash, attempt.checkpoint.hash)
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

// buildCfxInnerCandidate performs all canonical reads inside one open FN fence.
// Canonical-dependent errors remain provisional so the caller can run after(H)
// and v1 before returning them.
func (handler *CfxLogsApiHandler) buildCfxInnerCandidate(
	ctx context.Context,
	cfx cfxScanClient,
	req CfxScanLogParams,
	assumption *CfxPivotAssumption,
	outer cfxOuterState,
	attempt *cfxFNAttemptView,
) (cfxInnerCandidate, error) {
	candidate := cfxInnerCandidate{usage: cfxScanUsage{db: outer.dbAssumption}}

	result, provisionalErr := handler.runCfxPlan(ctx, cfx, req, outer.gen, outer.dbCache, attempt)
	if result != nil {
		candidate.usage.db = candidate.usage.db || result.canonicalUsageDB()
		candidate.usage.fn = result.canonicalUsageFN()
	}

	if provisionalErr != nil {
		if !isCanonicalDependentError(provisionalErr) {
			return cfxInnerCandidate{}, provisionalErr
		}
		candidate.usage.fn = true
	}

	if outer.fnAssumption {
		candidate.usage.fn = true

		assumptionEpoch := uint64(assumption.EpochNumber)
		pivot, err := attempt.pivot(assumptionEpoch)
		if err != nil {
			if !isCanonicalDependentError(err) {
				return cfxInnerCandidate{}, errors.WithMessagef(err, "failed to get block summary for epoch %d", assumptionEpoch)
			}
			if provisionalErr == nil {
				provisionalErr = err
			}
		}
		if err == nil && provisionalErr == nil && !equalCfxHash(pivot.hash, assumption.PivotBlockHash) {
			provisionalErr = newCanonicalDependentError(
				ErrScanLogsAssumptionNotMet,
				"expected pivot %s got %s for epoch %d",
				assumption.PivotBlockHash, pivot.hash, assumption.EpochNumber,
			)
		}
	}

	if provisionalErr == nil {
		result, provisionalErr = handler.finishCfxCandidate(
			req, assumption, outer.gen, result.Logs, result.NextCursor,
			candidate.usage.db, candidate.usage.fn, attempt,
		)
		if provisionalErr != nil && !isCanonicalDependentError(provisionalErr) {
			return cfxInnerCandidate{}, provisionalErr
		}
		if result != nil {
			candidate.usage.db = candidate.usage.db || result.canonicalUsageDB()
			candidate.usage.fn = candidate.usage.fn || result.canonicalUsageFN()
		}
	}

	candidate.result = result
	candidate.err = provisionalErr
	return candidate, nil
}

// cfxBlockRef is attempt-local canonical metadata. It never escapes the FN
// before/after fence or survives an inner retry.
type cfxBlockRef struct {
	hash        cfxtypes.Hash
	epochNumber uint64
	blockNumber uint64
}

// cfxFNBlockPlan is a materialized physical segment. The outer planner
// remains epoch-native; this value is rebuilt inside every FN attempt after the
// checkpoint-before read.
type cfxFNBlockPlan struct {
	blocks     scanRange
	cursor     *store.ScanCursor
	cursorHash *cfxtypes.Hash
}

// cfxFNAttemptView centralizes all Core canonical lookups made between one
// checkpoint-before/after pair. Successful lookups are cached only for this
// attempt, which both avoids duplicate RPCs and makes the retry boundary
// explicit in the type graph.
type cfxFNAttemptView struct {
	client          cfxScanClient
	checkpointEpoch uint64
	checkpoint      cfxBlockRef
	pivots          map[uint64]cfxBlockRef
	byNumber        map[uint64]cfxBlockRef
	byHash          map[cfxtypes.Hash]cfxBlockRef
	metrics         scanLogsMetricsRecorder
}

func newCfxFNAttemptView(
	client cfxScanClient,
	checkpointEpoch uint64,
	checkpointSummary *cfxtypes.BlockSummary,
	metrics scanLogsMetricsRecorder,
) (*cfxFNAttemptView, error) {
	checkpoint, err := newCfxBlockRef(checkpointSummary)
	if err != nil {
		return nil, err
	}

	view := &cfxFNAttemptView{
		client:          client,
		checkpointEpoch: checkpointEpoch,
		checkpoint:      checkpoint,
		pivots:          make(map[uint64]cfxBlockRef),
		byNumber:        make(map[uint64]cfxBlockRef),
		byHash:          make(map[cfxtypes.Hash]cfxBlockRef),
		metrics:         metrics,
	}
	view.rememberPivot(checkpoint)
	return view, nil
}

func (v *cfxFNAttemptView) remember(ref cfxBlockRef) {
	v.byNumber[ref.blockNumber] = ref
	v.byHash[ref.hash] = ref
}

func (v *cfxFNAttemptView) rememberPivot(ref cfxBlockRef) {
	v.remember(ref)
	v.pivots[ref.epochNumber] = ref
}

func (v *cfxFNAttemptView) pivot(epoch uint64) (cfxBlockRef, error) {
	if ref, ok := v.pivots[epoch]; ok {
		if v.metrics != nil {
			v.metrics.Mark("pivot_cache_reuse")
		}
		return ref, nil
	}

	if epoch > v.checkpointEpoch {
		return cfxBlockRef{}, newCanonicalDependentError(
			ErrScanLogsConsistency,
			"requested epoch %d is above checkpoint epoch %d", epoch, v.checkpointEpoch,
		)
	}

	summary, err := v.client.GetBlockSummaryByEpoch(cfxtypes.NewEpochNumberUint64(epoch))
	if v.metrics != nil {
		v.metrics.Mark("pivot_rpc")
	}
	if err != nil {
		return cfxBlockRef{}, errors.WithMessagef(
			err, "failed to get pivot summary for epoch %d", epoch,
		)
	}

	ref, err := newCfxBlockRef(summary)
	if err != nil {
		return cfxBlockRef{}, err
	}

	if ref.blockNumber > v.checkpoint.blockNumber {
		return cfxBlockRef{}, newCanonicalDependentError(
			ErrScanLogsConsistency,
			"pivot block %d of epoch %d is above checkpoint block %d",
			ref.blockNumber, epoch, v.checkpoint.blockNumber,
		)
	}

	v.rememberPivot(ref)
	return ref, nil
}

func (v *cfxFNAttemptView) block(number uint64) (cfxBlockRef, error) {
	if ref, ok := v.byNumber[number]; ok {
		return ref, nil
	}

	// Never issue a canonical lookup above the height protected by this attempt.
	if number > v.checkpoint.blockNumber {
		return cfxBlockRef{}, newCanonicalDependentError(
			ErrScanLogsInvalidCursor,
			"cursor block %d is above checkpoint block %d",
			number, v.checkpoint.blockNumber,
		)
	}

	summary, err := v.client.GetBlockSummaryByBlockNumber(hexutil.Uint64(number))
	if v.metrics != nil {
		v.metrics.Mark("cursor_summary")
	}
	if err != nil {
		return cfxBlockRef{}, errors.WithMessagef(
			err, "failed to get block summary for block %d", number,
		)
	}

	ref, err := newCfxBlockRef(summary)
	if err != nil {
		return cfxBlockRef{}, err
	}

	if ref.epochNumber > v.checkpointEpoch {
		return cfxBlockRef{}, newCanonicalDependentError(
			ErrScanLogsConsistency,
			"block %d belongs to epoch %d, which is above checkpoint epoch %d",
			number, ref.epochNumber, v.checkpointEpoch,
		)
	}

	v.remember(ref)
	return ref, nil
}

func (v *cfxFNAttemptView) blockByHash(hash cfxtypes.Hash) (cfxBlockRef, error) {
	if ref, ok := v.byHash[hash]; ok {
		return ref, nil
	}

	summary, err := v.client.GetBlockSummaryByHash(hash)
	if v.metrics != nil {
		v.metrics.Mark("tail_position_rpc")
	}
	if err != nil {
		return cfxBlockRef{}, errors.WithMessagef(
			err, "failed to get block summary for hash %s", hash,
		)
	}

	ref, err := newCfxBlockRef(summary)
	if err != nil {
		return cfxBlockRef{}, err
	}

	if ref.epochNumber > v.checkpointEpoch || ref.blockNumber > v.checkpoint.blockNumber {
		return cfxBlockRef{}, newCanonicalDependentError(
			ErrScanLogsConsistency,
			"block %s belongs to epoch %d/%d, which is above checkpoint epoch %d/%d",
			hash, ref.epochNumber, ref.blockNumber, v.checkpointEpoch, v.checkpoint.blockNumber,
		)
	}

	v.remember(ref)
	return ref, nil
}

// resolveBlockPlan materializes only the endpoints needed by the actual
// direction/cursor.
func (v *cfxFNAttemptView) resolveBlockPlan(
	gen cfxScanGeneration,
	segment scanSegment,
	reverse bool,
) (cfxFNBlockPlan, error) {
	if gen.fnEpochs.empty() {
		return cfxFNBlockPlan{}, errors.New("empty epoch range")
	}

	var cursorRef *cfxBlockRef
	if segment.cursor != nil {
		ref, err := v.block(segment.cursor.BlockNumber)
		if err != nil {
			return cfxFNBlockPlan{}, err
		}

		if !gen.fnEpochs.contains(ref.epochNumber) {
			return cfxFNBlockPlan{}, newCanonicalDependentError(
				ErrScanLogsInvalidCursor,
				"cursor block %d belongs to epoch %d outside of segment epochs [%d, %d]",
				segment.cursor.BlockNumber,
				ref.epochNumber,
				gen.fnEpochs.From,
				gen.fnEpochs.To,
			)
		}
		cursorRef = &ref
	}

	var fromBlock uint64
	if segment.cursor != nil && !reverse {
		fromBlock = segment.cursor.BlockNumber
	} else {
		resolved, err := v.firstBlockOfEpoch(gen)
		if err != nil {
			return cfxFNBlockPlan{}, err
		}
		fromBlock = resolved
	}

	var toBlock uint64
	if segment.cursor != nil && reverse {
		toBlock = segment.cursor.BlockNumber
	} else {
		pivot, err := v.pivot(gen.fnEpochs.To)
		if err != nil {
			return cfxFNBlockPlan{}, err
		}
		toBlock = pivot.blockNumber
	}

	blocks := scanRange{From: fromBlock, To: toBlock}
	if blocks.empty() {
		return cfxFNBlockPlan{}, newCanonicalDependentError(
			ErrScanLogsConsistency,
			"epochs [%d, %d] resolved to invalid blocks [%d, %d]",
			gen.fnEpochs.From, gen.fnEpochs.To, fromBlock, toBlock,
		)
	}

	if segment.cursor != nil && !blocks.contains(segment.cursor.BlockNumber) {
		return cfxFNBlockPlan{}, newCanonicalDependentError(
			ErrScanLogsInvalidCursor, "cursor is outside the resolved block range",
		)
	}

	plan := cfxFNBlockPlan{
		blocks: blocks, cursor: cloneScanCursor(segment.cursor),
	}
	if cursorRef != nil {
		hash := cursorRef.hash
		plan.cursorHash = &hash
	}
	return plan, nil
}

// firstBlockOfEpoch uses the previous epoch's pivot BN plus one. Core block
// numbers are the global execution-order coordinate and an epoch's pivot is its
// final executed block, so this is the inclusive lower boundary of the next
// epoch. Even if a node implementation leaves numeric gaps, querying the gap is
// harmless: it cannot include logs from the previous epoch.
//
// At the DB/FN boundary the same value is already captured as dbMaxBlock+1 and
// needs no RPC. A request that starts farther above the DB watermark is a pure
// FN range and therefore still resolves its own preceding pivot.
func (v *cfxFNAttemptView) firstBlockOfEpoch(gen cfxScanGeneration) (uint64, error) {
	fromEpoch := gen.fnEpochs.From
	if fromEpoch == 0 {
		return 0, nil
	}

	if gen.dbAvailable && fromEpoch == gen.dbMaxEpoch+1 {
		return gen.dbMaxBlock + 1, nil
	}

	previous, err := v.pivot(fromEpoch - 1)
	if err != nil {
		return 0, err
	}

	return previous.blockNumber + 1, nil
}

func (v *cfxFNAttemptView) tailPosition(log cfxtypes.Log) (*store.ScanCursor, error) {
	if err := validateCfxFNLog(log); err != nil {
		return nil, errors.WithMessage(err, "incomplete tail log")
	}

	ref, err := v.blockByHash(*log.BlockHash)
	if err != nil {
		return nil, err
	}

	logIndex := log.LogIndex.ToInt().Uint64()

	return &store.ScanCursor{BlockNumber: ref.blockNumber, LogIndex: logIndex}, nil
}

func newCfxBlockRef(summary *cfxtypes.BlockSummary) (cfxBlockRef, error) {
	if summary == nil || summary.EpochNumber == nil || summary.BlockNumber == nil {
		return cfxBlockRef{}, errors.New("incomplete block summary")
	}

	if summary.Hash == "" {
		return cfxBlockRef{}, errors.New("hash missing from block summary")
	}

	return cfxBlockRef{
		hash:        summary.Hash,
		epochNumber: summary.EpochNumber.ToInt().Uint64(),
		blockNumber: summary.BlockNumber.ToInt().Uint64(),
	}, nil
}

// cfxFNReaderSpec is fully block-native. Epoch semantics and epoch-to-block
// conversion belong to cfxFNAttemptView; the reader only executes one immutable
// physical segment and can therefore be replayed from the beginning on retry.
type cfxFNReaderSpec struct {
	blocks     scanRange
	filter     cfxtypes.LogFilter
	cursor     *store.ScanCursor
	cursorHash *cfxtypes.Hash
	reverse    bool
	windowSize uint64
}

type cfxFNReader struct {
	client  cfxScanClient
	attempt *cfxFNAttemptView
	spec    cfxFNReaderSpec
}

// Scan consumes one FN block segment. It makes cursor semantics
// deterministic before querying logs: the first physical window begins/ends at
// cursor.bn, so only logs whose blockHash equals the resolved cursor block hash
// need an exclusive logIndex comparison. Every other returned block is already
// on the valid side of the cursor even when the cursor block has no matching
// log for the current address/topic predicate.
func (r *cfxFNReader) Scan(ctx context.Context, remaining int) (fnSegmentBatch[cfxtypes.Log], error) {
	if remaining <= 0 || r.spec.blocks.empty() {
		return fnSegmentBatch[cfxtypes.Log]{}, nil
	}

	if r.attempt == nil {
		return fnSegmentBatch[cfxtypes.Log]{}, errors.New("missing attempt view")
	}

	var filterFirstWindow func([]cfxtypes.Log) []cfxtypes.Log
	if r.spec.cursor != nil {
		if r.spec.cursorHash == nil {
			return fnSegmentBatch[cfxtypes.Log]{}, errors.New("missing canonical block hash")
		}
		filterFirstWindow = func(logs []cfxtypes.Log) []cfxtypes.Log {
			return filterCfxCursorBlock(logs, *r.spec.cursor, *r.spec.cursorHash, r.spec.reverse)
		}
	}

	logs, err := scanFNBlockWindows(
		ctx,
		r.spec.blocks,
		r.spec.reverse,
		r.spec.windowSize,
		remaining,
		r.readWindow,
		filterFirstWindow,
	)
	if err != nil {
		return fnSegmentBatch[cfxtypes.Log]{}, err
	}

	batch := fnSegmentBatch[cfxtypes.Log]{Logs: logs}
	if len(logs) == 0 {
		return batch, nil
	}

	// Core space native logs omit blockNumber. Resolve only the final response log,
	// after filtering/reversing/truncation, rather than enriching every log.
	tail, err := r.attempt.tailPosition(logs[len(logs)-1])
	if err != nil {
		return fnSegmentBatch[cfxtypes.Log]{}, err
	}
	batch.TailPosition = tail
	return batch, validateFnBatch(batch, remaining)
}

// readWindow always rebuilds range fields from the immutable predicate. Epoch
// fields and blockHashes are cleared so no caller state can turn a Route-B
// block window into a mixed or ambiguous cfx_getLogs filter.
func (r *cfxFNReader) readWindow(ctx context.Context, from, to uint64) ([]cfxtypes.Log, error) {
	if err := checkTimeout(ctx); err != nil {
		return nil, err
	}

	filter := r.spec.filter
	filter.FromEpoch, filter.ToEpoch, filter.BlockHashes = nil, nil, nil
	filter.FromBlock = (*hexutil.Big)(new(big.Int).SetUint64(from))
	filter.ToBlock = (*hexutil.Big)(new(big.Int).SetUint64(to))

	logs, err := r.client.GetLogs(filter)
	if err != nil {
		return nil, err
	}
	for i := range logs {
		if err := validateCfxFNLog(logs[i]); err != nil {
			return nil, errors.WithMessagef(err, "invalid full node log at index %d", i)
		}
	}
	return logs, nil
}

// validateCfxFNLog checks every optional identity field used after the Reader
// boundary. Validation happens before cursor filtering, reversing or truncation
// so an incomplete non-tail log cannot panic or silently affect pagination.
func validateCfxFNLog(log cfxtypes.Log) error {
	if log.BlockHash == nil {
		return errors.New("missing block hash")
	}
	if log.EpochNumber == nil {
		return errors.New("missing epoch number")
	}
	if log.LogIndex == nil {
		return errors.New("missing log index")
	}
	return nil
}

func filterCfxCursorBlock(
	logs []cfxtypes.Log,
	cursor store.ScanCursor,
	cursorHash cfxtypes.Hash,
	reverse bool,
) []cfxtypes.Log {
	kept := make([]cfxtypes.Log, 0, len(logs))
	for i := range logs {
		log := logs[i]
		if !equalCfxHash(*log.BlockHash, cursorHash) {
			// The physical query boundary excludes all blocks on the consumed side,
			// so a different hash is always retained regardless of its local index.
			kept = append(kept, log)
			continue
		}

		index := log.LogIndex.ToInt().Uint64()
		if (!reverse && index > cursor.LogIndex) || (reverse && index < cursor.LogIndex) {
			kept = append(kept, log)
		}
	}
	return kept
}

var _ fnSegmentReader[cfxtypes.Log] = (*cfxFNReader)(nil)
