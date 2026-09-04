package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	cacheTypes "github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/types"
	metricUtil "github.com/Conflux-Chain/go-conflux-util/metrics"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
)

type scanLogsMetricsRecorder interface {
	Histogram(name string, value int64)
	Mark(name string)
	Percentage(name string, marked bool)
	Duration(name string, started time.Time)
}

type registryScanLogsMetrics struct{ method string }

func (m registryScanLogsMetrics) Histogram(name string, value int64) {
	metricUtil.GetOrRegisterHistogram("infura/rpc/%v/%v", m.method, name).Update(value)
}

func (m registryScanLogsMetrics) Mark(name string) {
	metricUtil.GetOrRegisterMeter("infura/rpc/%v/%v", m.method, name).Mark(1)
}

func (m registryScanLogsMetrics) Percentage(name string, marked bool) {
	metricUtil.GetOrRegisterTimeWindowPercentageDefault("infura/rpc/%v/%v", m.method, name).Mark(marked)
}

func (m registryScanLogsMetrics) Duration(name string, started time.Time) {
	metricUtil.GetOrRegisterTimer("infura/rpc/%v/%v", m.method, name).UpdateSince(started)
}

type scanLogsMetricsContextKey struct{}

func withScanLogsMetrics(ctx context.Context, recorder scanLogsMetricsRecorder) context.Context {
	return context.WithValue(ctx, scanLogsMetricsContextKey{}, recorder)
}

func scanLogsMetricsFromContext(ctx context.Context) scanLogsMetricsRecorder {
	recorder, _ := ctx.Value(scanLogsMetricsContextKey{}).(scanLogsMetricsRecorder)
	return recorder
}

func markScanLogsMetric(ctx context.Context, name string) {
	if recorder := scanLogsMetricsFromContext(ctx); recorder != nil {
		recorder.Mark(name)
	}
}

func recordScanLogsHistogram(ctx context.Context, name string, value int64) {
	if recorder := scanLogsMetricsFromContext(ctx); recorder != nil {
		recorder.Histogram(name, value)
	}
}

func recordScanLogsPercentage(ctx context.Context, name string, marked bool) {
	if recorder := scanLogsMetricsFromContext(ctx); recorder != nil {
		recorder.Percentage(name, marked)
	}
}

func newScanLogsMetrics(space string, withPivotAssumption bool) scanLogsMetricsRecorder {
	method := space + "_scanLogs"
	if withPivotAssumption {
		method += "WithPivotAssumption"
	}
	return registryScanLogsMetrics{method: method}
}

var (
	ErrScanLogsUnavailable = errors.New("scan logs rpc unavailable")

	ErrScanLogsInvalidParams = errors.New("invalid scan logs params")
	ErrScanLogsInvalidCursor = errors.New("invalid scan logs cursor")

	ErrScanLogsConsistency       = errors.New("inconsistent canonical views")
	ErrScanLogsAssumptionFailure = errors.New("pivot assumption failed")
)

// scanLogsError keeps the stable client-facing category outside the concrete
// cause. This makes the message read "category: cause" while preserving both
// errors.Is category matching and conventional cause traversal.
type scanLogsError struct {
	category error
	cause    error
}

func (e *scanLogsError) Error() string        { return fmt.Sprintf("%s: %s", e.category, e.cause) }
func (e *scanLogsError) Unwrap() error        { return e.cause }
func (e *scanLogsError) Cause() error         { return e.cause }
func (e *scanLogsError) Is(target error) bool { return errors.Is(e.category, target) }

// NewScanLogsError constructs a categorized scanLogs error. The category is
// the externally visible manifestation; cause is the concrete failure reason.
// Neither the returned error nor its category implements a JSON-RPC error code.
func NewScanLogsError(category, cause error) error {
	if category == nil {
		return cause
	}
	if cause == nil {
		return category
	}
	return &scanLogsError{category: category, cause: cause}
}

// NewScanLogsErrorf is the formatted counterpart of NewScanLogsError.
func NewScanLogsErrorf(category error, format string, args ...any) error {
	return NewScanLogsError(category, errors.Errorf(format, args...))
}

// canonicalDependentError marks an error observed from the current canonical
// chain view. It is provisional until the applicable FN after-check and DB v1
// check both pass. The wrapped error still carries the client-facing category;
// this wrapper only controls when the error may be published.
type canonicalDependentError struct{ err error }

func (e *canonicalDependentError) Error() string { return e.err.Error() }
func (e *canonicalDependentError) Unwrap() error { return e.err }
func (e *canonicalDependentError) Cause() error  { return errors.Cause(e.err) }

func newCanonicalDependentError(category error, format string, args ...any) error {
	return &canonicalDependentError{err: NewScanLogsErrorf(category, format, args...)}
}

func isCanonicalDependentError(err error) bool {
	var target *canonicalDependentError
	return errors.As(err, &target)
}

type scanSource uint8

const (
	scanSourceDB scanSource = iota + 1
	scanSourceFN
)

// ScanLogCursor is the public exclusive keyset cursor shared by Core space and eSpace.
// Store has the same logical key in its own package; conversion happens only
// at the Handler -> Store boundary so the public JSON type does not leak into
// storage APIs and the storage type does not leak into RPC parameters.
type ScanLogCursor struct {
	BlockNumber hexutil.Uint64 `json:"blockNumber"`
	LogIndex    hexutil.Uint64 `json:"logIndex"`
}

func (cursor *ScanLogCursor) UnmarshalJSON(data []byte) error {
	type plain ScanLogCursor
	var decoded plain
	if err := unmarshalStrictJSONObject(data, &decoded); err != nil {
		return errors.WithMessage(err, "invalid scan cursor")
	}
	if err := validateJSONObjectFields(
		data,
		[]string{"blockNumber", "logIndex"},
		[]string{"blockNumber", "logIndex"},
	); err != nil {
		return err
	}
	*cursor = ScanLogCursor(decoded)
	return nil
}

// unmarshalStrictJSONObject is shared by every public scanLogs request object.
// json.Decoder's DisallowUnknownFields only applies to the object currently
// being decoded; nested scanLogs types implement UnmarshalJSON themselves so
// strictness is recursive rather than only protecting the top-level request.
func unmarshalStrictJSONObject(data []byte, dst any) error {
	trimmed := bytes.TrimSpace(data)
	if bytes.Equal(trimmed, []byte("null")) {
		return errors.New("object must not be null")
	}

	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return errors.New("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}

func normalizeScanLogsLimit(limit hexutil.Uint64) (hexutil.Uint64, error) {
	if limit == 0 {
		return hexutil.Uint64(defaultScanLogsLimit), nil
	}
	if uint64(limit) > maxScanLogsLimit {
		return 0, errors.Errorf(
			"page limit %d exceeds configured maximum %d", limit, maxScanLogsLimit,
		)
	}
	return limit, nil
}

// EncodeScanLogsResult serializes a result once, enforces the response-size
// limit on those exact bytes, and returns a Lazy value that reuses the payload.
func EncodeScanLogsResult[T any](result T) (cacheTypes.Lazy[T], error) {
	payload, err := json.Marshal(result)
	if err != nil {
		return cacheTypes.Lazy[T]{}, errors.WithMessage(err, "failed to encode scan logs result")
	}
	if uint64(len(payload)) > maxGetLogsResponseBytes {
		return cacheTypes.Lazy[T]{}, errors.Errorf(
			"result body size is too large with more than %d bytes, please reduce scan limit",
			maxGetLogsResponseBytes,
		)
	}
	return cacheTypes.NewLazyWithJson[T](payload), nil
}

func validateJSONObjectFields(data []byte, required []string, nonNull []string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}

	for _, name := range required {
		if _, ok := fields[name]; !ok {
			return errors.Errorf("missing required field %q", name)
		}
	}
	for _, name := range nonNull {
		if raw, ok := fields[name]; ok && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return errors.Errorf("field %q must not be null", name)
		}
	}
	return nil
}

func (cursor *ScanLogCursor) toStoreCursor() *store.ScanCursor {
	if cursor == nil {
		return nil
	}
	return &store.ScanCursor{
		BlockNumber: uint64(cursor.BlockNumber), LogIndex: uint64(cursor.LogIndex),
	}
}

func newScanCursor(cursor *store.ScanCursor) *ScanLogCursor {
	if cursor == nil {
		return nil
	}
	return &ScanLogCursor{
		BlockNumber: hexutil.Uint64(cursor.BlockNumber),
		LogIndex:    hexutil.Uint64(cursor.LogIndex),
	}
}

func (cursor *ScanLogCursor) clone() *ScanLogCursor {
	if cursor == nil {
		return nil
	}
	copy := *cursor
	return &copy
}

type scanRange types.RangeUint64

// emptyScanRange is the canonical empty inclusive range. scanRange's zero
// value cannot mean empty because [0, 0] is a valid range containing height 0.
// Use this named value instead of scattering the non-obvious {From: 1, To: 0}
// sentinel throughout the planners.
var emptyScanRange = scanRange{From: 1, To: 0}

func (r scanRange) contains(v uint64) bool { return r.From <= v && v <= r.To }
func (r scanRange) empty() bool            { return r.From > r.To }

type scanSegment struct {
	// cursor is non-nil for at most one segment in a plan. The segment range is
	// stored by the space-specific generation because Core space FN uses epochs while
	// DB/eSpace use block numbers.
	source scanSource
	cursor *store.ScanCursor
}

type scanPlan struct {
	segments []scanSegment
}

func (plan scanPlan) contains(source scanSource) bool {
	for _, segment := range plan.segments {
		if segment.source == source {
			return true
		}
	}
	return false
}

func (plan scanPlan) startsWith(source scanSource) bool {
	return len(plan.segments) > 0 && plan.segments[0].source == source
}

func (plan scanPlan) cursorFor(source scanSource) *store.ScanCursor {
	for _, segment := range plan.segments {
		if segment.source == source {
			return cloneScanCursor(segment.cursor)
		}
	}
	return nil
}

type cursorOwner uint8

const (
	cursorOwnerNone cursorOwner = iota
	cursorOwnerDB
	cursorOwnerFN
)

func recordScanLogsCursorOwner(ctx context.Context, owner cursorOwner) {
	recordScanLogsPercentage(ctx, "plan/cursor_owner/none", owner == cursorOwnerNone)
	recordScanLogsPercentage(ctx, "plan/cursor_owner/db", owner == cursorOwnerDB)
	recordScanLogsPercentage(ctx, "plan/cursor_owner/fn", owner == cursorOwnerFN)
}

type canonicalCommitDecision uint8

const (
	canonicalCommit canonicalCommitDecision = iota
	canonicalRetryInner
	canonicalRetryOuter
)

// decideCanonicalCommit is the common success/error commit gate. DB version
// changes always invalidate the outer generation and its cache. With a stable
// DB generation, an FN checkpoint change or a DB/FN boundary mismatch retries
// only the inner FN attempt and therefore reuses the DB cache.
func decideCanonicalCommit(dbVersionStable, fnCheckpointStable, boundaryAligned bool) canonicalCommitDecision {
	if !dbVersionStable {
		return canonicalRetryOuter
	}
	if !fnCheckpointStable || !boundaryAligned {
		return canonicalRetryInner
	}
	return canonicalCommit
}

// buildScanPlan turns the source split and the one-time cursor placement into
// at most two already ordered segments. The complete request cursor is never
// copied into both sources: only its owner receives it and sources that precede
// the owner in response order are omitted.
//
// The table encoded below is deliberately explicit:
//
//	no cursor: forward DB -> FN, reverse FN -> DB
//	DB cursor: forward DB(cursor) -> FN, reverse DB(cursor)
//	FN cursor: forward FN(cursor), reverse FN(cursor) -> DB
//
// Because ranges do not overlap and the plan is already in response order, the
// runner can append batches directly. A merge/sort layer would only obscure
// cursor exclusivity and create another place for duplicate/omission bugs.
func buildScanPlan(hasDB, hasFN, reverse bool, owner cursorOwner, cursor *store.ScanCursor) scanPlan {
	appendSegment := func(plan *scanPlan, source scanSource, segmentCursor *store.ScanCursor) {
		plan.segments = append(plan.segments, scanSegment{
			source: source,
			cursor: cloneScanCursor(segmentCursor),
		})
	}

	var plan scanPlan
	switch owner {
	case cursorOwnerDB:
		if hasDB {
			appendSegment(&plan, scanSourceDB, cursor)
		}
		if !reverse && hasFN {
			appendSegment(&plan, scanSourceFN, nil)
		}
	case cursorOwnerFN:
		if hasFN {
			appendSegment(&plan, scanSourceFN, cursor)
		}
		if reverse && hasDB {
			appendSegment(&plan, scanSourceDB, nil)
		}
	default:
		if reverse {
			if hasFN {
				appendSegment(&plan, scanSourceFN, nil)
			}
			if hasDB {
				appendSegment(&plan, scanSourceDB, nil)
			}
		} else {
			if hasDB {
				appendSegment(&plan, scanSourceDB, nil)
			}
			if hasFN {
				appendSegment(&plan, scanSourceFN, nil)
			}
		}
	}

	return plan
}

func cloneScanCursor(cursor *store.ScanCursor) *store.ScanCursor {
	if cursor == nil {
		return nil
	}
	copy := *cursor
	return &copy
}

// classifyCursorOwner decides which one of the two disjoint block segments may
// receive the request cursor.
//
// Example: the request's materialized DB segment is BN [1000, 1500], and 1500
// is also the DB watermark.
//   - cursor BN 1200 belongs to DB;
//   - cursor BN 900 is invalid because it is outside this request's DB segment;
//   - cursor BN 1700 is an FN candidate and must never be passed to Store.
//
// The FN case is only a candidate here. eSpace checks it against its already
// frozen FN block range; Core first materializes its epoch-native FN range into
// an attempt-local block range. Returning one owner lets `buildScanPlan` give the
// cursor to exactly one segment and omit any segment that lies before it in
// response order.
func classifyCursorOwner(
	cursor *store.ScanCursor, dbBlocks scanRange, dbBlockWatermark uint64,
) (cursorOwner, error) {
	if cursor == nil {
		return cursorOwnerNone, nil
	}

	if cursor.BlockNumber > dbBlockWatermark {
		return cursorOwnerFN, nil
	}

	if dbBlocks.empty() {
		return cursorOwnerNone, errors.New("cursor is specified while the split DB segment is empty")
	}

	if !dbBlocks.contains(cursor.BlockNumber) {
		return cursorOwnerNone, errors.Errorf(
			"cursor %d is outside the block range [%d, %d] of the split DB segment",
			cursor.BlockNumber, dbBlocks.From, dbBlocks.To,
		)
	}
	return cursorOwnerDB, nil
}

// clipBlockRangeAtCursor turns a public exclusive cursor into the physical
// boundary of its owning block segment. The cursor block stays in the query so
// the reader can apply the logIndex predicate within that one block; blocks on
// the already-consumed side are never queried again.
//
// A cursor must be clipped only after owner placement. Passing the complete
// request cursor through every segment would make Store reject a cursor owned
// by FN (and vice versa), or cause one source to silently rescan another
// source's range.
func clipBlockRangeAtCursor(blocks scanRange, cursor *store.ScanCursor, reverse bool) (scanRange, error) {
	if cursor == nil {
		return blocks, nil
	}

	if blocks.empty() || !blocks.contains(cursor.BlockNumber) {
		return emptyScanRange, errors.Errorf(
			"cursor %d is outside block range [%d, %d]",
			cursor.BlockNumber, blocks.From, blocks.To,
		)
	}

	if reverse {
		blocks.To = cursor.BlockNumber
	} else {
		blocks.From = cursor.BlockNumber
	}
	return blocks, nil
}

var (
	defaultScanLogsLimit    = uint64(100)
	maxScanLogsLimit        = uint64(1_000)
	defaultScanLogsFNWindow = uint64(1_000)
)

const (
	boundaryRetryBackoff = 10 * time.Millisecond

	// A boundary mismatch can be a transient FN reorg, so one fresh FN view is
	// useful. Repeating forever is unsafe operationally: the DB may still contain
	// the stale data from the pre-reorg fork, in which case no amount of FN replay
	// can align it. After one additional attempt we fail fast and let
	// the caller retry after the indexer advances.
	maxBoundaryInnerRetries   = 1
	maxCheckpointInnerRetries = 1
)

type fnSegmentBatch[L any] struct {
	// Logs are already in final response direction and already truncated to the
	// supplied remaining limit.
	Logs []L
	// TailPosition is the public cursor key of Logs[len(Logs)-1]. It is batch
	// metadata, not the final page cursor: a later segment may replace it.
	TailPosition *store.ScanCursor
}

// fnSegmentReader is intentionally narrow. It owns only one immutable FN
// segment and can be recreated/replayed from the beginning on every inner
// retry. It must not split DB/FN ranges, close the canonical fence, construct a
// PivotGuard, or decide page-level cursor semantics.
type fnSegmentReader[L any] interface {
	Scan(ctx context.Context, remaining int) (fnSegmentBatch[L], error)
}

func validateFnBatch[L any](batch fnSegmentBatch[L], remaining int) error {
	if len(batch.Logs) > remaining {
		return errors.Errorf("segment returned %d logs with remaining limit %d", len(batch.Logs), remaining)
	}
	if (len(batch.Logs) == 0) != (batch.TailPosition == nil) {
		return errors.New("segment tail position does not match an empty/non-empty batch")
	}
	return nil
}

// appendUpTo appends only the capacity still available in a page. Full Node
// ordering and range membership are part of the getLogs contract.
func appendUpTo[L any](dst, src []L, limit int) []L {
	want := limit - len(dst)
	if want <= 0 {
		return dst
	}

	if len(src) > want {
		src = src[:want]
	}
	return append(dst, src...)
}

// scanFNBlockWindows contains the source-independent mechanics of a block
// range scan: directional window traversal, result-too-large shrinking,
// first-window cursor filtering, final-direction ordering and page truncation.
// Core space/eSpace adapters remain responsible for constructing their native RPC
// filters and deriving a tail cursor.
func scanFNBlockWindows[L any](
	ctx context.Context,
	blocks scanRange,
	reverse bool,
	windowSize uint64,
	remaining int,
	readWindow func(context.Context, uint64, uint64) ([]L, error),
	filterFirstWindow func([]L) []L,
) ([]L, error) {
	if remaining <= 0 || blocks.empty() {
		return nil, nil
	}
	if windowSize == 0 {
		windowSize = defaultScanLogsFNWindow
	}

	logs := make([]L, 0, remaining)
	firstWindow := true
	consume := func(windowLogs []L) {
		if firstWindow && filterFirstWindow != nil {
			windowLogs = filterFirstWindow(windowLogs)
		}
		firstWindow = false
		if reverse {
			slices.Reverse(windowLogs)
		}
		logs = appendUpTo(logs, windowLogs, remaining)
	}

	if reverse {
		for high := blocks.To; ; {
			low := blocks.From
			// high-blocks.From avoids the overflow in high-from+1 for the
			// theoretical full uint64 range.
			if high-blocks.From >= windowSize {
				low = high - windowSize + 1
			}

			windowLogs, err := readWindow(ctx, low, high)
			markScanLogsMetric(ctx, "fn/window")
			if err != nil {
				if isWhitelistedFNOversizedError(err) && low < high {
					markScanLogsMetric(ctx, "fn/shrink")
					windowSize = max(uint64(1), (high-low+1)/2)
					continue
				}
				return nil, err
			}
			consume(windowLogs)
			if len(logs) == remaining || low == blocks.From {
				break
			}
			high = low - 1
		}
		return logs, nil
	}

	for low := blocks.From; ; {
		high := blocks.To
		if blocks.To-low >= windowSize {
			high = low + windowSize - 1
		}

		windowLogs, err := readWindow(ctx, low, high)
		markScanLogsMetric(ctx, "fn/window")
		if err != nil {
			if isWhitelistedFNOversizedError(err) && low < high {
				markScanLogsMetric(ctx, "fn/shrink")
				windowSize = max(uint64(1), (high-low+1)/2)
				continue
			}
			return nil, err
		}
		consume(windowLogs)
		if len(logs) == remaining || high == blocks.To {
			break
		}
		low = high + 1
	}
	return logs, nil
}

// dbScanCache is scoped to one outer DB generation. Ensure incrementally grows
// a response-direction prefix and uses the cached tail as an internal exclusive
// cursor, which is required by reverse FN->DB pages whose FN retry may change
// the remaining capacity.
type dbScanCache[L any] struct {
	// scan returns native logs plus a parallel key sidecar. The sidecar exists
	// only inside the DB cache for two reasons:
	//
	//  1. Core space's native types.Log has no blockNumber, so after a Store row is
	//     converted to the final SDK log there is no way to reconstruct its
	//     public (blockNumber, logIndex) cursor without keeping this key.
	//  2. An FN retry may change how many cached DB rows the current page consumes.
	//     Tail(n) therefore needs the key of the consumed prefix, not necessarily
	//     the key at the end of everything a previous attempt cached.
	//
	// keys is not returned to the RPC client, carries no FN/canonical metadata,
	// and deliberately avoids wrapping every result in a larger scanEntry union.
	scan      func(context.Context, *store.ScanCursor, int) ([]L, []store.ScanCursor, error)
	initial   *store.ScanCursor
	logs      []L
	keys      []store.ScanCursor
	exhausted bool
}

func (c *dbScanCache[L]) Ensure(ctx context.Context, n int) error {
	// n is a desired total cached prefix, not an incremental count. This matters
	// for reverse FN->DB: after an FN retry, remaining may grow from 20 to 40 and
	// only the additional 20 DB rows should be queried.
	if n <= len(c.logs) || c.exhausted {
		markScanLogsMetric(ctx, "db/cache_reuse")
		return nil
	}

	cursor := cloneScanCursor(c.initial)
	if len(c.logs) > 0 {
		// The cached tail is an internal exclusive cursor. Reusing the original
		// request cursor here would refetch the already cached prefix.
		cursor = c.Tail(len(c.logs))
	}

	want := n - len(c.logs)
	if len(c.logs) == 0 {
		markScanLogsMetric(ctx, "db/query")
	} else {
		markScanLogsMetric(ctx, "db/cache_extend")
	}
	logs, keys, err := c.scan(ctx, cursor, want)
	if err != nil {
		return err
	}
	if len(logs) > want {
		return errors.Errorf("scan returned %d logs with remaining cache limit %d", len(logs), want)
	}
	if len(logs) != len(keys) {
		return errors.New("scan log/key sidecars have different lengths")
	}

	c.logs = append(c.logs, logs...)
	c.keys = append(c.keys, keys...)

	if len(logs) < want {
		// A short Store scan means this DB segment is exhausted. Remember the
		// negative result so subsequent FN retries do not repeat an empty query.
		c.exhausted = true
	}
	return nil
}

func (c *dbScanCache[L]) Tail(n int) *store.ScanCursor {
	// Tail is relative to the prefix consumed by the current candidate. For
	// example, an earlier FN attempt may have cached 40 DB rows while a later FN
	// attempt leaves room for only 20; the correct page cursor is keys[19], not
	// the cache's last key keys[39].
	if n <= 0 || len(c.keys) == 0 {
		return nil
	}

	if n > len(c.keys) {
		n = len(c.keys)
	}
	return cloneScanCursor(&c.keys[n-1])
}

func (c *dbScanCache[L]) Prefix(n int) []L {
	if n > len(c.logs) {
		n = len(c.logs)
	}
	return c.logs[:n]
}

// These exact strings are the stable production-node forms accepted for
// window shrinking. A code match alone is deliberately insufficient: an
// arbitrary fullnode failure must not be hidden by repeatedly narrowing the
// range. Additional node-version samples should be added as exact entries.
var fnOversizedMessages = map[string]struct{}{
	"the query set is too large":                               {},
	"this query results in too many logs":                      {},
	"the result set exceeds the max limit":                     {},
	"the query timed out after exceeding the maximum duration": {},
}

func isWhitelistedFNOversizedError(err error) bool {
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	if _, ok := fnOversizedMessages[message]; ok {
		return true
	}
	return false
}

func waitBoundaryRetry(ctx context.Context) error {
	// Stable boundary mismatch means both views are individually stable but not
	// yet aligned. Back off before rebuilding only the FN attempt; a tight loop
	// would overload the node while waiting for it to converge with the indexer.
	timer := time.NewTimer(boundaryRetryBackoff)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
