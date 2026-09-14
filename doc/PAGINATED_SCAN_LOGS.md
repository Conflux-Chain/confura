# Paginated Event Log Scanning with `scanLogs`

## Overview

Confura provides cursor-paginated event log scanning for both Conflux Core Space and eSpace. The APIs are designed for applications that need to walk a large log range in deterministic, bounded pages without manually guessing a safe epoch or block window.

The four RPC methods are:

| Space | Basic pagination | Pagination with reorg detection |
|:------|:-----------------|:--------------------------------|
| Core Space | `cfx_scanLogs` | `cfx_scanLogsWithPivotAssumption` |
| eSpace | `eth_scanLogs` | `eth_scanLogsWithPivotAssumption` |

Each successful call returns standard chain-native log objects plus an exclusive `nextCursor`. A continuation sends that cursor back with the same filter and direction. Confura resumes strictly after the cursor in forward mode or strictly before it in reverse mode, so the boundary log is not returned twice.

Use `scanLogs` when you need to:

- export or backfill logs over a large historical range;
- process logs in bounded batches with a stable ordering;
- avoid range-splitting logic based on full-node `getLogs` limits;
- resume a scan from a persisted checkpoint; or
- detect a reorganization between pages with the pivot-assumption variants.

`scanLogs` complements rather than replaces `cfx_getLogs` and `eth_getLogs`:

| Requirement | Recommended API |
|:------------|:----------------|
| Return all matching logs in one response when the result is known to be small | `cfx_getLogs` or `eth_getLogs` |
| Process a potentially large result set page by page | `cfx_scanLogs` or `eth_scanLogs` |
| Detect whether already-scanned progress was invalidated by a reorganization | `cfx_scanLogsWithPivotAssumption` or `eth_scanLogsWithPivotAssumption` |

## Quick Start

The following eSpace request scans forward through blocks `0x100000` to `0x200000`, returning at most 100 matching logs:

```shell
curl -X POST http://127.0.0.1:8545 \
  -H 'Content-Type: application/json' \
  -d '{
    "jsonrpc": "2.0",
    "method": "eth_scanLogs",
    "params": [
      {
        "filter": {
          "fromBlock": "0x100000",
          "toBlock": "0x200000",
          "address": "0x1111111111111111111111111111111111111111",
          "topic0": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        },
        "limit": 100
      }
    ],
    "id": 1
  }'
```

An abbreviated response has this shape:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "logs": [
      {
        "address": "0x1111111111111111111111111111111111111111",
        "topics": [
          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        ],
        "blockNumber": "0x100120",
        "logIndex": "0x3"
      }
    ],
    "nextCursor": {
      "blockNumber": "0x100120",
      "logIndex": "0x3"
    }
  }
}
```

To fetch the next page, keep the filter, range, limit, and direction unchanged and add the returned cursor:

```json
{
  "jsonrpc": "2.0",
  "method": "eth_scanLogs",
  "params": [
    {
      "filter": {
        "fromBlock": "0x100000",
        "toBlock": "0x200000",
        "address": "0x1111111111111111111111111111111111111111",
        "topic0": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
      },
      "limit": 100,
      "cursor": {
        "blockNumber": "0x100120",
        "logIndex": "0x3"
      }
    }
  ],
  "id": 2
}
```

## Request Reference

### JSON-RPC Parameters

The basic methods accept one request object:

```text
cfx_scanLogs(request)
eth_scanLogs(request)
```

The reorg-aware methods accept the same request followed by an optional pivot assumption:

```text
cfx_scanLogsWithPivotAssumption(request, pivotAssumption?)
eth_scanLogsWithPivotAssumption(request, pivotAssumption?)
```

Omit the assumption on the first page. Once a page returns `pivotGuard`, pass that object as the second positional parameter on the next request. An assumption is mandatory when a cursor is supplied to a `WithPivotAssumption` method.

### Shared Request Object

| Field | Type | Required | Default | Description |
|:------|:-----|:---------|:--------|:------------|
| `filter` | object | No | Empty filter | Range and indexed predicates. The Core Space and eSpace fields differ as described below. |
| `limit` | JSON integer | No | `100` | Maximum logs in this page. `0` also selects the default. The configured maximum is `1,000` by default. Do not encode this field as a hex or decimal string. |
| `cursor` | object | No | None | Exclusive position returned as `nextCursor` by the previous page. Both cursor fields are required when the object is present. |
| `reverse` | boolean | No | `false` | `false` scans in ascending order; `true` scans in descending order. |

The request decoder is strict at every object level. Unknown fields, an explicit `null`, malformed quantities, and incomplete cursor or pivot objects are rejected. Omitted optional fields are valid.

### Core Space Filter

The `filter` accepted by `cfx_scanLogs` and `cfx_scanLogsWithPivotAssumption` contains:

| Field | Type | Required | Default | Description |
|:------|:-----|:---------|:--------|:------------|
| `fromEpoch` | epoch number or tag | No | `latest_state` | Inclusive lower epoch bound. Numeric epochs use JSON-RPC hex quantity strings such as `"0x1"`. |
| `toEpoch` | epoch number or tag | No | `latest_state` | Inclusive upper epoch bound. An explicit numeric value later than the frozen `latest_state` is rejected. |
| `address` | base32 address | No | Any address | One Core Space contract address. This is a single string, not the address array accepted by `cfx_getLogs`. |
| `topic0` | 32-byte hash | No | Any topic | Exact first topic, normally the event signature hash. Other topic positions and OR-arrays are not supported. |

Range tags are resolved once at the beginning of each request. If only one historical endpoint is supplied, remember that the missing endpoint defaults to `latest_state`; for predictable historical scans, explicitly set both bounds.

### eSpace Filter

The `filter` accepted by `eth_scanLogs` and `eth_scanLogsWithPivotAssumption` contains:

| Field | Type | Required | Default | Description |
|:------|:-----|:---------|:--------|:------------|
| `fromBlock` | block number or tag | No | `latest` | Inclusive lower block bound. Numeric blocks use JSON-RPC hex quantity strings. |
| `toBlock` | block number or tag | No | `latest` | Inclusive upper block bound. An explicit numeric value later than the frozen latest block is rejected. |
| `address` | 20-byte address | No | Any address | One eSpace contract address. This is a single string, not the address array accepted by `eth_getLogs`. |
| `topic0` | 32-byte hash | No | Any topic | Exact first topic, normally the event signature hash. Other topic positions and OR-arrays are not supported. |

Node-supported block tags such as `latest`, `safe`, and `finalized` are resolved to a numeric block once per request. eSpace ranges are normalized against the configured eSpace transition height; a range wholly before that height returns no logs, while a crossing range starts at the transition height.

### Cursor

The same cursor shape is used in both spaces:

```json
{
  "blockNumber": "0x100120",
  "logIndex": "0x3"
}
```

Both values are JSON-RPC hex quantities. The cursor ordering key is the pair `(blockNumber, logIndex)`:

- forward mode returns keys greater than the cursor;
- reverse mode returns keys less than the cursor;
- without a cursor, scanning starts at the selected range boundary.

The cursor is exclusive. Do not increment `blockNumber` or `logIndex`, and do not build a cursor from the visible log object yourself. In particular, Core Space log objects do not expose all of the physical block-number information used by the scanner. Treat `nextCursor` as an opaque checkpoint and return it unchanged.

A cursor is not cryptographically bound to its original request. The client is responsible for preserving `filter`, bounds, and `reverse` across pages. Changing them can intentionally produce a different scan, but can also skip or repeat logs.

## Response and Completion Rules

All four methods return an object with these fields:

| Field | Present | Description |
|:------|:--------|:------------|
| `logs` | Always | An array of standard `cfx_getLogs` or `eth_getLogs` log objects in response order. An exhausted scan returns an empty array. |
| `nextCursor` | When `logs` is non-empty | Position of the final log in response order. It is returned even when the current page happens to be the last page. |
| `pivotGuard` | Reorg-aware methods when a guard can be established | Canonical block identity to pass as the next request's pivot assumption. It is omitted by the basic methods. |

Use either of these completion strategies:

1. Stop immediately when `logs.length` is smaller than the effective page size (`limit`, or 100 when `limit` is omitted or zero); the requested range has been exhausted.
2. Always continue with `nextCursor` until an empty `logs` array is returned. This is simpler and costs one final request when the last non-empty page contains exactly `limit` logs.

Do not interpret the mere presence of `nextCursor` as a `hasMore` flag.

### Ordering

With `reverse: false`, results are ordered by ascending `(blockNumber, logIndex)`. With `reverse: true`, results are ordered by the same key in descending order. The scanner preserves this order when a page crosses the boundary between indexed database data and the latest full-node data.

## Core Space Examples

### Forward Scan

This example scans `Transfer(address,address,uint256)` events from a regular Core Space contract:

```shell
curl -X POST http://127.0.0.1:22537 \
  -H 'Content-Type: application/json' \
  -d '{
    "jsonrpc": "2.0",
    "method": "cfx_scanLogs",
    "params": [
      {
        "filter": {
          "fromEpoch": "0x9fdef7",
          "toEpoch": "0x9fdf03",
          "address": "cfx:acckucyy5fhzknbxmeexwtaj3bxmeg25b2b50pta6v",
          "topic0": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        },
        "limit": 100
      }
    ],
    "id": 1
  }'
```

### Reverse Scan

Set `reverse` on the first page and every continuation:

```json
{
  "jsonrpc": "2.0",
  "method": "cfx_scanLogs",
  "params": [
    {
      "filter": {
        "fromEpoch": "0x100000",
        "toEpoch": "0x200000"
      },
      "limit": 250,
      "reverse": true
    }
  ],
  "id": 1
}
```

The first result is the highest matching log in the requested epoch range. The returned cursor resumes below the last log in that page.

## eSpace Examples

### Unfiltered Bounded Scan

Address and topic are optional. The following scans every eSpace log in the selected block range in pages of 500:

```json
{
  "jsonrpc": "2.0",
  "method": "eth_scanLogs",
  "params": [
    {
      "filter": {
        "fromBlock": "0x100000",
        "toBlock": "0x110000"
      },
      "limit": 500
    }
  ],
  "id": 1
}
```

Broad filters are supported, but a dense range can still hit the request timeout. Prefer an address and/or `topic0` when they express the application requirement.

### JavaScript Pagination Loop

This dependency-free example scans a fixed eSpace block range. It persists only after the application has successfully processed a page; a production consumer should store the cursor and its unchanged request parameters atomically with its own progress.

```js
const endpoint = "http://127.0.0.1:8545";
const limit = 500;
const filter = {
  fromBlock: "0x100000",
  toBlock: "0x200000",
  address: "0x1111111111111111111111111111111111111111",
  topic0: "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
};

let cursor;

for (;;) {
  const request = { filter, limit };
  if (cursor) request.cursor = cursor;

  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      jsonrpc: "2.0",
      method: "eth_scanLogs",
      params: [request],
      id: 1,
    }),
  }).then((r) => r.json());

  if (response.error) throw new Error(response.error.message);

  const page = response.result;
  await processLogs(page.logs);

  if (page.logs.length < limit) break;
  cursor = page.nextCursor;
}
```

## Reorg-Aware Pagination

### Why a Pivot Assumption Is Useful

Every individual `scanLogs` request performs internal consistency checks, but ordinary cursor pagination does not create an immutable snapshot across several independent requests. If the canonical chain reorganizes after page 1 and before page 2, a cursor alone cannot prove that the already-processed boundary is still canonical.

The `WithPivotAssumption` methods add that cross-request check:

1. The first request omits both `cursor` and pivot assumption.
2. Confura returns `nextCursor` and a `pivotGuard` for a non-empty page.
3. The next request sends `nextCursor` inside the request object and the previous `pivotGuard` as the second positional parameter.
4. Confura verifies that the guarded epoch/block still has the expected canonical pivot/hash before returning the next page.
5. If it changed, the request fails with the `pivot assumption failed` category instead of silently continuing over a different history.

The Core Space guard shape is:

```json
{
  "epochNumber": "0x1234",
  "pivotBlockHash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}
```

The eSpace guard shape is:

```json
{
  "blockNumber": "0x1234",
  "blockHash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}
```

Both fields are required. Quantity fields use hex encoding.

### First eSpace Page

```json
{
  "jsonrpc": "2.0",
  "method": "eth_scanLogsWithPivotAssumption",
  "params": [
    {
      "filter": {
        "fromBlock": "0x100000",
        "toBlock": "0x200000",
        "address": "0x1111111111111111111111111111111111111111"
      },
      "limit": 100
    }
  ],
  "id": 1
}
```

Assume it returns:

```json
{
  "logs": ["..."],
  "nextCursor": {
    "blockNumber": "0x100120",
    "logIndex": "0x3"
  },
  "pivotGuard": {
    "blockNumber": "0x100120",
    "blockHash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  }
}
```

### eSpace Continuation

```json
{
  "jsonrpc": "2.0",
  "method": "eth_scanLogsWithPivotAssumption",
  "params": [
    {
      "filter": {
        "fromBlock": "0x100000",
        "toBlock": "0x200000",
        "address": "0x1111111111111111111111111111111111111111"
      },
      "limit": 100,
      "cursor": {
        "blockNumber": "0x100120",
        "logIndex": "0x3"
      }
    },
    {
      "blockNumber": "0x100120",
      "blockHash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }
  ],
  "id": 2
}
```

Core Space follows the same two-call pattern. Its continuation uses the Core cursor inside the request and the previous Core guard as the second parameter:

```json
{
  "jsonrpc": "2.0",
  "method": "cfx_scanLogsWithPivotAssumption",
  "params": [
    {
      "filter": {
        "fromEpoch": "0x100000",
        "toEpoch": "0x200000"
      },
      "limit": 100,
      "cursor": {
        "blockNumber": "0x101234",
        "logIndex": "0x2"
      }
    },
    {
      "epochNumber": "0x101000",
      "pivotBlockHash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    }
  ],
  "id": 2
}
```

For forward scans, the output guard normally advances to the new page boundary; pass the newly returned guard to the following request. For reverse scans, the first page fixes the upper guard, and continuations preserve that assumption. In either direction, simply feeding each returned `pivotGuard` into the next call produces the correct behavior.

An empty first page has no log from which to derive a guard, so `pivotGuard` is omitted. An empty continuation echoes the validated input guard. The pivot mechanism detects a canonical mismatch; it does not make a live `latest` range into a permanently frozen snapshot.

### Recovery from a Pivot Failure

Treat `pivot assumption failed` as an explicit signal that previously scanned data may have been reorganized. The correct recovery point depends on the application:

- a derived index can roll back to a saved finalized checkpoint and rescan;
- an idempotent consumer can restart the requested range and upsert by `(blockHash, logIndex)` or another chain-native identity; or
- a latency-tolerant pipeline can scan only finalized ranges and avoid most reorg recovery work.

Do not automatically discard the assumption and continue from the same cursor. That would suppress the warning while retaining progress from a potentially different fork.

## How It Works

### Keyset Pagination

Offset pagination becomes progressively more expensive and is unstable when rows are added or removed ahead of an offset. `scanLogs` instead uses a keyset cursor ordered by `(blockNumber, logIndex)`. Confura queries directly from that exclusive key, applies `limit`, and returns the last emitted key as the next checkpoint.

The indexed store has scan-oriented indexes for universal, address, topic, and address-plus-topic routes. Supplying an address and/or `topic0` lets the database use the corresponding route instead of filtering a broad result after pagination.

### Indexed and Near-Head Data

Confura may serve one request from two sources:

- persisted historical logs from the indexed database; and
- the not-yet-indexed near-head suffix from an upstream full node.

The request range is split at the current index watermark. Forward scans process database data before full-node data; reverse scans process full-node data before database data. The segments do not overlap, and the cursor belongs to only one segment, which prevents duplication at the source boundary.

Full-node reads are performed in bounded windows. If a recognized full-node "too many logs" response is returned, Confura narrows that physical window and retries. This is internal: the client still works only with its chosen page size and cursor.

### Per-Request Canonical Consistency

Confura uses optimistic consistency fences around a page:

- it checks the database reorganization version before and after database-dependent work;
- it reads a full-node checkpoint before and after full-node-dependent work; and
- when both sources are consumed, it verifies that the full node and database agree at their boundary.

If a view changes during the attempt, Confura rebuilds the affected work and retries within the request timeout. A stable disagreement is returned as an `inconsistent canonical views` error rather than merging incompatible results.

These checks substantially reduce mixed-view pages, but they are not a transactional blockchain snapshot. An extremely short-lived A-to-B-to-A change between two checkpoint reads cannot be observed. Prefer finalized ranges for strict historical exports, and use the pivot-assumption methods when continuity between pages matters.

## Errors and Retry Guidance

`scanLogs` attaches stable text categories to semantic errors. The concrete reason follows the category in the JSON-RPC error message.

| Message category | Meaning | Client action |
|:-----------------|:--------|:--------------|
| `scan logs rpc unavailable` | The RPC server was started without the required log-store handler. | Check the endpoint or server database configuration; retrying the same endpoint will not help until configuration changes. |
| `invalid scan logs params` | Invalid range, limit, or missing pivot assumption. JSON decoding errors can also reject malformed request shapes before this category is produced. | Correct the request. Do not retry unchanged. |
| `invalid scan logs cursor` | Cursor is incomplete, outside the normalized request range, or incompatible with the current source segment. | Restore the cursor and exact request parameters saved from the preceding page, or restart the scan. |
| `inconsistent canonical views` | Database and full-node canonical views could not be aligned, or required mapping data was inconsistent. | Retry with backoff. If persistent, wait for indexing/reorg convergence or alert the operator. |
| `pivot assumption failed` | The supplied pivot/hash is no longer canonical or is unavailable. | Apply the application's reorg rollback policy; do not continue from the cursor without reconciliation. |

Other possible failures include:

- a request timeout (the current default is 3 seconds);
- a response exceeding the configured `getLogs` response-byte limit (10 MiB by default), in which case reduce `limit`;
- an upstream full-node or transport error; and
- a `data already pruned` store error when the requested lower bound predates retained indexed coverage. Confura rejects rather than silently clipping old history, because clipping would make an incomplete scan look exhausted.

Use bounded exponential backoff for transient transport, timeout, and consistency errors. Invalid parameter, invalid cursor, unavailable-handler, and pivot-failure conditions require correction or explicit recovery rather than blind retries.

## Usage Notes and Best Practices

1. **Keep the request stable.** Persist the filter, numeric bounds, `reverse`, cursor, and pivot guard together. Changing the query while reusing a cursor changes its meaning.
2. **Use a fixed upper bound for finite jobs.** A tag such as `latest` or `latest_state` is frozen only for one RPC call and can advance between pages. Resolve and store a numeric upper bound before starting an export if the job must terminate over one fixed range.
3. **Prefer finalized data when possible.** Pivot guards detect reorganized progress, but finalized ranges reduce rollback complexity and give the strongest practical consistency.
4. **Treat log processing as idempotent.** Persist progress only after processing the full page. On restart, resend the last committed cursor; because the cursor is exclusive, this resumes at the next log.
5. **Do not use `nextCursor` as `hasMore`.** Stop on a short page or fetch until the next page is empty.
6. **Tune page size by payload, not only count.** Logs with large `data` fields can hit the response-byte limit below the maximum item count. Reduce `limit` when that happens.
7. **Use selective predicates for dense ranges.** A single address and exact `topic0` improve index routing and reduce timeout risk.
8. **Use the correct address format.** Core Space accepts one network-correct base32 address; eSpace accepts one 20-byte hex address.
9. **Keep quantity encodings straight.** Range bounds, cursor fields, and pivot quantities are JSON-RPC hex strings. `limit` is a non-negative JSON integer such as `100`, not `"0x64"`.
10. **Do not combine this with trace-log opt-in assumptions.** `scanLogs` does not use the `includeTraceLogs` endpoint flag to synthesize early internal contract events. Use the documented `cfx_getLogs` flow when querying those trace-derived events.

## Operator Configuration

The methods are part of the public `cfx` and `eth` namespaces, but the matching RPC process must have its database-backed log handler configured. Without it, the method returns `scan logs rpc unavailable`.

Relevant configuration defaults are:

```yaml
requestControl:
  scanLogs:
    # Must be at least 100 and no greater than the Store limit of 10,000.
    maxLimit: 1000
    # Internal physical full-node scan window.
    fullnodeWindowSize: 1000
  resourceLimits:
    maxGetLogsResponseBytes: 10485760
```

`maxLimit` controls the largest client-selected page. The default page remains 100 when `limit` is omitted or zero. `fullnodeWindowSize` controls internal near-head queries and is not the public page size; Core Space epoch ranges are mapped to physical block ranges before these windows are read. `maxGetLogsResponseBytes` is enforced after the complete result object is serialized, so operators should account for `logs`, cursor, and pivot-guard overhead.

## Client Implementation Checklist

- Select the Core Space or eSpace method and use its matching address/range format.
- Set explicit inclusive lower and upper bounds for historical scans.
- Choose a page `limit` no greater than the endpoint's configured maximum.
- Process logs in the returned order.
- Reuse `nextCursor` unchanged and keep `reverse` and the filter unchanged.
- Stop on a short page or an empty page.
- For reorg-aware scans, pass each returned `pivotGuard` as the next call's second parameter.
- Persist application progress atomically and define a rollback policy for pivot failures.
