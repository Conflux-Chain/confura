# Enhanced RPC Features

Confura behaves like a full-node-compatible JSON-RPC gateway, but several high-traffic methods are backed by indexed storage, trace-derived data, and request-control middleware. This document describes four user-visible differences from a plain full node: dynamic `getLogs` query bounds, cursor-paginated `scanLogs`, early internal contract event logs, and rate limit diagnostics.

## `getLogs` with Dynamic Query Bounds

Full nodes usually protect `cfx_getLogs` and `eth_getLogs` by enforcing a relatively small block or epoch range. Confura serves historical logs from an off-chain index, so it does not need to reject every wide-range query up front.

For indexed log data, Confura accepts large ranges and evaluates whether the query is practical by the actual work it produces:

- Result count: a response can contain up to `10,000` logs.
- Response size: the default response body limit is `10 MB`.
- Query latency: indexed log queries time out after the configured maximum duration, currently `3s` by default.
- Filter complexity: address count, topic count, and block-hash count still have configurable limits to prevent unbounded fan-out.

This means a low-frequency contract can be queried across a very large historical range, even from genesis to the latest indexed block, as long as the matching data set stays small enough. Applications do not need to split such queries by a fixed block window.

When a query is too large, Confura returns an error that includes a suggested range. Internally this is represented as `SuggestedFilterOversizedError`. The message keeps the original reason and appends either a suggested block range or a suggested epoch range, for example:

```text
the result set exceeds the max limit of 10000 logs, please narrow down your filter conditions: a suggested block range is [1000000, 1234567]
```

or:

```text
the query set is too large, please narrow down your filter condition: a suggested epoch range is [90000000, 90001000]
```

Clients should use the suggested end block or epoch as the upper bound for the next request, then continue scanning from the following block or epoch until the desired range is covered.

### Example: Wide Historical Query

```shell
curl -X POST http://127.0.0.1:22537 \
  -H 'Content-Type: application/json' \
  -d '{
    "jsonrpc": "2.0",
    "method": "cfx_getLogs",
    "params": [
      {
        "fromEpoch": "0x0",
        "toEpoch": "latest_state",
        "address": ["cfx:type.contract:..."],
        "topics": [["0x..."]]
      }
    ],
    "id": 1
  }'
```

If the matching logs are sparse, this can complete in one request. If the result set or response body is too large, retry using the suggested range from the error message.

### Full Node Delegation

Confura may still delegate the newest, not-yet-indexed part of a log query to an upstream full node. That delegated portion is checked with configured split ranges, because it is still subject to full-node query constraints. Once data has been indexed, the dynamic result-size and latency controls described above apply.

## Cursor-Paginated `scanLogs`

For large exports and backfills, Confura exposes `cfx_scanLogs` and `eth_scanLogs`. Unlike `getLogs`, these methods return a bounded page and an exclusive `(blockNumber, logIndex)` cursor. The next request sends the cursor back with the same filter and direction, allowing a client to scan a large inclusive epoch or block range without inventing fixed range windows.

The filter supports one optional contract address and one optional `topic0`. Core Space uses `fromEpoch`/`toEpoch`; eSpace uses `fromBlock`/`toBlock`. Results are ordered ascending by default or descending with `reverse: true`. The default page size is 100 logs, and the configured maximum is 1,000 by default.

For workflows that need to detect reorganizations between pages, use `cfx_scanLogsWithPivotAssumption` or `eth_scanLogsWithPivotAssumption`. The first non-empty page returns a `pivotGuard`; pass it as the next call's second argument together with `nextCursor`. Confura rejects the continuation if that guarded pivot or block hash is no longer canonical.

Confura transparently joins indexed historical data with a not-yet-indexed full-node suffix while preserving ordering and cursor exclusivity. It also checks database versions, full-node checkpoints, and the database/full-node boundary so one page is not knowingly assembled from inconsistent canonical views.

For complete request and response schemas, Core Space and eSpace examples, pagination loops, pivot-guard semantics, error handling, implementation details, and operator configuration, see [Paginated Event Log Scanning with `scanLogs`](./PAGINATED_SCAN_LOGS.md).

## Early Internal Contract Event Logs

Some early Conflux 1.0 internal contract calls did not emit standard event logs on-chain. A plain full node cannot return those historical events through `cfx_getLogs`, even though applications often expect to query them like normal contract events.

Confura can reconstruct these events from synchronized trace data and return them through the standard `cfx_getLogs` response format. Existing SDKs and log decoders can keep using `cfx_getLogs`; the only client-side change is using an endpoint with the `includeTraceLogs` query parameter.

```text
https://main.confluxrpc.com/?includeTraceLogs
```

or:

```text
https://main.confluxrpc.com/?includeTraceLogs=true
```

The supported internal contracts are Staking, SponsorWhitelistControl, and AdminControl. Mixed-address queries that combine these supported internal contracts with normal contracts are not supported; issue separate requests instead.

For supported contract addresses, event signatures, query constraints, synthesis rules, and examples, see [Internal Contract Event Logs](./INTERNAL_CONTRACT_EVENT_LOGS.md).

## Rate Limit Status Diagnostics

Confura applies rate limits at two levels:

- Daily total requests, enforced with the `rpc_all_daily` resource.
- QPS, enforced globally with `rpc_all_qps` and per method with resources such as `cfx_getStatus_qps`, `cfx_getLogs_qps`, or `eth_getLogs_qps`.

Strategies can use either `fixed_window` or `token_bucket` rules, and can be resolved by API key or by client IP. If a request is limited, Confura returns JSON-RPC error code `-32005` with either `daily request count exceeded` or `request rate exceeded` in the message.

Clients and operators can inspect the effective rate limit state with:

```text
diagnostic_getRateLimitStatus
```

The `diagnostic` module is not public by default. It must be included in the RPC server's exposed modules before the method can be called.

### Example Request

```shell
curl -X POST http://127.0.0.1:22537/{api_key} \
  -H 'Content-Type: application/json' \
  -d '{
    "jsonrpc": "2.0",
    "method": "diagnostic_getRateLimitStatus",
    "params": [],
    "id": 1
  }'
```

### Example Response Shape

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "strategy": {
      "ID": 1,
      "Name": "vip1",
      "LimitOptions": {
        "rpc_all_daily": {
          "Interval": 86400000000000,
          "Quota": 1000000
        },
        "rpc_all_qps": {
          "Rate": 20,
          "Burst": 40
        }
      }
    },
    "limitType": "by_key",
    "info": {
      "userType": "provisioned_user",
      "clientIp": "203.0.113.10",
      "apiKey": "example-api-key"
    }
  }
}
```

The exact strategy contents depend on the configured rate limit rules and the resolved user type. Guest users are usually identified by `clientIp`, provisioned users by `apiKey`, and Web3Pay users include `web3payInfo`. An empty result means no matching strategy was resolved for the current request context.
