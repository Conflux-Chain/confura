# RPC And Routing

Use this reference for JSON-RPC behavior, fullnode routing, enhanced log APIs, diagnostics, and CfxBridge.

## Spaces And Servers

- Core Space RPC server: `confura rpc --cfx`, config prefix `rpc`.
- eSpace RPC server: `confura rpc --eth`, config prefix `ethrpc`.
- CfxBridge server: `confura rpc --cfxBridge`, config prefix `rpc.cfxBridge`.
- Core Space modules include `cfx`, `txpool`, `pos`, `trace`, `debug`, `gasstation`, and `diagnostic`.
- eSpace modules include `eth`, `web3`, `net`, `trace`, `parity`, `debug`, `txpool`, `gasstation`, and `diagnostic`.
- Empty `exposedModules` means public APIs are exposed according to server defaults. Include `diagnostic` explicitly when operators need diagnostic RPC methods.

## Fullnode Routing

- Node Manager groups fullnodes by purpose, including HTTP, websocket, log, filter, fullstate, and archive groups for Core Space and eSpace.
- RPC Proxy can call Node Manager through HTTP or gRPC router URLs under `node.router`.
- Load balancers should preserve real IP headers, because routing and rate limiting may use client IP.
- Consistent hash settings live under `node.hashRing`.
- Health monitoring settings live under `node.monitor`.

### Group Selection Contract

Before an RPC method executes, `clientMiddleware` selects a fullnode group:

| Method class | Core group | eSpace group |
|---|---|---|
| Default methods | `cfxhttp` | `ethhttp` |
| `*_getLogs` | `cfxlog` | `ethlogs` |
| Filter methods | `cfxfilter` | `ethfilter` |
| Auth route override | DB route group when configured | DB route group when configured |

If a method seems to hit the wrong upstream, inspect group selection before changing handlers.

### Router Priority

Routing is chained: Redis, Node Manager gRPC, Node Manager HTTP RPC, local fallback from Node Manager, local config router, then per-group failover URL. A higher-priority router can hide lower-priority config changes.

## Storage-Backed RPC Behavior

- Confura can serve high-frequency or expensive methods from cache or indexed storage.
- `getLogs` can use off-chain indexes instead of a fullnode bloom-filter path.
- The newest, not-yet-indexed portion of log queries may still be delegated to a fullnode and is subject to configured split ranges.
- If storage is unavailable or disabled, handlers may fall back to fullnodes depending on the method and configuration.
- Rate limit and diagnostic handlers depend on DB-backed registry initialization; no matching DB store means no rate registry for that space.

## Dynamic getLogs Bounds

Source anchor: `doc/RPC_FEATURES.md`.

For indexed log data, Confura limits actual work rather than enforcing a fixed block/epoch span:

- Result count limit defaults to 10,000 logs.
- Response body limit defaults to 10 MB.
- Indexed log query timeout defaults to 3 seconds.
- Address, topic, and block-hash fan-out have configurable limits under `requestControl.logFilter`.

When a query is too large, errors may include a suggested block or epoch range. Clients should retry up to the suggested end, then continue from the next block or epoch.

### getLogs Split Logic

- Core block-hash filters are converted to block numbers by querying fullnode summaries, deduped, then split into DB and fullnode hashes.
- Core epoch filters are converted into DB block ranges using epoch-block mapping tables; suggestion errors can be converted back to epoch ranges.
- eSpace block-hash filters query the fullnode for the block number before deciding DB vs fullnode.
- Partial ranges are split at the max indexed epoch/block: DB handles indexed history, fullnode handles the tail.
- Reorg version is checked before and after reads; changed versions retry until timeout.

Do not simplify this into a fixed block-range rule; that would undo Confura's main `getLogs` enhancement.

## Internal Contract Event Logs

- Core Space historical internal contract events can be reconstructed from trace data.
- End users enable this path by adding `includeTraceLogs` to the endpoint query.
- Supported contracts and detailed rules live in `doc/INTERNAL_CONTRACT_EVENT_LOGS.md`.
- Mixed queries that combine supported internal contract addresses with normal contract addresses are not supported; issue separate requests.

## Rate Limit Diagnostics

- Rate limits can be enforced by API key or client IP.
- Daily quota uses `rpc_all_daily`.
- Global QPS uses `rpc_all_qps`.
- Method QPS resources follow names such as `cfx_getStatus_qps`, `cfx_getLogs_qps`, or `eth_getLogs_qps`.
- Limited requests return JSON-RPC error code `-32005`.
- `diagnostic_getRateLimitStatus` requires exposing the `diagnostic` module.

Strategy JSON accepted by admin CLI is a map from resource to rule. Rule algorithms are `fixed_window` with `Interval`/`Quota` or `token_bucket` with `Rate`/`Burst`.

## Source Pointers

- Server setup: `rpc/server.go`, `rpc/apis.go`, `cmd/rpc.go`.
- Middleware: `rpc/server_middleware.go`.
- Core API: `rpc/cfx_api.go`, `rpc/cfx_api_filter.go`, `rpc/cfx_api_pubsub.go`.
- eSpace API: `rpc/eth_api.go`, `rpc/eth_api_filter.go`, `rpc/eth_api_pubsub.go`.
- Bridge: `rpc/cfxbridge/`.
- Handlers: `rpc/handler/`.
- Log filter parsing and limits: `store/log_filter.go`.
