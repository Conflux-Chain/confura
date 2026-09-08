# System Invariants

Read this before non-trivial code changes. These are source-level contracts that are easy for agents to miss because they are spread across packages rather than stated in README files.

## Space Separation Is Structural

- Core Space and eSpace paths are parallel but not interchangeable.
- CLI flags use `--cfx`/`--db` for Core Space and `--eth` for eSpace.
- Config roots are paired: `rpc` vs `ethrpc`, `store` vs `ethstore`, `virtualFilters` vs `ethVirtualFilters`, `cfx` vs `eth`.
- Stores are paired: `mysql.CfxStore` and `mysql.EthStore`; common admin stores are reached through `StoreContext.GetCommonStore("cfx"|"eth")`.
- Address validation differs: Core Space allowlists use Conflux base32 addresses; eSpace allowlists use hex addresses.

When editing shared logic, explicitly check both spaces and avoid assuming Ethereum block-number semantics apply to Core epoch filters.

## Store Enablement Controls Component Capability

`cmd/util/data_context.go` creates DB stores only when `store.mysql.enabled` or `ethstore.mysql.enabled` is true. This has cascading effects:

- Sync only has clients for spaces with enabled stores.
- RPC gets storage-backed handlers only when the matching DB store exists.
- Rate limit registries are initialized from DB only when the matching DB store exists.
- Virtual filter server constructors expect the matching DB virtual filter log store; a missing DB store can be fatal.
- Admin CLIs for rate limit, ACL, and node route return "DB store is unavailable" when store config is disabled.

Deployment guidance must therefore pair service commands with the matching store configuration.

## RPC Middleware Order Matters

`rpc/server_middleware.go` registers static middlewares in a fixed order:

1. recover
2. anti-injection
3. auth
4. allowlists
5. daily and QPS rate limits
6. metrics
7. logging
8. client provider selection
9. uniform error conversion
10. missing-ID prevention

Comments say missing-ID prevention should be checked first, but it is hooked after other middlewares because of the underlying hook execution semantics. Do not reorder this casually. A middleware change can affect auth context, rate limit keys, metrics labels, and selected fullnode clients.

## Client Group Selection Is Method-Sensitive

`clientMiddleware` selects a fullnode group before API handlers run:

- Core default: `cfxhttp`.
- Core `cfx_getLogs`: `cfxlog`.
- Core filter APIs: `cfxfilter`.
- eSpace default: `ethhttp`.
- eSpace `eth_getLogs`: `ethlogs`.
- eSpace filter APIs: `ethfilter`.
- For other methods, authenticated route groups can override the default through DB-backed route config.

If a new method needs a special fullnode group, update method classification near the middleware, not just the API handler.

## Router Chain Priority

`node.MustNewRouter` chains routers in this priority:

1. Redis router when `node.router.redisUrl` is set.
2. Node Manager gRPC router when `node.router.nodeRpcUrlProto` or `ethNodeRpcUrlProto` is set.
3. Node Manager HTTP RPC router when `node.router.nodeRpcUrl` or `ethNodeRpcUrl` is set.
4. Local router loaded from Node Manager RPC as fallback when HTTP RPC is configured.
5. Pure local router from configured node URLs when no remote router is configured.
6. Per-group chained failover URL if no router returns a node.

Routing bugs often come from a higher-priority router returning stale data, not from the final configured URL list.

## API Exposure Defaults Are Public-Only

`rpc/apis.go` exposes only `Public: true` APIs when `exposedModules` is empty. Non-public modules such as `trace`, `debug`, `diagnostic`, metrics service, some eSpace modules, and gasstation require explicit exposure. Do not tell operators that `diagnostic_getRateLimitStatus` works unless `diagnostic` is exposed.

## getLogs Correctness Contracts

Storage-backed `getLogs` is guarded by several contracts:

- Indexed DB ranges are split from not-yet-indexed fullnode ranges.
- Core epoch filters are converted to block-number ranges using epoch-block mapping tables.
- Reorg version is checked before and after storage/fullnode reads; changed versions trigger retry until timeout.
- Indexed log queries are timeout-bound by `store.TimeoutGetLogs` when bound checks are required.
- Result count limit is `store.MaxLogLimit` and response body size comes from `requestControl.resourceLimits`.
- Oversized queries should return suggested block or epoch ranges when possible.
- Bound checks are disabled for filters that cannot be narrowed meaningfully, such as a single block/epoch or single block hash.
- Core pruned logs can optionally delegate to archive fullnodes through `CfxPrunedLogsHandler`; eSpace pruned-log handling is not implemented in the same way.

Any `getLogs` change should check Core and eSpace handlers, store filter parsing, docs, and tests or validators.

## Persistence Types Affect RPC Behavior

`store.persistence.types` and `ethstore.persistence.types` default to `[log]`. Supported types are `block`, `transaction`, `receipt`, and `log`. If operators expect storage-backed block/receipt/transaction behavior, the matching type must be enabled and sync must be running for that space.

## Generated And External Boundaries

- Do not hand-edit `node/router/proto/*.pb.go` without updating `node/router/proto/router.proto` and regenerating.
- Treat Grafana JSON as deployable artifacts; update `grafana/README.md` when dashboard inventory changes.
- Treat config examples and docs as user-facing API. Update `doc/DEPLOY.md`, `doc/RPC_FEATURES.md`, or `config/config.yml` when behavior or defaults change.

## Operational Safety

- DB writes from admin CLIs often pause for Enter confirmation. Mention this in operator instructions.
- Debug endpoints, diagnostic modules, DSNs, API keys, billing keys, alert webhooks, and private node URLs are sensitive.
- Avoid advising destructive DB pruning, partition drops, or container resets without an explicit target and backup plan.
