# Implementation Playbooks

Use these recipes for common Confura changes. They are intentionally source-oriented: read the named files before editing.

## Add Or Change A Public RPC Method

1. Determine space and namespace: Core `cfx`/`txpool`/`pos`, eSpace `eth`/`web3`/`net`, or CfxBridge.
2. Inspect API registration in `rpc/apis.go`.
3. Inspect method receiver files: `rpc/cfx_api.go`, `rpc/eth_api.go`, `rpc/*_filter.go`, `rpc/*_pubsub.go`, or `rpc/cfxbridge/*`.
4. If the method uses persisted data, add or update a handler under `rpc/handler/` rather than putting store logic directly in the API surface.
5. If it needs a special fullnode group, update method classification used by `clientMiddleware`.
6. Decide whether the module should be public by default. If not public, document that `exposedModules` must include it.
7. Add or update tests in the nearest package. For bridge conversion, check `rpc/cfxbridge/*_test.go`.
8. Update docs when behavior is public or operationally visible.

Red flags:

- Adding a method only to one space when shared middleware expects both.
- Exposing debug/diagnostic behavior publicly by default.
- Bypassing auth/rate/allowlist middleware by creating a separate server path.

## Change getLogs Behavior

Read first:

- `store/log_filter.go`
- `rpc/handler/cfx_logs.go`
- `rpc/handler/eth_logs.go`
- `rpc/cfx_api_filter.go`
- `rpc/eth_api_filter.go`
- `store/mysql/store_log*.go`

Checklist:

- Preserve DB/fullnode split semantics.
- Preserve reorg-version retry.
- Preserve timeout and oversized-result error behavior.
- Preserve Core epoch-to-block conversion and eSpace block-number semantics.
- Check the delegated fullnode range guard: `MaxLogEpochRange` and `MaxLogBlockRange`.
- Update `doc/RPC_FEATURES.md` for user-visible error, limit, or suggested-range behavior.
- Add tests around single-range, wide-range, oversized, and partial DB/fullnode split cases if feasible.

Operational diagnostic:

- If the bug only occurs near head, inspect fullnode delegation.
- If it only occurs for old Core logs, inspect pruning and archive delegation.
- If suggestions are wrong for Core epoch filters, inspect epoch-block mapping conversion.

## Add Or Change A Config Option

1. Find the owning config struct and viper key:
   - `config/config.go` for global init sequence.
   - `node/config.go` for node groups, monitor, router.
   - `store/mysql/config.go` and `store/store.go` for DB and persistence.
   - `virtualfilter/config.go` for filter TTL and memory bounds.
   - `sync/catchup/config.go` for catch-up and boost.
   - `rpc/handler/*` or `util/rpc/cache` for request-control options.
2. Add a struct field with a default tag when appropriate.
3. Update `config/config.yml` comments and examples.
4. Add environment-variable guidance if operators will use it.
5. Check Docker Compose if the option is required for the default stack.
6. Add tests when parsing, defaults, or validation matter.

Do not add a config key only to `config/config.yml`; runtime code must unmarshal and use it.

## Change Rate Limit, ACL, Or API Key Behavior

Read first:

- `util/rate/`
- `util/acl/`
- `rpc/server_middleware.go`
- `cmd/ratelimit/`
- `cmd/acl/`
- `store/mysql/store_ratelimit.go`
- `store/mysql/store_user.go`

Contracts:

- Limit types are by key or by IP.
- Strategy algorithms are `fixed_window` and `token_bucket`.
- Resource names include `rpc_all_daily`, `rpc_all_qps`, and method QPS names such as `cfx_getLogs_qps`.
- DB-backed registries reload periodically from DB.
- ACL address validation differs by network.
- Diagnostic visibility requires exposing the `diagnostic` module.

When changing schema or stored config shape, include migration/backward-compatibility notes.

## Change Node Routing Or Health Behavior

Read first:

- `node/config.go`
- `node/router.go`
- `node/manager.go`
- `node/manager_monitor.go`
- `node/node_status.go`
- `cmd/noderoute/`

Checklist:

- Preserve group names and prefixes; metrics depend on `cfx`/`eth` group prefixes.
- Check router priority: Redis, gRPC, HTTP RPC, local fallback, configured failover.
- Check client-group selection in RPC middleware.
- Check Node Manager server endpoints and Docker Compose service URLs.
- Update operational docs for `X-Forwarded-For`/`X-Real-IP` if routing or IP hashing changes.

## Change Sync Or Storage Behavior

Read first:

- `cmd/sync.go`
- `cmd/util/data_context.go`
- `sync/`
- `sync/catchup/`
- `store/`
- `store/mysql/`

Checklist:

- Confirm store is enabled before expecting sync to start.
- Confirm persistence types include the data being synced.
- Check pruning and partition behavior for log indexes.
- For Core trace log sync, check `sync/tracelog/` and internal contract log docs.
- For catch-up boost, check memory threshold, task sizing, and persistence flushing.

## Change Virtual Filter Behavior

Read first:

- `cmd/vfilter.go`
- `virtualfilter/config.go`
- `virtualfilter/*_system.go`
- `virtualfilter/*_chain.go`
- `virtualfilter/*_api.go`
- `rpc/*_api_filter.go`

Checklist:

- Core uses epoch terminology; eSpace uses block terminology.
- Service endpoints are `virtualFilters.endpoint` and `ethVirtualFilters.endpoint`.
- RPC proxy client settings live under `virtualFilters.client` and `ethVirtualFilters.client`.
- Matching MySQL store must be enabled.
- TTL and max-full-filter settings are memory and staleness controls, not just user-facing timeouts.

## Update Deployment Or Docker Behavior

1. Check `docker-compose.yml`, `Dockerfile`, `config/config.yml`, and `doc/DEPLOY.md`.
2. Confirm Core/eSpace service pairs are both handled when appropriate.
3. Check environment variable names map to viper paths with `INFURA_`.
4. Keep ports aligned with config defaults.
5. Update Grafana or InfluxDB guidance when metrics labels, dashboards, or datasource requirements change.

## Review Checklist

Before finalizing:

- Did the change touch both Core and eSpace where needed?
- Did the change preserve middleware, routing, and store enablement contracts?
- Did config/docs/examples change with runtime behavior?
- Did tests cover the closest package and the shared contract?
- Did the answer avoid exposing secrets or advising unsafe production operations?
