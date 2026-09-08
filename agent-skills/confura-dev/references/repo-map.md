# Confura Repo Map

Use this map to choose source files before making claims or edits. Prefer reading code near the behavior under discussion instead of relying on this summary alone.

## Primary Documents

- `README.md`: product overview, components, build command, run commands, contribution notes.
- `doc/ARCHITECTURE.md`: high-level system architecture and request flow.
- `doc/DEPLOY.md`: prerequisites, config loading, environment overrides, Docker quick start, load balancer and Grafana setup.
- `doc/RPC_FEATURES.md`: dynamic `getLogs` bounds, trace-derived internal contract logs, rate limit diagnostics.
- `doc/INTERNAL_CONTRACT_EVENT_LOGS.md`: supported internal contracts, event reconstruction rules, examples.
- `grafana/README.md`: dashboard inventory and datasource naming requirement.

## Source Packages

- `cmd/`: Cobra entrypoints and operational tools.
  - `cmd/root.go`: aggregate root flags and global boot path.
  - `cmd/rpc.go`: `rpc --cfx`, `rpc --eth`, `rpc --cfxBridge`.
  - `cmd/sync.go`: `sync --db`, `sync --eth`, `sync --trace`.
  - `cmd/nm.go`: node management servers.
  - `cmd/vfilter.go`: virtual filter servers.
  - `cmd/ratelimit`, `cmd/acl`, `cmd/noderoute`: database-backed admin tools.
  - `cmd/test`: validation tools comparing Confura with full nodes.
- `config/`: startup initialization and version metadata. `config.Init()` wires viper, metrics/logging utilities, store, node, and RPC defaults. Environment variables use the `INFURA_` prefix.
- `rpc/`: JSON-RPC API implementations, server setup, middleware, filters, pub/sub, diagnostics, CfxBridge, trace/debug/gasstation modules.
- `rpc/handler/`: handlers that serve RPC data from DB, fullnodes, virtual filter, gas station, or transaction relay.
- `node/`: fullnode client providers, router factories, node manager, health monitoring, route server, route proto.
- `store/`: storage abstractions and chain data models.
- `store/mysql/`: MySQL implementation, migrations, log indexes, rate limit/user/ACL/node route tables, partitioning and pruning.
- `store/redis/`: Redis-backed cache/store helpers.
- `sync/`: Core Space, eSpace, trace log, catch-up, pivot window, and election logic.
- `virtualfilter/`: filter polling, in-memory chains, service systems, and Core/eSpace APIs.
- `util/`: shared helpers, cache, metrics, ACL, rate limit, relay, RPC utilities, math/map/LRU helpers.
- `types/`: cross-package data types such as gas station and ranges.
- `test/`: runtime validators used by `confura test`.
- `grafana/`: Grafana dashboard JSON models.

## Common Change Targets

- Add or change a public RPC method: inspect `rpc/apis.go`, the relevant `rpc/*_api.go`, `rpc/server.go`, and `rpc/handler/*` if it touches persisted data.
- Change `getLogs` behavior: inspect `rpc/*_api_filter.go`, `rpc/handler/*logs*.go`, `store/log_filter.go`, `store/mysql/store_log*.go`, and `doc/RPC_FEATURES.md`.
- Change rate limiting: inspect `util/rate`, `rpc/server_middleware.go`, `cmd/ratelimit`, `store/mysql/store_ratelimit.go`, and diagnostic RPC handlers.
- Change node routing or health: inspect `node/factory.go`, `node/router.go`, `node/manager.go`, `node/manager_monitor.go`, `node/node_status.go`, `cmd/noderoute`.
- Change sync performance or correctness: inspect `sync/`, `sync/catchup/`, `store/mysql/`, and relevant validators in `cmd/test`.
- Change deployment defaults: inspect `config/config.yml`, `docker-compose.yml`, `Dockerfile`, `doc/DEPLOY.md`, and `grafana/README.md`.

## Task-To-Code Fast Paths

- "RPC returns wrong data": start at the API method, then handler, then store/fullnode split, then middleware context.
- "RPC is rate limited unexpectedly": start at `rpc/server_middleware.go`, then `util/rate/registry*.go`, then DB key/strategy loaders in `store/mysql`.
- "RPC hits the wrong fullnode": start at `clientMiddleware`, then `node/router.go`, then node manager state and DB node routes.
- "Docker stack boots but method falls back to fullnode": check whether matching MySQL store and persistence types are enabled.
- "Virtual filter does nothing": check `cmd/vfilter.go`, matching DB store, `virtualFilters.client.enabled`, and filter node groups.
- "Sync starts but no data appears": check store enablement, persistence types, fullnode client config, and sync from-height/from-epoch.
- "Diagnostic method missing": check `rpc/apis.go` public flag and `exposedModules`; empty module list does not expose private modules.

## Files That Encode Hidden Product Rules

- `rpc/server_middleware.go`: middleware order, auth/rate/ACL context, client group selection.
- `cmd/util/data_context.go`: store enablement and sync client creation.
- `node/router.go`: router priority and failover behavior.
- `rpc/apis.go`: public/private module exposure policy.
- `store/log_filter.go`: log limits, bound-check toggles, suggested-range error types.
- `rpc/handler/cfx_logs.go` and `rpc/handler/eth_logs.go`: DB/fullnode split, reorg guard, oversized response behavior.
- `store/store.go`: persistence type defaults and supported data types.
- `util/rate/strategy.go`: accepted rate limit JSON shape.

## Generated or Special Files

- `node/router/proto/*.pb.go` are generated from `node/router/proto/router.proto`. Do not hand-edit generated files unless intentionally updating generated output.
- Backup-like files such as `*.bak` and `*.bk` may exist in a dirty worktree. Do not assume they are canonical without user confirmation.
