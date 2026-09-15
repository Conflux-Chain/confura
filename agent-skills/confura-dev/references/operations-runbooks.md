# Operations Runbooks

Use these flows for self-hosted troubleshooting. Confirm environment, network, and space before suggesting changes.

## RPC Proxy Is Not Responding

1. Identify server: Core `rpc --cfx`, eSpace `rpc --eth`, or bridge `rpc --cfxBridge`.
2. Check configured endpoint: `rpc.endpoint`, `ethrpc.endpoint`, or `rpc.cfxBridge.endpoint`.
3. Check container/process status and logs.
4. Confirm upstream fullnode endpoints under `cfx.http`, `eth.http`, or bridge node config.
5. If node routing is enabled, verify `INFURA_NODE_ROUTER_*` service URL and Node Manager status.
6. If storage-backed methods fail, verify MySQL settings and migrations.

Source path after basic checks: `cmd/rpc.go` for service boot, `rpc/server.go` for server creation, `rpc/server_middleware.go` for request context.

## getLogs Query Is Slow Or Oversized

1. Determine Core Space `cfx_getLogs` vs eSpace `eth_getLogs`.
2. Check whether data is indexed in MySQL and whether the query reaches not-yet-indexed ranges.
3. Inspect limits under `requestControl.logFilter` and `requestControl.resourceLimits`.
4. If Confura returns a suggested range, advise scanning that range first and continuing from the next block/epoch.
5. For archive/pruned Core Space logs, check `rpc.throttling.redisUrl` and archive fullnode availability.
6. For internal contract historical logs, confirm trace sync and use `includeTraceLogs`.

Source path after config checks: `store/log_filter.go` for limits, `rpc/handler/cfx_logs.go` or `rpc/handler/eth_logs.go` for split and suggestion behavior, `store/mysql/store_log*.go` for index performance.

Interpretation shortcuts:

- Error mentions suggested range: client should page by suggested range, not switch to a fixed tiny window automatically.
- Only newest range is slow: likely fullnode delegation.
- Only historical Core internal contracts are missing: likely trace log sync or `includeTraceLogs`.
- Error says stale/pruned: check archive fallback, pruning settings, and archive fullnode availability.

## Sync Is Falling Behind

1. Identify Core Space `sync --db`, eSpace `sync --eth`, or trace sync `sync --trace`.
2. Check fullnode latency and reliability in `cfx.http`, `eth.http`, and catch-up node pools.
3. Inspect `sync.cfx.maxEpochs`, `sync.eth.maxBlocks`, catch-up settings, and boost mode settings.
4. Check MySQL latency, partitions, max connections, and storage growth.
5. Use Grafana sync RED, fullnode query, and store ops dashboards when metrics are enabled.

Source path after metrics checks: `cmd/sync.go`, `sync/catchup/`, `sync/pivot_window.go`, and matching MySQL stores. Check `store.persistence.types`; syncing logs only will not populate block/receipt/transaction data.

## Node Manager Marks Nodes Unhealthy

1. Check node group: Core HTTP/fullstate/ws/log/filter/archive or eSpace equivalent.
2. Inspect `node.monitor.unhealth` thresholds for failures, lag, latency percentile, and max latency.
3. Verify each fullnode endpoint directly from the same network as Node Manager.
4. Confirm Node Manager endpoint exposure and router URLs used by RPC Proxy.
5. Use the Node Management Grafana dashboard if metrics are enabled.

Source path after endpoint checks: `node/manager_monitor.go`, `node/node_status.go`, `node/config.go`, and `node/router.go`.

## Virtual Filter Issues

1. Identify Core `vf --cfx` or eSpace `vf --eth`.
2. Check service endpoint: `virtualFilters.endpoint` or `ethVirtualFilters.endpoint`.
3. Check RPC Proxy client config: `virtualFilters.client.enabled/serviceRpcUrl` or eSpace equivalent.
4. Verify MySQL store is enabled for the matching space.
5. Inspect TTL and max full filter block/epoch settings for memory pressure or stale filters.
6. Use `confura test vf` for runtime validation against a fullnode.

Source path after service checks: `cmd/vfilter.go`, `virtualfilter/config.go`, `virtualfilter/cfx_system.go`, `virtualfilter/eth_system.go`, and matching `*_chain.go` tests.

## Rate Limit Or API Key Issues

1. Confirm whether the request is limited by API key or IP.
2. Check strategies with `confura ratelimit lss --network cfx|eth`.
3. Check keys with `confura ratelimit lsk --network cfx|eth --strategy <name>`.
4. Confirm `diagnostic` module exposure before suggesting `diagnostic_getRateLimitStatus`.
5. Remember runtime registries reload DB rate limit settings periodically.

## Metrics Or Grafana Missing Data

1. Confirm `metrics.enabled=true`.
2. Confirm InfluxDB host, DB, credentials, and network reachability.
3. Check `INFURA_METRICS_INFLUXDB_HOST` and `INFURA_METRICS_INFLUXDB_DB` in Docker Compose.
4. Confirm Grafana datasource name starts with `confura_*`.
5. Import the relevant dashboard JSON from `grafana/`.

## Production Safety

- Do not suggest deleting databases, dropping partitions, or resetting containers without explicit confirmation.
- Take backups before schema, partition, or prune changes.
- Treat debug endpoints and diagnostic modules as sensitive. Expose them only to trusted networks.
