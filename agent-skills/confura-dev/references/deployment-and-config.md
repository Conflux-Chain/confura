# Deployment And Configuration

Use this reference for self-hosted deployments, environment variables, Docker Compose, ports, and config tuning.

## Prerequisites

Source anchor: `doc/DEPLOY.md`.

- Minimum hardware: 2+ CPU cores, 4 GB RAM, 200 GB storage with pruning or 1 TB otherwise, 8 MBit/sec download.
- Recommended hardware: 4+ CPU cores, 8 GB+ RAM, high-performance SSD with at least 1 TB free, 25+ MBit/sec download.
- Optional dependencies depend on selected features:
  - MySQL 5.7+ for persistent off-chain storage.
  - Redis 6.2+ for optional cache/throttling.
  - InfluxDB 1.x for metrics storage.
  - Grafana 8.0.x+ for dashboards. The Docker Compose file currently uses Grafana 11.2.2.

## Config Loading

- Confura loads `config.yml` or `config/config.yml` from the current directory at startup.
- `config/config.yml` is the template with comments and defaults.
- Environment variables prefixed with `INFURA_` override config values.
- Config paths map to env vars by replacing path separators with underscores, for example `rpc.endpoint` becomes `INFURA_RPC_ENDPOINT`.
- Redact DSNs, API keys, billing keys, webhook URLs, and private fullnode URLs in any user-facing output.

## Important Config Areas

- `rpc`: Core Space RPC endpoint, websocket/debug endpoints, exposed modules, bridge settings, throttling.
- `ethrpc`: eSpace RPC endpoint, websocket/debug endpoints, exposed modules, revalidation.
- `cfx` and `eth`: SDK fullnode client endpoints, retry, timeout, connection, circuit breaker.
- `sync`: Core/eSpace sync starts, batch sizes, trace log sync, catch-up sync, boost mode, HA election.
- `store` and `ethstore`: MySQL, Redis, persistence data types, partition settings.
- `node`: fullnode groups, health monitor, node manager endpoints, router URLs, failover.
- `virtualFilters` and `ethVirtualFilters`: virtual filter service endpoints, TTL, memory bounding, client service URL.
- `requestControl`: log filter fan-out limits, fullnode split ranges, response size limit, eSpace cache settings.
- `metrics`: InfluxDB host, DB, namespace, report interval.
- `alert`: notification channels and custom tags.
- `web3pay`: billing or VIP subscription middleware.

## Configuration Dependency Matrix

| Goal | Required config |
|---|---|
| Core storage-backed `getLogs` | `store.mysql.enabled=true`, `store.persistence.types` includes `log`, Core sync running |
| eSpace storage-backed `eth_getLogs` | `ethstore.mysql.enabled=true`, `ethstore.persistence.types` includes `log`, eSpace sync running |
| Core RPC uses Node Manager | `INFURA_NODE_ROUTER_NODERPCURL` or `node.router.nodeRpcUrl`, plus Node Manager service |
| eSpace RPC uses Node Manager | `INFURA_NODE_ROUTER_ETHNODERPCURL` or `node.router.ethNodeRpcUrl`, plus eSpace Node Manager service |
| Core virtual filter through RPC | Core virtual filter service, `virtualFilters.client.enabled=true`, `virtualFilters.client.serviceRpcUrl` |
| eSpace virtual filter through RPC | eSpace virtual filter service, `ethVirtualFilters.client.enabled=true`, `ethVirtualFilters.client.serviceRpcUrl` |
| Rate limit by DB strategy/key | Matching MySQL store enabled, strategy/key records configured, RPC service restarted or registry reload interval elapsed |
| `diagnostic_getRateLimitStatus` | Matching rate registry plus `diagnostic` in exposed modules |
| Grafana dashboards show data | `metrics.enabled=true`, InfluxDB reachable, datasource name starts with `confura_*` |
| Core pruned log archive fallback | Core DB store plus `rpc.throttling.redisUrl` and archive fullnode support |

If a feature does not work, check this matrix before changing code.

## Docker Compose Services

Source anchor: `docker-compose.yml`.

- `node-management`: Core Space node manager, command `nm --cfx`, port `22530`.
- `ethnode-management`: eSpace node manager, command `nm --eth`, port `28530`.
- `chain-sync`: Core Space sync, command `sync --db`.
- `ethchain-sync`: eSpace sync, command `sync --eth`.
- `virtual-filter`: Core Space virtual filter, command `vf --cfx`, port `42537`.
- `ethvirtual-filter`: eSpace virtual filter, command `vf --eth`, port `48545`.
- `rpc-proxy`: Core Space RPC proxy, command `rpc --cfx`, port `22537`.
- `ethrpc-proxy`: eSpace RPC proxy, command `rpc --eth`, port `28545`.
- `cfxbridge`: CfxBridge RPC proxy, command `rpc --cfxBridge`, port `32537`.
- `db`: MySQL 8.
- `influxdb`: InfluxDB 1.8.
- `grafana`: Grafana.

Compose service names are also dependency URLs. For example, Core RPC uses `http://node-management:22530` and `http://virtual-filter:42537` in the default Compose stack. A port published to the host is not the same as the URL used between containers.

## Docker Quick Start

Use `docker-compose build`, `docker-compose up -d`, and `docker-compose ps` for the documented local stack. If the environment uses Compose v2, `docker compose` may be equivalent, but prefer the command already used by the repo docs unless the user says otherwise.

## Load Balancer

- Preserve real IP with `X-Forwarded-For` or `X-Real-IP`.
- Use IP-hash routing to keep client-to-RPC proxy routing consistent.
- Avoid exposing debug endpoints publicly.

## Grafana

- Dashboard JSON files live in `grafana/`.
- Datasource names must start with `confura_*` for dashboard integration.
- Dashboards cover RPC RED, RPC drilldown, input breakdown, batch, Pub/Sub, fullnode proxy, store ops, node management, sync RED, sync fullnode query, sync store ops, and virtual filter operations.
