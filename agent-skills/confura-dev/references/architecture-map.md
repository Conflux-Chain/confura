# Architecture Map

Source anchor: `doc/ARCHITECTURE.md`. Use code for exact implementation details.

## Components

- Load Balancer: routes user requests to RPC Proxy and passes real client IP through `X-Forwarded-For` or `X-Real-IP`.
- RPC Proxy: exposes JSON-RPC, tries optimized storage-backed paths, forwards to fullnodes when needed, and delegates filter APIs to Virtual Filter when configured.
- Node Manager: manages fullnode groups, monitors health, and provides routing decisions to RPC Proxy.
- Virtual Filter: polls fullnodes for filter changes, stores changes, and serves filter API requests.
- Sync: indexes blockchain data from fullnodes into persistent storage for optimized RPC responses.
- Store: MySQL and optional Redis backends for chain data, log indexes, rate limits, users, routes, caches, and partitions.
- Metrics stack: InfluxDB plus Grafana dashboards for RED metrics and component-specific views.

## Request Flow

1. Load balancer receives JSON-RPC traffic and preserves the original client IP.
2. RPC Proxy determines the module/method and selects an optimized path.
3. RPC Proxy asks Node Manager for a fullnode route when the request needs upstream execution.
4. Storage-backed RPC methods read from MySQL/Redis when data is indexed and current enough.
5. Filter APIs may go to Virtual Filter, which polls and tracks filter changes separately from fullnodes.
6. Fullnodes serve data that is not indexed, not cacheable, or requires canonical node execution.

## Data Flow

- Sync continuously pulls Core Space/eSpace data from fullnodes and persists configured data types.
- Trace log sync can reconstruct historical internal contract logs for Core Space.
- Store partitioning and pruning keep log tables bounded according to configured partition policies.
- Node Manager monitors fullnode health and removes or recovers nodes according to configured thresholds.
- Metrics report request rate, error, duration, store ops, sync ops, node status, and virtual filter operations.

## Design Questions to Resolve Early

- Which space is involved: Core Space (`cfx`) or eSpace (`eth`)?
- Which component owns the behavior: RPC Proxy, Node Manager, Sync, Virtual Filter, Store, or admin CLI?
- Is the request served from storage, a fullnode, virtual filter, cache, or bridge conversion layer?
- Does correctness depend on latest indexed height, finality, reorg behavior, or fullnode fallback?
- Does the change affect public JSON-RPC compatibility or only self-hosted deployment behavior?
