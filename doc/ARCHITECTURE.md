
# System Architecture

![System Architecture](./sa.svg)

## System Components

- Load Balancer: Routes user requests to the RPC Proxy, passing the user’s real IP via the `x-real-ip` or `x-forwarded-for` HTTP headers. 
- RPC Proxy: Attempts to fulfill requests using off-chain storage first; if needed, routes to the appropriate Full Node or Virtual Filter.
- Node Manager: Manages Full Node routing to the RPC Proxy and monitors node cluster health. 
- Virtual Filter: Handles filter API requests by polling Full Nodes, storing changes, and responding with the latest data.
- Sync: Continuously indexes blockchain data from Full Nodes into storage for efficient retrieval and responsiveness.

## Workflow

1. Request Handling:
- The Load Balancer receives the user request and routes it to the appropriate RPC Proxy based on IP hash.

2. Request Processing:
- The RPC Proxy consults the Node Manager to assign the appropriate Full Node based on the request type and real IP.
- It then assesses the request type to determine routing:
    - Attempts to fetch data from storage for requests that might be persisted; if unavailable, forwards to the Full Node.
    - Directs filter API requests to the Virtual Filter.
    - Sends other requests to the Full Node for direct processing.

3. Processing by Full Nodes:
- Full Nodes process requests when data is not available in storage or when direct processing is required.

4. Continuous Monitoring and Indexing:
- The Node Manager continuously monitors Full Node health, removing unhealthy nodes to maintain a stable and reliable cluster.
- Sync persistently indexes blockchain data into storage, ensuring optimal performance for future queries.
- The Virtual Filter regularly polls Full Nodes for filter updates, storing changes to enable quick and efficient data retrieval.