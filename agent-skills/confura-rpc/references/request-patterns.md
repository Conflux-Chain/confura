# Request Patterns

Use this reference for basic Confura RPC request construction and user-facing conventions.

## Space Selection

| User intent | Space | Methods | Range vocabulary | Address format |
|---|---|---|---|---|
| Native Conflux Core data | Core Space | `cfx_*` | epoch or block, depending on method/filter | Conflux base32, e.g. `cfx:...` |
| EVM-compatible data | eSpace | `eth_*`, `web3_*`, `net_*` | block number or block tags | Hex, e.g. `0x...` |

Ask the user which space they are using when it is not clear. Do not translate Core addresses into eSpace addresses unless the user explicitly asks for a bridge/compatibility workflow.

## Method And Module Checks

- Confura expects JSON-RPC method names in `namespace_method` form, for example `cfx_getStatus`, `eth_getLogs`, `diagnostic_getRateLimitStatus`, or `gasstation_suggestedGasFees`.
- If a method is unavailable, first check space, namespace, and exposed module. Public endpoints often expose fewer modules than self-hosted endpoints.
- For public default module behavior and optional modules, read `modules-and-capabilities.md`.

## Endpoint Handling

- If the user provides an endpoint, use that exact endpoint and preserve its path or query parameters.
- If the user uses an API key path such as `https://example.rpc/{api_key}`, keep the API key path in all follow-up examples, but redact real keys in explanations.
- If a feature requires an endpoint query parameter, append it without dropping existing query parameters. For example, add `includeTraceLogs` as `?includeTraceLogs` or `&includeTraceLogs`.
- Do not present repo examples as a complete live endpoint catalog. If exact public endpoints matter, verify from current official sources or ask the user for the endpoint.

## JSON-RPC Curl Template

Use this shape for examples:

```shell
curl -X POST '<endpoint>' \
  -H 'Content-Type: application/json' \
  -d '{
    "jsonrpc": "2.0",
    "method": "<method>",
    "params": [<params>],
    "id": 1
  }'
```

## Core getLogs Template

Core `cfx_getLogs` accepts one filter shape at a time:

- Epoch range: `fromEpoch`/`toEpoch`.
- Block number range: `fromBlock`/`toBlock`.
- Block hash set: `blockHashes`.

If no range type is provided, Confura defaults to a latest-state epoch range. Do not mix epoch range, block range, and block hashes in one filter.

```json
{
  "fromEpoch": "0x0",
  "toEpoch": "latest_state",
  "address": ["cfx:type.contract:..."],
  "topics": [["0x..."]]
}
```

For consistency-sensitive recent queries, prefer an upper bound such as `latest_finalized` when acceptable.

## eSpace getLogs Template

eSpace `eth_getLogs` accepts either a block range or a single `blockHash`. If neither is provided, Confura defaults to the latest block range. Do not mix `fromBlock`/`toBlock` with `blockHash`.

```json
{
  "fromBlock": "0x0",
  "toBlock": "latest",
  "address": ["0x..."],
  "topics": [["0x..."]]
}
```

## Common Agent Checks

- If the user gets no logs, check space/address format first.
- If a Core query uses `fromBlock`/`toBlock`, confirm the user intended block-number filtering rather than epoch filtering.
- If an eSpace query uses epochs or Conflux base32 addresses, point out the mismatch.
- If the request combines a huge range with common event topics, prepare the user for suggested-range retries.
- If the endpoint returns auth or quota errors, move to `rate-limit-diagnostics.md`.
- If the user is calling a Core-like endpoint that is actually backed by eSpace, read `cfxbridge-compatibility.md` before explaining odd zero values or unsupported epoch tags.

## Source Anchors

- `rpc/log_filter.go`: filter type parsing, defaults, validation, and deduplication.
- `util/rpc/middlewares/anti_injection.go`: method-name validation.
- `rpc/apis.go`: public/default module exposure.
