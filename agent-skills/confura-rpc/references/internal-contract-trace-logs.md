# Core Internal Contract Synthetic Logs

Use this reference when users ask how to query early Core Space internal contract events through Confura.

## What The Feature Does

Early Conflux 1.0 internal contract calls did not emit normal event logs on-chain. Confura can reconstruct supported internal contract events from synchronized trace data and return them through the standard `cfx_getLogs` response format.

No request-body changes are required. Users enable the feature by adding `includeTraceLogs` to the RPC endpoint URL.

```text
https://main.confluxrpc.com/?includeTraceLogs
```

or:

```text
https://main.confluxrpc.com/?includeTraceLogs=true
```

For user-provided endpoints, append `includeTraceLogs` without dropping existing path, API key, or query parameters.

## Supported Contracts And Events

### Staking

Mainnet address: `cfx:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaajrwuc9jnb`  
Testnet address: `cfxtest:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaajh3dw3ctn`

| Event | Signature |
|---|---|
| `Deposit` | `Deposit(address,uint256)` |
| `Withdraw` | `Withdraw(address,uint256)` |
| `VoteLocked` | `VoteLocked(address,uint256,uint256)` |

### SponsorWhitelistControl

Mainnet address: `cfx:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaaegg2r16ar`  
Testnet address: `cfxtest:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaaeprn7v0eh`

| Event | Signature |
|---|---|
| `SponsorGas` | `SponsorGas(address,address,uint256)` |
| `SponsorCollateral` | `SponsorCollateral(address,address)` |
| `WhitelistAddedByAdmin` | `WhitelistAddedByAdmin(address,address,address[])` |
| `WhitelistRemovedByAdmin` | `WhitelistRemovedByAdmin(address,address,address[])` |
| `WhitelistAdded` | `WhitelistAdded(address,address[])` |
| `WhitelistRemoved` | `WhitelistRemoved(address,address[])` |

### AdminControl

Mainnet address: `cfx:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2mhjju8k`  
Testnet address: `cfxtest:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaaawby2s44d`

| Event | Signature |
|---|---|
| `AdminChanged` | `AdminChanged(address,address,address)` |
| `ContractDestroyed` | `ContractDestroyed(address,address)` |

## Query Rules

- Method is still `cfx_getLogs`.
- Only Core Space is supported; this is not an eSpace `eth_getLogs` feature.
- `address` must contain only supported internal contract addresses for synthetic log lookup.
- If `address` is omitted, or contains no supported internal contracts, Confura falls back to normal `cfx_getLogs`.
- Mixed supported internal contract addresses plus regular contract addresses are invalid; split into separate requests.
- Topic filtering works with standard event `topic0` hashes.
- If every `topic0` in the first topic dimension is unsupported, Confura returns no synthetic logs rather than scanning unrelated trace events.
- Returned logs use normal `cfx_getLogs` response shape, but `logIndex` is synthetic and not comparable with normal contract log indices.
- The same `getLogs` result-limit, response-size, timeout, and suggested-range behavior can apply to synthetic logs.

## Event Synthesis Rules

Confura generates a synthetic event only when all are true:

- The transaction succeeded.
- The internal contract call succeeded.
- Every ancestor call in the call stack succeeded.

If a parent call reverts after an internal contract call succeeds, the synthetic event is not generated.

## Example: Staking Deposit

`topic0` is `keccak256("Deposit(address,uint256)")`:

```text
0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c
```

```shell
curl -X POST 'https://main.confluxrpc.com/?includeTraceLogs' \
  -H 'Content-Type: application/json' \
  -d '{
    "jsonrpc": "2.0",
    "method": "cfx_getLogs",
    "params": [
      {
        "fromEpoch": "0x6B9A02F",
        "toEpoch": "0x6B9A03A",
        "address": ["cfx:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaajrwuc9jnb"],
        "topics": [
          ["0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"]
        ]
      }
    ],
    "id": 1
  }'
```

## Agent Response Patterns

When a user asks why they get no early internal contract logs:

1. Check that the endpoint includes `includeTraceLogs`.
2. Check that the method is `cfx_getLogs`, not `eth_getLogs`.
3. Check that every address is one of the supported internal contract addresses.
4. Check the event `topic0`.
5. Check whether the query range covers the historical call.
6. Explain that reverted parent calls do not produce synthetic logs.

When a user asks for normal logs and internal logs together, recommend two requests and merge client-side if needed.

## Source Anchors

- `doc/GETLOGS.md`: user-facing feature description and event matrix.
- `rpc/cfx_api.go`: `includeTraceLogs` query parameter and mixed-address guard.
- `rpc/handler/cfx_internal_contract_logs.go`: internal contract log filtering, timeout, and bound checks.
- `sync/tracelog/event.go`: supported internal contract event signatures and topic hashes.
