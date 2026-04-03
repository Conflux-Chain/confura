# Early Internal Contract Event Logs Support

## Overview

Due to historical limitations of the Conflux 1.0 chain, three early internal contracts (Staking, SponsorWhitelistControl, and AdminControl) did not emit standard Event Logs during execution. As a result, these events could not be queried via the standard `cfx_getLogs` RPC interface.

Confura addresses this by synchronizing on-chain Trace data and assembling synthetic event logs for internal contracts. Users can query these events through the standard `cfx_getLogs` method by using a dedicated RPC endpoint — **no changes to the request body or existing SDK integration are required**.

---

## Supported Internal Contracts and Events

### Staking Contract

**Contract Address**: 

- Mainet: https://confluxscan.org/address/cfx:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaajrwuc9jnb
- Testnet: https://testnet.confluxscan.org/address/cfxtest:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaajh3dw3ctn

| Event | Signature | Parameters |
|:------|:----------|:-----------|
| `Deposit` | `Deposit(address,uint256)` | `user` (indexed): caller address; `amount`: deposited amount |
| `Withdraw` | `Withdraw(address,uint256)` | `user` (indexed): caller address; `amount`: withdrawn amount |
| `VoteLocked` | `VoteLocked(address,uint256,uint256)` | `user` (indexed): caller address; `amount`: locked amount; `unlockBlockNumber`: unlock block number |

### SponsorWhitelistControl Contract

**Contract Address**: 

- Mainet: https://confluxscan.org/address/cfx:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaaegg2r16ar
- Testnet: https://testnet.confluxscan.org/address/cfxtest:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaaeprn7v0eh

| Event | Signature | Parameters |
|:------|:----------|:-----------|
| `SponsorGas` | `SponsorGas(address,address,uint256)` | `sponsor` (indexed): sponsor address; `contractAddr` (indexed): sponsored contract address; `upperBound`: gas fee upper bound |
| `SponsorCollateral` | `SponsorCollateral(address,address)` | `sponsor` (indexed): sponsor address; `contractAddr` (indexed): sponsored contract address |
| `WhitelistAddedByAdmin` | `WhitelistAddedByAdmin(address,address,address[])` | `admin` (indexed): admin address; `contractAddr` (indexed): contract address; `users`: list of added addresses |
| `WhitelistRemovedByAdmin` | `WhitelistRemovedByAdmin(address,address,address[])` | `admin` (indexed): admin address; `contractAddr` (indexed): contract address; `users`: list of removed addresses |
| `WhitelistAdded` | `WhitelistAdded(address,address[])` | `sponsor` (indexed): caller address; `users`: list of added addresses |
| `WhitelistRemoved` | `WhitelistRemoved(address,address[])` | `sponsor` (indexed): caller address; `users`: list of removed addresses |

### AdminControl Contract

**Contract Address**: 

- Mainet: https://confluxscan.org/address/cfx:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2mhjju8k
- Testnet: https://testnet.confluxscan.org/address/cfxtest:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaaawby2s44d

| Event | Signature | Parameters |
|:------|:----------|:-----------|
| `AdminChanged` | `AdminChanged(address,address,address)` | `admin` (indexed): current admin address; `contractAddr` (indexed): contract address; `newAdmin`: new admin address |
| `ContractDestroyed` | `ContractDestroyed(address,address)` | `admin` (indexed): admin address; `contractAddr` (indexed): destroyed contract address |

---

## How to Use

### Enabling Internal Contract Event Logs

Append the `includeTraceLogs` query parameter to the RPC endpoint URL. No modifications to the request body are needed, and all existing SDKs remain fully compatible.

```
https://main.confluxrpc.com/?includeTraceLogs
```

or equivalently:

```
https://main.confluxrpc.com/?includeTraceLogs=true
```

Once enabled, `cfx_getLogs` requests sent to this endpoint will support early internal contract event log queries. The request body format remains identical to the standard `cfx_getLogs` specification.

### Query Constraints

> ⚠️ **Only three internal contracts (staking, admin control, and sponsor contracts) are supported for synthetic event log queries. Mixed-address queries are not allowed.**

| Scenario | `address` Field | Behavior |
|:---------|:----------------|:---------|
| Supported internal contracts only ✅ | All addresses are supported internal contract addresses | Returns synthetic event logs reconstructed from trace data |
| Other contracts or unspecified ✅ | All addresses are unsupported internal contracts or normal contracts, or `address` is omitted | Follows standard `cfx_getLogs` logic; `includeTraceLogs` has no effect |
| Mixed addresses ❌ | Contains both supported internal contracts and any other addresses | Returns an error |

---

## Example

Query `Deposit` Events from the Staking Contract

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

> `0xe1fffcc4...` is the result of `keccak256("Deposit(address,uint256)")`.

---

## Important Notes

### Event Synthesis Rules

Internal contract events are not natively emitted on-chain. They are synthetically assembled by Confura from Trace data. An event is generated for an internal contract call only when **all three** of the following conditions are met:

| Condition | Description |
|:----------|:------------|
| Transaction succeeded | The transaction's overall execution status is `Success` |
| Current call succeeded | The internal contract call itself completed with `Success` |
| Entire call chain succeeded | All ancestor calls in the call stack also completed with `Success` |

**Example:**

```
Call chain: EOA → Contract A → Contract B → Staking.deposit() ✅
                                    ↓
                             Contract B reverts ❌

Result: Although Staking.deposit() itself succeeded,
        its parent call (Contract B) failed.
        → No Deposit event is generated.
```

### `logIndex` Field

The `logIndex` field in internal contract event logs is a synthetic index assigned based on the Trace parsing order. It belongs to a separate numbering space from normal contract event logs and is not comparable across the two types.

### Response Format

The response format is fully compatible with the standard `cfx_getLogs` response. All fields carry the same semantics and can be parsed directly using existing SDKs or tooling.

---

## FAQ

**Q: Can I query early internal contract events without the `includeTraceLogs` parameter?**

A: No. Early internal contract event logs are only returned when `includeTraceLogs` is appended to the RPC endpoint URL (e.g., `https://main.confluxrpc.com/?includeTraceLogs`). Requests sent to the standard endpoint are unaffected and behave exactly as before.

**Q: Why does a mixed-address query return an error?**

A: The three early internal contract events and other contract events are stored in separate data sources. Merging results across both sources in a single query is not supported in the current version. If you need both, please issue two separate queries.

**Q: Can the returned events be affected by chain reorganizations (Reorg)?**

A: The service implements Reorg detection and an optimistic locking mechanism that validates data consistency before and after each query. If a Reorg occurs during a query, the request is automatically retried to ensure no stale data is returned. For the strongest consistency guarantees, it is recommended to use `latest_finalized` as the upper bound when querying recent blocks.

**Q: Is historical data complete? From which block height is data available?**

A: Yes. The sync service has completed a full historical backfill, covering all internal contract calls from the genesis block onward.

**Q: How do I calculate `topic0` for an event?**

A: `topic0` is the Keccak-256 hash of the event signature string. For example:

```
keccak256("Deposit(address,uint256)")
= 0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c
```

You can compute this using developer tools or libraries such as [ethers.js](https://docs.ethers.org/), [web3.py](https://web3py.readthedocs.io/), or online utilities like the [Ethereum Signature Database](https://www.4byte.directory/).