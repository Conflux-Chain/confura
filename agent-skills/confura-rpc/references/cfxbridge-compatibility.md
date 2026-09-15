# CfxBridge Compatibility

Use this reference when a user calls Core-style `cfx_*`, `trace_*`, or `txpool_*` methods against an endpoint that is backed by eSpace through Confura's CfxBridge.

## What CfxBridge Is

CfxBridge adapts eSpace data into Core-style RPC shapes. It is a compatibility layer, not a complete Core Space implementation.

Public bridge modules:

- `cfx`
- `trace`
- `txpool`

## Address And Range Inputs

For bridge-supported fields:

- Addresses can be provided as 20-byte hex addresses or Conflux base32 addresses. The bridge converts them to eSpace addresses internally.
- Block-number-like Core epoch inputs are interpreted as eSpace block numbers for many methods.
- Bridge filter/block-number arguments support hex numbers, `earliest`, and `latest_state`; do not assume all Core epoch tags are accepted.

For bridge `cfx_getLogs`:

- The filter is converted to an eSpace `eth_getLogs` query.
- `fromEpoch` and `toEpoch` map to eSpace block range.
- `blockHashes` accepts only one eSpace block hash in the bridge representation.
- Some Core log-filter fields such as offset/limit are ignored by the bridge adapter.

## Result Semantics

CfxBridge returns eSpace-derived values in Core-shaped responses. Common surprises:

- There is only one pivot-like block per eSpace block/epoch.
- `cfx_getBlocksByEpoch` returns a single block hash when the eSpace block exists.
- Core-only economic and storage-collateral concepts are zero, empty, or nil. Examples include staking balance, deposit list, vote list, collateral for storage, sponsor fields, admin, and storage root.
- `cfx_estimateGasAndCollateral` maps from eSpace gas estimation; storage collateral is zero.
- Core call request fields such as `storageLimit` and `transactionType` are ignored by the bridge call adapter.

## Trace Behavior

- `trace_transaction` and `trace_block` can convert eSpace traces into Core-shaped localized traces if upstream trace APIs are available.
- `trace_filter` is not implemented by the bridge and returns an empty trace list.

## Answering Guidance

When users see zero staking/sponsor/collateral values or unsupported epoch tags on a bridge endpoint, do not diagnose it as data loss. Explain that CfxBridge is mapping eSpace semantics into Core-shaped methods. If they need native Core state, use a real Core endpoint. If they need EVM state, prefer native `eth_*` methods on an eSpace endpoint.

## Source Anchors

- `rpc/apis.go`: CfxBridge public modules.
- `rpc/cfxbridge/types.go`: address, epoch, call, and log-filter conversion.
- `rpc/cfxbridge/cfx_api.go`: Core-shaped method implementations and zero/empty Core-only fields.
- `rpc/cfxbridge/trace_api.go`: trace conversion and unsupported `trace_filter`.
- `rpc/cfxbridge/txpool_api.go`: pending nonce behavior.
