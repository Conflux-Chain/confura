# getLogs Dynamic Query Bounds

Use this reference when helping users call `cfx_getLogs` or `eth_getLogs` through Confura.

## What Is Different From A Plain Fullnode

Plain fullnodes often reject log queries by fixed block or epoch windows. Confura can serve indexed historical logs from off-chain storage, so a wide query can succeed when the matching result set is small.

Confura evaluates practical work rather than just range width:

- Result count: up to 10,000 logs.
- Response body size: default limit is 10 MB.
- Indexed query latency: default timeout is 3 seconds.
- Filter fan-out: address count, topic count, and block-hash count still have configurable limits.
- Newest not-yet-indexed data may still be delegated to upstream fullnodes and can still be subject to fullnode-style split-range limits.

## Filter Shape Rules

Core `cfx_getLogs` filter types are mutually exclusive:

- Epoch range: `fromEpoch`/`toEpoch`.
- Block number range: `fromBlock`/`toBlock`.
- Block hashes: `blockHashes`.

If no type is provided, Confura uses an epoch range at `latest_state`. For block-range Core filters, both `fromBlock` and `toBlock` must be present.

eSpace `eth_getLogs` filter types are mutually exclusive:

- Block range: `fromBlock`/`toBlock`.
- Single block hash: `blockHash`.

If no type is provided, Confura uses the latest block. If the normalized `toBlock` is before the eSpace hardfork block configured by Confura, `eth_getLogs` returns an empty array.

For both spaces:

- `address` count is bounded by configuration.
- `topics` can have at most 4 dimensions.
- Each topic dimension has a configurable count limit.
- Duplicate addresses, block hashes, and topics are deduplicated internally, but clients should still avoid noisy filters.

## How To Answer Users

Tell users not to pre-split every query by a fixed small window. Instead:

1. Send a specific filter with the largest useful historical range.
2. If it succeeds, keep the result.
3. If Confura returns a suggested block or epoch range, retry with that suggested upper bound.
4. Continue from the next block or epoch after the suggested range.
5. Stop when the original target range is fully covered.

## Suggested Range Errors

Confura oversized errors can include phrases like:

```text
a suggested block range is [1000000, 1234567]
```

or:

```text
a suggested epoch range is [90000000, 90001000]
```

The user should use the suggested range's `To` value as the next request's upper bound, then resume from `To + 1`.

If the error has no suggested range, do not invent one. Narrow by address, topic, or range. For very high-frequency topics, add more selective indexed topics or split by time.

## Store, Fullnode, And Tail Behavior

Confura can split a log request internally:

- Indexed historical data can be served from Confura storage.
- Newer data beyond the indexed frontier can be delegated to a fullnode.
- Core epoch-range requests may be converted to block ranges internally and then converted back into suggested epoch ranges when possible.
- `cfx_getFilterLogs` and `eth_getFilterLogs` may reuse Confura's enhanced `getLogs` path when virtual filter is enabled.

This means a query can be fast for deep history but still fail or narrow at the newest tail. For backfills, prefer explicit numeric upper bounds and advance them after indexing catches up.

## Retry Loop Pseudocode

Core epoch query:

```text
from = original_from_epoch
while from <= original_to_epoch:
  request fromEpoch=from, toEpoch=original_to_epoch
  if success:
    append logs
    break
  if error has suggested epoch range [suggested_from, suggested_to]:
    request fromEpoch=from, toEpoch=suggested_to
    append logs from successful retry
    from = suggested_to + 1
    continue
  otherwise handle error normally
```

eSpace block query:

```text
from = original_from_block
while from <= original_to_block:
  request fromBlock=from, toBlock=original_to_block
  if success:
    append logs
    break
  if error has suggested block range [suggested_from, suggested_to]:
    request fromBlock=from, toBlock=suggested_to
    append logs from successful retry
    from = suggested_to + 1
    continue
  otherwise handle error normally
```

## Example Core Wide Query

```shell
curl -X POST '<core_endpoint>' \
  -H 'Content-Type: application/json' \
  -d '{
    "jsonrpc": "2.0",
    "method": "cfx_getLogs",
    "params": [
      {
        "fromEpoch": "0x0",
        "toEpoch": "latest_state",
        "address": ["cfx:type.contract:..."],
        "topics": [["0x..."]]
      }
    ],
    "id": 1
  }'
```

## Example eSpace Wide Query

```shell
curl -X POST '<espace_endpoint>' \
  -H 'Content-Type: application/json' \
  -d '{
    "jsonrpc": "2.0",
    "method": "eth_getLogs",
    "params": [
      {
        "fromBlock": "0x0",
        "toBlock": "latest",
        "address": ["0x..."],
        "topics": [["0x..."]]
      }
    ],
    "id": 1
  }'
```

## Common Pitfalls

- If the query is for a high-frequency event over a wide range, it may exceed result count or response size even if indexed.
- If the newest tail is not indexed yet, fullnode delegation can impose narrower limits.
- If the user mixes Core epoch tags with eSpace `eth_getLogs`, correct the space mismatch.
- If the user receives no suggested range, do not fabricate one; ask them to narrow by address, topic, or range.
- If the user asks for finalized consistency, avoid `latest`/`latest_state` where a finalized upper bound is acceptable.

## Source Anchors

- `rpc/log_filter.go`: filter type defaults, mutual exclusion, and validation.
- `store/log_filter.go`: result limit, timeout, and suggested range error strings.
- `rpc/handler/cfx_logs.go`: Core storage/fullnode split and epoch suggestion conversion.
- `rpc/handler/eth_logs.go`: eSpace storage/fullnode split and block suggestion.
- `rpc/eth_api.go`: eSpace hardfork empty-range behavior.
