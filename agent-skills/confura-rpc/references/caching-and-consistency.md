# Caching And Consistency

Use this reference when users see slightly stale data, repeated `eth_call` errors, pending transaction visibility issues, or historical state availability errors.

## Short-Lived Caches

Confura caches selected lightweight methods to protect upstream nodes.

Core Space defaults:

- `cfx_getStatus`: about 1 second per node.
- `cfx_epochNumber`: derived from cached status when possible; explicit `earliest` returns `0` directly.
- `cfx_bestBlockHash`: about 1 second via status/best-hash cache.
- `cfx_gasPrice`: about 3 seconds.
- `cfx_clientVersion`: about 1 minute.

eSpace defaults:

- `net_version`: about 1 minute.
- `web3_clientVersion`: about 1 minute.
- `eth_chainId`: long-lived, about 1 year.
- `eth_blockNumber` and block-tag normalization: about 1 second.
- `eth_gasPrice`: about 3 seconds.
- `eth_call`: about 1 second, cache size 1024 by default.

`eth_call` caches both successful results and JSON-RPC errors for the short TTL. If a user immediately repeats a reverting or state-dependent call, they may see the same error until the TTL expires.

## Pending Transaction Cache

After `eth_sendRawTransaction` succeeds, Confura records the transaction hash in a pending transaction cache.

Defaults:

- Pending transaction cache TTL: about 3 minutes.
- Initial check exemption: about 3 seconds.
- Recheck interval after exemption: about 1 second.

User-facing effect:

- `eth_getTransactionByHash` can return cached pending transaction information before hitting a fullnode.
- `eth_getTransactionReceipt` can return empty while the transaction is still pending.
- For send-then-receipt polling, wait a few seconds, then poll with backoff rather than hammering every few milliseconds.

## Data Cache And Store Reads

Confura can serve some recent or historical eSpace data from near-head cache, data cache, or store before falling back to a fullnode. Historical logs can be served from Confura storage. This improves availability and throughput, but users should still design indexers to handle reorgs and retryable upstream failures.

For consistency-sensitive workflows:

- Use explicit numeric block or epoch upper bounds for backfills.
- Wait for finality or a confirmation depth before irreversible off-chain actions.
- Avoid assuming `latest` or `latest_state` is identical across all upstream nodes at the same instant.

## Historical State Fallback

For state methods such as balances, code, storage, calls, gas estimation, and trace/debug state reads, Confura can retry on a full-state node group when the initial node returns errors matching:

- `state is not ready`
- `out-of-bound StateAvailabilityBoundary`

If the user still sees these errors, the endpoint may not have full-state fallback configured, or the requested historical state may be outside available data.

## Receipt Revalidation

Some eSpace deployments enable receipt revalidation. If a receipt does not match the transaction's block hash, Confura can check other nodes. If no valid match is found, the user may see:

```text
no matching receipts found: this may indicate potential data corruption
```

Treat this as a consistency warning. Retry after a short delay, use a more finalized block, or ask the provider if the issue persists.

## Source Anchors

- `util/rpc/cache/cache_cfx.go` and `util/rpc/cache/status.go`: Core cache TTLs and status-derived values.
- `util/rpc/cache/cache_eth.go`: eSpace cache TTLs and pending transaction cache settings.
- `util/rpc/cached_client_eth.go`: `eth_call`, pending transaction, transaction, and receipt cache behavior.
- `rpc/handler/cfx_state.go` and `rpc/handler/eth_state.go`: full-state fallback.
- `rpc/eth_api.go`: receipt revalidation error.
