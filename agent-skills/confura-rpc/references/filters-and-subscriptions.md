# Filters And Subscriptions

Use this reference when users ask about polling filters, `getFilterChanges`, `getFilterLogs`, or WebSocket subscriptions.

## Polling Filter Methods

Core filter methods:

- `cfx_newFilter`
- `cfx_newBlockFilter`
- `cfx_newPendingTransactionFilter`
- `cfx_getFilterChanges`
- `cfx_getFilterLogs`
- `cfx_uninstallFilter`

eSpace filter methods:

- `eth_newFilter`
- `eth_newBlockFilter`
- `eth_newPendingTransactionFilter`
- `eth_getFilterChanges`
- `eth_getFilterLogs`
- `eth_uninstallFilter`

## Filter Semantics

- Treat `newFilter` plus `getFilterChanges` as polling for changes. For deterministic historical backfills, prefer direct `cfx_getLogs` or `eth_getLogs`.
- eSpace log filters default `fromBlock` and `toBlock` to `latest`; `pending` is not supported for `eth_newFilter`.
- eSpace reorged logs can be returned again with `removed: true`.
- Core virtual filters can return log changes and chain reorg entries.
- `getFilterChanges` for pending transaction and block filters returns hashes; log filters return log objects.

## Virtual Filter Behavior

Confura can proxy filter APIs through its virtual filter service. User-visible behavior:

- Default virtual filter TTL is 1 minute. Clients should poll before the TTL expires.
- Expired or invalid filters can produce `filter not found`.
- When virtual filter is enabled, `cfx_getFilterLogs` and `eth_getFilterLogs` retrieve the original filter and reuse Confura's enhanced `getLogs` path, so dynamic bounds and storage/fullnode behavior may apply.
- When virtual filter is not enabled, filter methods are delegated to the upstream fullnode.

## WebSocket Subscriptions

Subscriptions require a WebSocket endpoint. HTTP requests usually return notifications unsupported.

Core subscriptions:

- `cfx_subscribe` with `newHeads`.
- `cfx_subscribe` with `epochs`; only `latest_mined` and `latest_state` are supported, and nil defaults to `latest_mined`.
- `cfx_subscribe` with `logs`.

eSpace subscriptions:

- `eth_subscribe` with `newHeads`.
- `eth_subscribe` with `logs`.

Do not promise eSpace `newPendingTransactions` or `syncing` subscriptions unless the endpoint provider documents support.

## Queue And Reconnect Guidance

Confura delegates upstream subscriptions and fans out notifications with a per-subscription queue. If the client does not drain messages fast enough, the subscription can fail with a queue-overflow style error and the connection may close.

For users:

- Keep the WebSocket reader loop fast.
- Acknowledge and process messages asynchronously in the application.
- Reconnect and resubscribe on `subscription proxy error`, queue overflow, or connection close.
- For log subscriptions, keep filters narrow by address and topic.

## Source Anchors

- `rpc/cfx_api_filter.go` and `rpc/eth_api_filter.go`: polling filter methods and virtual filter delegation.
- `virtualfilter/config.go`: default TTL.
- `virtualfilter/filter.go`: `filter not found`.
- `rpc/cfx_api_pubsub.go` and `rpc/eth_api_pubsub.go`: supported subscription types.
- `rpc/pubsub.go`: queue size and overflow behavior.
