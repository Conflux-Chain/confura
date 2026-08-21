# Gas And Fees

Use this reference when users ask about gas price, priority fee, fee history, or gasstation methods.

## Native Fee Methods

Core Space:

- `cfx_gasPrice`
- `cfx_maxPriorityFeePerGas`
- `cfx_feeHistory`

eSpace:

- `eth_gasPrice`
- `eth_maxPriorityFeePerGas`
- `eth_feeHistory`

Confura guards both `cfx_feeHistory` and `eth_feeHistory`:

- `blockCount` must be at most 1024.
- `rewardPercentiles` must contain at most 50 values.

If a user needs a longer history, split the request into multiple fee-history calls.

## Optional Gasstation Module

If the endpoint exposes `gasstation`, users can call:

```text
gasstation_suggestedGasFees
```

The response can include:

- `low`, `medium`, and `high` fee estimates.
- `estimatedBaseFee`.
- `networkCongestion`.
- recent and historical priority-fee ranges.
- historical base-fee range.
- `priorityFeeTrend` and `baseFeeTrend`.

If the gasstation historical handler is unavailable, Confura falls back to current chain data. In that fallback, `low`, `medium`, and `high` can be identical because they are derived from latest base fee plus max priority fee.

Core also exposes a legacy gasstation method when the module is enabled:

```text
gasstation_price
```

It returns `fast`, `fastest`, `safeLow`, and `average` gas-price fields.

## Answering Guidance

- If `gasstation_suggestedGasFees` returns "method not found", explain that the `gasstation` module is optional and use native fee methods instead.
- Treat returned fee quantities as chain units: Core values are in drip-like native units, eSpace values are in wei-like EVM units.
- For transactions, prefer `maxPriorityFeePerGas` plus current or recent base-fee estimates over stale hardcoded gas prices.
- For fee analytics, use `feeHistory` within the 1024-block limit and paginate.

## Source Anchors

- `rpc/cfx_gastation_api.go` and `rpc/eth_gastation_api.go`: gasstation method behavior and fallback.
- `types/gastation.go`: response fields.
- `rpc/handler/gasstation.go`: historical window and percentiles.
- `rpc/cfx_api.go` and `rpc/eth_api.go`: `feeHistory` guard limits.
