---
name: confura-rpc
description: Help end users and application developers use Confura public or self-hosted RPC services correctly. Use when an agent needs to explain or generate Core Space or eSpace JSON-RPC requests, choose method namespaces and exposed modules, handle `cfx_getLogs` or `eth_getLogs` dynamic bounds and suggested retry ranges, query early Core internal-contract synthetic logs with `includeTraceLogs`, use filters or WebSocket subscriptions, interpret caching and consistency behavior, call optional gasstation or diagnostic APIs, diagnose auth, allowlist, rate-limit, and server-busy errors, or explain CfxBridge compatibility. Do not use for Confura source-code development, deployment, or operations; use a developer/operator skill instead.
---

# Confura RPC

## Overview

Use this skill when helping application developers or end users consume Confura-compatible JSON-RPC endpoints. Keep the answer focused on request construction, method availability, error handling, retry strategy, endpoint query parameters, and user-visible Confura behavior.

Do not include Confura deployment, database, sync, node-manager, or source-code guidance. If the user asks how to run or modify Confura itself, use a developer/operator skill instead.

## First Decision

Before answering, classify the user request:

- General RPC call or endpoint usage: read `references/request-patterns.md`.
- Method availability, exposed modules, public vs optional APIs, or "method not found": read `references/modules-and-capabilities.md`.
- Wide historical log query, oversized `getLogs`, or suggested retry range: read `references/getlogs-dynamic-bounds.md`.
- Early Core Space internal contract events, Staking/Sponsor/AdminControl logs, or `includeTraceLogs`: read `references/internal-contract-trace-logs.md`.
- Polling filters, `getFilterChanges`, `getFilterLogs`, or WebSocket subscriptions: read `references/filters-and-subscriptions.md`.
- Rate limit error, quota/QPS reason, API key/IP strategy, or `diagnostic_getRateLimitStatus`: read `references/rate-limit-diagnostics.md`.
- Invalid key, allowlist denial, invalid method, server busy, or confusing HTTP/gateway error: read `references/errors-and-auth.md`.
- Gas price, CIP-1559 fee history, max priority fee, or `gasstation_*`: read `references/gas-and-fees.md`.
- Recent/pending transaction visibility, stale reads, cached responses, historical state availability, or receipt correctness: read `references/caching-and-consistency.md`.
- Core-like RPC backed by eSpace, bridge endpoints, converted addresses, or CfxBridge surprises: read `references/cfxbridge-compatibility.md`.

## Working Rules

- Always distinguish Core Space (`cfx_*` methods, epochs, Conflux base32 addresses) from eSpace (`eth_*` methods, block numbers, hex addresses).
- Always distinguish mainnet, testnet, public hosted endpoints, and self-hosted endpoints. If the user did not provide an endpoint, ask for or use placeholders rather than inventing one.
- Treat module availability as endpoint-specific. Public default modules are narrower than the full Confura codebase; optional modules such as `diagnostic`, `gasstation`, `trace`, `debug`, and some `txpool` APIs may be disabled.
- For API-key endpoints, preserve the path or query format the user provides. Do not expose or repeat secret keys in summaries.
- JSON-RPC method names should use the `namespace_method` shape, such as `cfx_getStatus` or `eth_getLogs`.
- For `getLogs`, prefer Confura's suggested retry range over fixed small block windows when an oversized error includes a suggestion.
- For Core internal contract synthetic logs, use `cfx_getLogs` with the same request body shape as normal logs, but add `includeTraceLogs` to the endpoint URL.
- For mixed normal-contract and supported internal-contract log queries, issue separate requests.
- For recent data where consistency matters, prefer finalized/stable upper bounds where applicable and explain that some read methods are cached for short TTLs.
- If asked about exact public endpoint availability, verify from current official sources or ask the user for the endpoint. Repo examples are not necessarily a complete endpoint catalog.

## Reference Index

- `references/request-patterns.md`: Core/eSpace request construction, endpoint and API-key handling, safe examples.
- `references/modules-and-capabilities.md`: default public modules, optional modules, method availability checks.
- `references/getlogs-dynamic-bounds.md`: dynamic `getLogs` behavior, limits, suggested range retry loops.
- `references/internal-contract-trace-logs.md`: early internal contract synthetic logs and supported event matrix.
- `references/filters-and-subscriptions.md`: virtual filter, polling filter, and WebSocket subscription behavior.
- `references/rate-limit-diagnostics.md`: rate limit error interpretation and diagnostic request flow.
- `references/errors-and-auth.md`: auth token, allowlist, invalid method, IP detection, and server-busy error handling.
- `references/gas-and-fees.md`: fee-history limits, priority fee methods, and optional gasstation responses.
- `references/caching-and-consistency.md`: short-lived RPC caches, pending transaction cache, state fallback, receipt revalidation.
- `references/cfxbridge-compatibility.md`: CfxBridge endpoint semantics and compatibility traps.
