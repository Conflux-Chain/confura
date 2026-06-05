# Modules And Capabilities

Use this reference when users ask whether a method should exist on a Confura endpoint, or when they see "method not found" or module-related errors.

## Default Public Modules

When Confura is configured with an empty exposed-module list, it exposes only modules marked public in the code.

| Endpoint kind | Public default modules |
|---|---|
| Core Space | `cfx`, `txpool`, `pos` |
| eSpace | `eth`, `web3`, `net` |
| CfxBridge | `cfx`, `trace`, `txpool` |

Optional modules can be exposed by self-hosted or specially configured endpoints, but are not public defaults.

| Space | Optional or non-default modules |
|---|---|
| Core Space | `trace`, `gasstation`, `diagnostic`, `debug`, metrics service |
| eSpace | `trace`, `parity`, `debug`, `txpool`, `gasstation`, `diagnostic` |

## Method Availability Triage

When a method fails:

1. Check the namespace: Core methods are `cfx_*`; eSpace methods are `eth_*`, `web3_*`, or `net_*`.
2. Check that the endpoint is for the intended space. A Core endpoint will not serve ordinary `eth_*` methods unless it is a bridge or mixed gateway.
3. Check whether the module is public on that endpoint. `diagnostic_*`, `gasstation_*`, `trace_*`, and `debug_*` are commonly disabled.
4. Check JSON-RPC method spelling. Confura's anti-injection middleware only accepts alphanumeric namespace and method parts joined by one underscore.
5. If the endpoint is self-hosted, ask the operator which modules are exposed rather than assuming the codebase's full module set is enabled.

## Optional Feature Guidance

- `diagnostic_getRateLimitStatus` is useful for quota debugging but requires the `diagnostic` module.
- `gasstation_suggestedGasFees` is useful for fee suggestions but requires the `gasstation` module.
- Trace and debug APIs may require full-state or trace-capable upstream nodes and are commonly restricted.
- eSpace `txpool_*` is not public by default, while Core `txpool` is public by default.

## Source Anchors

- `rpc/apis.go`: API module definitions and public flags.
- `rpc/server.go`: exposed-module filtering behavior.
- `util/rpc/middlewares/anti_injection.go`: method-name validation.
