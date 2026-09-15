# Rate Limit Diagnostics

Use this reference when users see Confura rate limit errors or ask how to inspect their current rate limit state.

## Error Meaning

Confura rate limit failures use JSON-RPC error code `-32005`.

Common messages:

- `daily request count exceeded`: the daily quota resource was exhausted.
- `request rate exceeded`: a QPS or token-bucket style resource was exceeded.

Rate limits can be resolved by API key or by client IP, depending on how the endpoint is configured and how the user authenticates.

## Resource Names

Confura commonly uses:

- `rpc_all_daily`: daily total requests.
- `rpc_all_qps`: global QPS.
- Method QPS resources such as `cfx_getStatus_qps`, `cfx_getLogs_qps`, or `eth_getLogs_qps`.

Strategies can use:

- `fixed_window`
- `token_bucket`

## Diagnostic Method

Users can inspect the effective rate limit state with:

```text
diagnostic_getRateLimitStatus
```

Important: the endpoint must expose the `diagnostic` module. Public endpoints may not expose it. If the method is unavailable, do not assume the user has no limits; explain that diagnostics may be disabled.

Operators may also expose:

```text
diagnostic_listRateLimitStrategies
```

Use it only when the endpoint exposes `diagnostic` and the user is allowed to inspect strategy definitions.

## Example Request

```shell
curl -X POST '<endpoint>/<api_key>' \
  -H 'Content-Type: application/json' \
  -d '{
    "jsonrpc": "2.0",
    "method": "diagnostic_getRateLimitStatus",
    "params": [],
    "id": 1
  }'
```

If the user's endpoint does not use API-key paths, remove `/<api_key>`. If it uses a different key format, preserve that format.

## How To Explain A Response

Response shape:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "strategy": {
      "ID": 1,
      "Name": "vip1",
      "LimitOptions": {
        "rpc_all_daily": {
          "Interval": 86400000000000,
          "Quota": 1000000
        },
        "rpc_all_qps": {
          "Rate": 20,
          "Burst": 40
        }
      }
    },
    "limitType": "by_key",
    "info": {
      "userType": "provisioned_user",
      "clientIp": "203.0.113.10",
      "apiKey": "example-api-key"
    }
  }
}
```

Explain:

- `strategy.Name`: which policy applies.
- `LimitOptions`: the resources and limit parameters.
- `limitType`: whether the request is identified by key or IP.
- `info.clientIp`: the IP Confura sees, useful when a proxy/NAT is involved.
- `info.userType`: commonly `guest`, `web3pay_user`, or `provisioned_user`.
- Empty result: no matching strategy was resolved for the request context, not necessarily no global limit.

If the endpoint returns `rate limit registry is not configured`, diagnostics are exposed but the registry is not configured for that Confura instance.

## How Confura Applies Limits

Confura can enforce:

- Overall QPS with resource `rpc_all_qps`.
- Per-method QPS with resource names such as `cfx_getLogs_qps` or `eth_getLogs_qps`.
- Daily total request count with resource `rpc_all_daily`.

The effective strategy is resolved from request context. It can be based on IP, API key, Web3Pay/VIP context, or provisioned key metadata. For IP-based limits, users behind NAT, shared gateways, or proxies may share quota.

## User Guidance

For `daily request count exceeded`:

- Reduce polling frequency or batch requests where possible.
- Cache stable responses client-side.
- Wait for quota reset or use a higher quota/API key if available.

For `request rate exceeded`:

- Add exponential backoff and jitter.
- Limit concurrent workers.
- Use method-specific throttling, especially for expensive calls like `getLogs`.
- For `getLogs`, use suggested dynamic ranges and avoid retry storms over the same oversized range.

For IP-based limits:

- Explain that users behind NAT or shared proxies may share a quota.
- If they have an API key, ensure requests use the endpoint/key format expected by the provider.

For diagnostic method unavailable:

- Explain that the endpoint may not expose `diagnostic`.
- Ask the RPC provider/operator for the applicable limit policy, or use observed error messages and retry behavior.

## Source Anchors

- `util/rpc/middlewares/rate_limit.go`: QPS and daily limit resources, error code `-32005`.
- `rpc/diagnostic_api.go`: diagnostic method behavior.
- `util/rate/resolver.go`: guest, Web3Pay, and provisioned strategy resolution.
- `util/rpc/handlers/ip.go`: client IP extraction from forwarding headers.
