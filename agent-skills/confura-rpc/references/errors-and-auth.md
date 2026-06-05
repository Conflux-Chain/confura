# Errors And Auth

Use this reference for Confura user-visible auth, method validation, allowlist, IP, and upstream availability errors.

## Invalid Method

Confura validates JSON-RPC method names before dispatch. Accepted shape:

```text
namespace_method
```

Both sides must be alphanumeric. Examples:

- Good: `cfx_getStatus`, `eth_getLogs`, `diagnostic_getRateLimitStatus`.
- Bad: `eth.getLogs`, `eth-getLogs`, `eth_getLogs_extra`.

Invalid shape returns:

```text
invalid JSON-RPC method
```

## API Key Placement

Confura can read an access token from:

- The first path segment: `https://endpoint.example/<access_token>`.
- The `Access-Token` HTTP header.

If a token is present, it must be at least 20 characters and contain only letters and digits. Invalid or expired keys can return messages like:

- `invalid api key`
- `api key is already expired`

When writing examples, preserve the user's key placement style and redact real keys in explanations.

## Allowlist Denials

If an endpoint returns an allowlist error, such as:

```text
access forbidden by allowlists
```

the request reached Confura but was denied by endpoint policy. Ask the user to check the API key, origin, IP, or provider-side allowlist configuration. This is different from a rate-limit error.

## Server Busy

Confura normalizes some upstream or gateway failures into:

```text
server is too busy, please try again later
```

This can come from upstream 502/503/504 failures or an open circuit breaker. Recommend exponential backoff with jitter, avoiding retry storms, and trying a stable upper bound for expensive historical calls. Do not treat it as proof that the JSON-RPC request body is invalid.

## Client IP And Shared Quota

Confura derives client IP from forwarding headers when present:

- `X-Forwarded-For`
- `X-Real-Ip`

It scans right-to-left for a public IP before falling back to the remote address. For rate-limit or allowlist issues, users behind NAT, shared gateways, or proxies may be affected by another caller sharing the same visible IP.

## Source Anchors

- `util/rpc/middlewares/anti_injection.go`: method-name validation.
- `util/rpc/handlers/ip.go`: access-token extraction and client IP extraction.
- `util/rpc/handlers/vip.go`: access-token validity.
- `util/rpc/middlewares/auth.go`: invalid and expired key errors.
- `util/rpc/middlewares/error.go`: server-busy normalization.
- `util/rpc/middlewares/allowlists.go`: allowlist denial wrapping.
