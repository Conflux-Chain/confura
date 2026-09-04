# scanLogs 内部使用说明

`scanLogs` 是面向大范围日志顺序扫描的内部 JSON-RPC，不替代支持复杂条件的 `getLogs`。当前提供：

- `cfx_scanLogs`
- `cfx_scanLogsWithPivotAssumption`
- `eth_scanLogs`
- `eth_scanLogsWithPivotAssumption`

请求只支持无过滤、单 address、单 topic0、address + topic0 四种组合。所有数值均使用 JSON-RPC hex quantity。

## 分页规则

- Cursor 是排他的 `(blockNumber, logIndex)`；下一页原样传回上一页的 `nextCursor`。
- `reverse` 缺省为 `false`。
- `limit` 缺省或为 `0x0` 时使用内置默认值 100。
- 服务端不使用 `limit + 1` 探测；客户端在 `len(logs) == effectiveLimit` 时继续请求，短页或空页表示扫描结束。
- 显式数值上界高于请求入口冻结的 `latest_state`（Core）或 `latest`（eSpace）时返回错误，不会截断到当前链头。
- 动态 Tag 在请求入口只解析一次，Handler 重试不会随着链头前进扩大范围。

## WithPivotAssumption

- 第一页（`cursor` 缺省）允许不传第二个 PivotAssumption 参数。
- 续页（`cursor` 非空）必须传入上一页的 `pivotGuard`，否则返回 invalid params。
- 第一页有日志时服务端生成 `pivotGuard`；第一页为空且未提供 assumption 时省略 guard。
- 提供的 assumption 与当前 canonical view 不一致时返回 `pivot assumption failed`（stale 语义）。

## 运行约束

- scanLogs 依赖日志 Store。RPC Proxy 未配置对应 Store 时仍可启动，但调用 scanLogs 会返回 `scan logs unavailable`。
- DB 已裁剪的范围直接返回 pruned 错误，不回退归档节点。
- 响应序列化结果超过 `requestControl.resourceLimits.maxGetLogsResponseBytes` 时整次请求报错，不会静默截断。
- FN checkpoint 双读是乐观一致性检查，不能发现查询期间的 `A→B→A`。调用方可选择 confirmed/safe/finalized 降低风险；严格快照仍依赖节点提供 view token 或原子范围 RPC。

## 请求示例

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "eth_scanLogsWithPivotAssumption",
  "params": [{
    "filter": {
      "blockRange": {"fromBlock": "0x100", "toBlock": "latest"},
      "address": "0x1111111111111111111111111111111111111111"
    },
    "limit": "0x64",
    "reverse": true
  }]
}
```

续页在第一个参数增加 `cursor`，并将上一页的 `pivotGuard` 作为第二个参数传入。
