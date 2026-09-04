# scanLogs 发布 Runbook

本地综合测试环境和分阶段执行命令见 [scanLogs 本地测试环境与执行 Runbook](./scanlogs-local-test-runbook.md)。

## 发布前门禁

1. 按 `scanlogs-index-ddl-runbook.md` 完成所有日志表复合索引升级。
2. 确认四类过滤的正序/逆序共 8 组 EXPLAIN 命中新复合索引且没有 `Using filesort`。
3. 运行 `go test ./...`，并在 staging 完成 Core/eSpace 分页结果与固定 canonical view 下 `getLogs` oracle 的比对。
4. 确认部署实例配置了对应日志 Store；未配置时 Proxy 可以启动，但 scanLogs 会返回 unavailable。

## 灰度顺序

1. 单实例开放普通 Core/eSpace scanLogs，并通过现有 QPS 限流控制流量。
2. 观察错误率、耗时、返回条数、DB/FN/mixed 来源、FN window/shrink、DB outer retry、FN inner retry、boundary mismatch、DB cache 和响应大小。
3. 开放 WithPivotAssumption，重点观察 stale、PivotGuard 和续页缺失 assumption 错误。
4. 执行正序、逆序、空页、显式未来上界、ACL 拒绝和响应超限抽样。
5. 指标稳定后逐步扩大流量。

## Go/No-Go 条件

- 不存在 ACL 绕过或 scanLogs 被路由到普通 HTTP 节点组。
- 分页拼接无重复、无遗漏且 Cursor 严格单调。
- 没有持续 boundary mismatch、异常 FN 缩窗或慢 SQL 放大。
- 响应超限返回明确错误，没有短页式静默截断。

## 回滚

下线包含 scanLogs RPC 接入的应用版本；已建立的复合索引保留。回滚不删除索引，也不修改现有 `getLogs` 行为。
