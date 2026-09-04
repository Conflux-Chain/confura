# scanLogs 本地综合测试计划

> 对应任务：`[scanLogs][测试] scanLogs 综合测试与发布验收`
>
> 目标：在应用发布前，用可重复的本地流程完成 DDL、数据正确性、功能、一致性故障、性能和回归测试，最后将同一套只读用例带到 staging 验收。

具体执行入口、环境变量和人工操作步骤见 [scanLogs 本地测试环境与执行 Runbook](../scripts/scanlogs-local-test-runbook.md)。所有测试的前置数据、输入、步骤、断言、证据和 PASS/FAIL 标准，以 [scanLogs 综合测试用例规格](scanlogs_test_cases.md) 为准；manifest 只是该规格的执行参数载体。

## 1. 测试范围和总体策略

本计划覆盖：

- Core Space 和 eSpace 的四个 scanLogs RPC；
- 无过滤、address、topic0、address + topic0 四类过滤；
- 正序、逆序，纯 DB、纯 FN、跨 DB/FN 水位；
- Cursor、Limit、Range、PivotAssumption/PivotGuard 和空页语义；
- DDL 幂等性、全量物理表索引和查询计划；
- DB outer / FN inner retry、boundary 对齐、DB cache 复用、FN 缩窗和 Core 路线 B；
- 严格 JSON、默认错误码、ACL、Full Node 日志组路由和响应超限；
- 现有 `cfx_getLogs` / `eth_getLogs` 回归以及本地性能基线。

测试分成两条通道：

1. **真实集成通道**：本地 MySQL 8、本地编译的 Confura、Core/eSpace Full Node。用于 DDL、分页 oracle 比对、功能和性能测试。
2. **可控故障通道**：带计数器的 fake Store/FN、stub RPC 和 fake clock。用于稳定注入 reorgVersion、checkpoint、boundary 和 oversized 事件。这些场景不依赖真链恰好发生 reorg。

FN `A→B→A` 是已接受的一致性边界，不将“必须检出 ABA”设为通过条件。测试只要求 `before=A, after=B` 的可观察变化一定阻止 candidate 提交。

优先级口径：DDL、数据正确性、一致性、安全、协议和回归用例均为 P0；性能指标未达建议值但无正确性/稳定性异常时可评为 P1；额外的全仓 `-race` 属于加强项。

## 2. 执行前的协议口径门禁

在生成 oracle 前先将以下预期写入测试 manifest，并由开发和测试各确认一次。当前代码口径为：

| 项目 | 当前实现的预期 |
|---|---|
| 默认 Limit | `limit` 缺省或为 `0x0` 时取 100；`maxLimit` 默认 1000 |
| stale | JSON-RPC code 为框架默认 `-32000`，message 需表达 `pivot assumption failed` / `pivot assumption does not match` |
| WithPivot 首个空页 | 没有输入 assumption 时 `pivotGuard` 省略；有 assumption 时原样返回 |
| 逆序 Guard | 首页有数据时取本页最高位日志；续页和空页保持首页 assumption |
| NextCursor | 非空页等于该页最后一条日志的 `(blockNumber, logIndex)`；空页为 `nil` |
| 结束判定 | 不用 `limit+1`；短页或空页结束，恰好整页时允许再请求一个空页 |

如果产品口径与上表不同，先修正代码或内部文档，不在测试脚本中兼容两套相互冲突的期望。

## 3. 本地测试拓扑

```text
E2E validator ─┬─ getLogs oracle ────────> Core/eSpace FN
               └─ scanLogs ─────────────> Core/eSpace RPC
                                                ├─ Store read ─> MySQL 8 (Core/eSpace DB)
                                                └─ logs group > node manager ─> FN

Core/eSpace FN ───────> Core/eSpace sync ────> MySQL 8 (Core/eSpace DB)
```

| 组件 | 建议实例 | 要求 |
|---|---:|---|
| MySQL | 1 | MySQL 8.x；Core/eSpace 使用两个独立测试库 |
| Core/eSpace sync | 各 1 | 先追到预定水位，再暂停，使 DB/FN 分界可重复 |
| node manager | 各 1 | 普通节点组和 logs 节点组分别可观测 |
| RPC Proxy | 各 1 | 使用当前 checkout 编译的二进制，连接对应 Store |
| Full Node | Core/eSpace 各 1 | 同一 endpoint 同时作为 logs 来源和 `getLogs` oracle；要求保留测试范围 |
| 故障 stub | 按需 | 本地 HTTP stub/fake client，不连公网 |

可以复用 `docker-compose.yml` 启动 MySQL/InfluxDB，但不能直接使用未重建的 `conflux/confura:latest` 作为被测版本。应当使用当前 checkout 执行 `make build`，或者显式重建并记录本地镜像 digest。

### 3.1 运行记录目录

每次执行使用独立 Run ID：

```bash
export SCANLOGS_RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
export SCANLOGS_ARTIFACT_DIR="$(pwd)/artifacts/scanlogs/${SCANLOGS_RUN_ID}"
mkdir -p "${SCANLOGS_ARTIFACT_DIR}"/{env,unit,ddl,data,e2e,fault,api,regression,perf,staging}

go version >"${SCANLOGS_ARTIFACT_DIR}/env/go-version.txt"
git rev-parse HEAD >"${SCANLOGS_ARTIFACT_DIR}/env/git-revision.txt"
git status --short >"${SCANLOGS_ARTIFACT_DIR}/env/git-status.txt"
```

同时记录 MySQL 版本、Confura 启动配置摘要、Full Node 客户端版本、Core/eSpace DB 水位和测试机硬件。不得将 DB 密码、API key 或带 token 的完整 endpoint 写入 artifacts。

### 3.2 环境预检

1. `go version` 满足 `go.mod` 的 Go 1.23 toolchain。
2. `mysql --version` 为 MySQL 8.x client，服务端支持 `ALGORITHM=INPLACE, LOCK=NONE`。
3. Core/eSpace Full Node 的数字高度、block/epoch summary 和 `getLogs` 可查。
4. `node.logNodes` / `node.ethLogNodes` 不为空；RPC 已指向本地 node manager。
5. Core RPC 已配置 `store.mysql`，eSpace RPC 已配置 `ethstore.mysql`；两个 persistence types 都包含 `log`。
6. 在可丢弃的测试库操作 DDL，不对开发者的唯一数据副本执行 DROP INDEX。

服务启动后先用原始 JSON-RPC 请求验证 `cfx_getStatus`、`eth_blockNumber`、`cfx_getLogs`、`eth_getLogs` 及四个 scanLogs 方法可达。

## 4. 测试数据集和 canonical view

### 4.1 固定数字视图

每次测试必须生成 `data/manifest.json`，至少包含：

- Core `fromEpoch/toEpoch`、两端 pivot hash；
- eSpace `fromBlock/toBlock`、两端 block hash；
- 数据采集时的 DB earliest/latest mapping；
- 四类过滤所用 address/topic0、oracle 条数和预期页数；
- 大小合约/大 topic 如已迁移，记录其 shared/dedicated 状态；
- 每个高度的选择理由：纯 DB、纯 FN或跨水位。

不使用动态 tag 生成 oracle。先将 `latest_finalized`/`latest_confirmed`/`finalized` 解析成数字高度，之后 `getLogs` 和 scanLogs 都只使用同一组数字端点。动态 tag 的“只解析一次”另放在故障通道测试。

在 oracle 查询和分页扫描前后再读取上界 hash；两次不一致时本轮作废重跑，不将其记为 scanLogs 数据错误。

### 4.2 三类数据来源范围

暂停本地 sync 后读取：

```sql
SELECT epoch, bn_min, bn_max, pivot_hash
FROM epoch_block_map ORDER BY epoch ASC LIMIT 1;

SELECT epoch, bn_min, bn_max, pivot_hash
FROM epoch_block_map ORDER BY epoch DESC LIMIT 1;
```

以固定的 latest mapping 为分界：

| 来源 | Core 范围 | eSpace 范围 | 预期 |
|---|---|---|---|
| 纯 DB | `toEpoch <= dbMaxEpoch` | `toBlock <= dbMaxBn` | 不调 FN checkpoint，不做 boundary |
| 纯 FN | `fromEpoch > dbMaxEpoch` | `fromBlock > dbMaxBn` | 做 FN checkpoint，不做 boundary |
| mixed | `fromEpoch <= dbMaxEpoch < toEpoch` | `fromBlock <= dbMaxBn < toBlock` | 同时使用 DB/FN，做 boundary 对齐 |

另启动一个指向空 mapping 测试库的 RPC 实例，验证它退化为纯 FN；用独立可丢弃库构造 earliest 高于请求下界的情形，验证 pruned 错误直接返回。

### 4.3 过滤数据要求

每个 Space 至少选出以下数据：

| 数据 ID | 用途 | 最低要求 |
|---|---|---|
| D1 | 密集无过滤 | oracle 条数 `>= 3 * 分页 limit` |
| D2 | address | 至少跨 3 页，并有同 block 多 logIndex |
| D3 | topic0 | 至少跨 3 页 |
| D4 | address + topic0 | 至少跨 2 页 |
| D5 | 稀疏/空结果 | 一组不存在的 address/topic0，一组大范围稀疏日志 |
| D6 | 页数恰好整除 | oracle 条数恰好是 limit 的倍数，验证最后可追加一个空页 |
| D7 | Core 同 epoch 多 block | `bn_max > bn_min`，且至少两个 block 中有匹配日志 |
| D8 | DB/FN boundary | 水位前后都有匹配日志 |

数据选择器应自动扫描候选范围并输出 manifest；不将某个网络的高度、address 或 topic 硬编码进通用测试逻辑。

## 5. 执行阶段

### T0：编译、单测和覆盖盘点

先执行：

```bash
make build
go test -count=1 ./store/mysql ./rpc/handler ./util/acl
go test -race -count=1 ./store/mysql ./rpc/handler ./util/acl
go test -count=1 ./...
```

将标准输出和退出码保存到 `unit/`。如果全量 `-race ./...` 在本机耗时可接受，将其作为最终补充门禁。

现有单测可复用，但本测试任务在执行 E2E 前还要做一次“Todo → 自动化用例”映射：

| 能力 | 已有主要入口 | 本任务需补齐 |
|---|---|---|
| Store keyset/分区/路由 | `store/mysql/store_scan_log*_test.go` | 真 MySQL 跨分区查询 |
| Plan、Core/eSpace Reader、Pivot | `rpc/handler/scan_logs_test.go` | 跨 RPC + Store 的端到端断言 |
| 严格 JSON、Limit、响应超限 | `rpc/handler/scan_logs_rpc_test.go` | 原始 HTTP JSON-RPC code/message 断言 |
| ACL | `util/acl/validator_scan_logs_test.go` | 带真实 API key/中间件的拒绝请求 |
| logs 节点组路由 | 实现已接入 middleware | 增加 middleware 单测与双 stub 集成测试 |
| outer/inner/boundary/cache | 已有部分 fake 用例 | 按 T4 事件表完整补齐调用次数和顺序断言 |

盘点表中任一 P0 行没有可执行的自动化用例时，先补测试，不以人工读代码代替。

### T1：测试库 DDL 全流程

对 Core/eSpace 两个库分别执行以下顺序。本地可通过 `MYSQL_PWD` 直接提供密码：

```bash
MYSQL_PWD=<PASSWORD> ./scripts/scanlogs-index-ddl.sh --database <DB> --address-partitions <N> --topic-partitions <N> --host 127.0.0.1 --port 3306 --user root --mode plan
MYSQL_PWD=<PASSWORD> ./scripts/scanlogs-index-ddl.sh --database <DB> --address-partitions <N> --topic-partitions <N> --host 127.0.0.1 --port 3306 --user root --mode add --execute
MYSQL_PWD=<PASSWORD> ./scripts/scanlogs-index-ddl.sh --database <DB> --address-partitions <N> --topic-partitions <N> --host 127.0.0.1 --port 3306 --user root --mode add --execute
MYSQL_PWD=<PASSWORD> ./scripts/scanlogs-index-ddl.sh --database <DB> --address-partitions <N> --topic-partitions <N> --host 127.0.0.1 --port 3306 --user root --mode verify
MYSQL_PWD=<PASSWORD> ./scripts/scanlogs-index-ddl.sh --database <DB> --address-partitions <N> --topic-partitions <N> --host 127.0.0.1 --port 3306 --user root --mode drop --execute
MYSQL_PWD=<PASSWORD> ./scripts/scanlogs-index-ddl.sh --database <DB> --address-partitions <N> --topic-partitions <N> --host 127.0.0.1 --port 3306 --user root --mode verify
```

第二次 `add` 必须全部 skip 或无变更，用来证明幂等。`drop` 只删除被替代的旧索引，最后一次 `verify` 必须在旧索引已不存在时仍通过。

验收点：

- 枚举出的每个物理表都有正确列顺序的新索引；
- 无过滤/address/topic0/address+topic0 × 正逆序的 8 个逻辑查询计划全部命中预期 key，`Extra` 不包含 `Using filesort`；
- 脚本对每个非空物理表都做相应 EXPLAIN，因此实际输出可以多于 8 行，不能只抽一张表代表整个表族；
- `logs_*`、`addr_logs_*`、`topic_logs_*`、`clogs_*`、`tlogs_*` 中存在的表族都必须被覆盖；
- 任一索引定义冲突、表缺失或 EXPLAIN 不合格时返回非 0。

在可丢弃的 schema 副本上再执行负向用例：缺物理表、同名错列索引、错误分区数、`add/drop` 不带 `--execute`。全部应非 0 退出，且不应开始后续 DROP。

### T2：E2E 分页校验器

在 `test/scanlogs_validate.go` 与 `cmd/test/scanlogs.go` 增加可重复的校验入口，建议命令形式为：

```bash
./bin/confura test scanlogs \
  --cfx-fn-endpoint <CFX_FN> --cfx-infura-endpoint http://127.0.0.1:22537 \
  --eth-fn-endpoint <ETH_FN> --eth-infura-endpoint http://127.0.0.1:28545 \
  --case-manifest "${SCANLOGS_ARTIFACT_DIR}/data/manifest.json" \
  --page-limits 1,7,100 \
  --output "${SCANLOGS_ARTIFACT_DIR}/e2e/result.json"
```

如校验器还未落地，它属于本测试任务的产出，不以一批不可重复的 curl 命令替代。

校验器算法：

1. 读取 manifest 中的固定数字范围和过滤。
2. 在 Full Node 上用同一范围调用 `getLogs`，得到 oracle。
3. eSpace 直接取日志 `(blockNumber, logIndex)`；Core 用带 cache 的 `GetBlockSummaryByHash` 将每个日志 blockHash 转成真实 blockNumber。
4. 以 `(blockNumber, logIndex)` 为排序 key；以 `(blockHash, transactionHash, logIndex)` 为日志身份，对结构化 JSON 做完整等值比较。
5. 从无 Cursor 开始请求，每页将 `nextCursor` 原样带到下一页；WithPivot 续页同时传入上一页 Guard。
6. 检查页内严格单调、跨页严格单调、没有身份重复、每页数量 `<= effectiveLimit`。
7. 正序拼接结果与 oracle 完全一致；逆序结果与 `reverse(oracle)` 完全一致。
8. 非空页检查 Cursor 等于本页 tail key；空页检查 `logs=[]` 且 Cursor 省略。
9. 在开始和结束后重新检查 canonical 上界 hash；如果 view 改变，标记该 case 为 invalidated 而不是 failed，整个 case 重跑。
10. 失败时输出第一个分歧 key、页号、请求、预期/实际日志和 Guard，同时生成 JSON/JUnit 结果。

为防止死循环，校验器需有 `maxPages`、case timeout 和“连续两页 Cursor 相同立即失败”保护。

### T3：基础 E2E 矩阵

基础矩阵固定使用较小的 `limit=7` 强制多页：

```text
2 Space × 2 方向 × 3 来源 × 4 过滤 × 2 方法变体 = 96 个 P0 case
```

其中两个方法变体是普通 scanLogs 和 WithPivotAssumption。数据稀疏导致某个组合不足两页时，更换数据集，不降低页数要求。

再执行以下边界用例：

| ID | 场景 | 通过条件 |
|---|---|---|
| CUR-01 | Cursor 落在同 block 两个 logIndex 之间 | 排他，不重返 cursor 前日志 |
| CUR-02 | Cursor 是范围内不存在的 logIndex | 返回严格位于其后/前的第一条 |
| CUR-03 | Cursor 位于 DB/FN 分界两侧 | owner 唯一，不向两个 Segment 重复传递 |
| CUR-04 | Cursor 低于下界、高于冻结上界 | `-32000` invalid cursor，无部分结果 |
| CUR-05 | Cursor 位于范围端点 | 正逆序均遵循排他语义 |
| LIM-01 | `limit` 缺省、`0x0`、`0x1`、`0x64`、`maxLimit` | 条数不超限，缺省/0 按协议门禁取值 |
| LIM-02 | `maxLimit+1` | `-32000` invalid params |
| LIM-03 | oracle 条数恰好整除 limit | 所有数据完整，允许最后一次空请求 |
| RNG-01 | 缺省端点、数字端点、动态 tag | 解析后的实际范围与 oracle 一致 |
| RNG-02 | `from > to`、显式未来 upper | `-32000` invalid params/filter，不静默截断 |
| EMP-01 | 无匹配日志 | `logs=[]`、无 Cursor，普通 scan 无 Guard |
| MAP-01 | mapping 全空 | 纯 FN 执行，结果与 oracle 一致 |
| MAP-02 | earliest 存在但 latest 缺失 | 重试后返回 consistency error，不当作空 Store |
| MAP-03 | 请求早于已知 earliest | 直接返回 pruned error，不回退 FN |

### T4：Pivot 与一致性故障注入

#### Pivot 用例

| ID | 操作 | 通过条件 |
|---|---|---|
| PIV-01 | WithPivot 正序首页/续页 | Guard 对应每页 tail 日志的 canonical pivot |
| PIV-02 | WithPivot 逆序首页 | Guard 对应该页最高位日志 |
| PIV-03 | WithPivot 逆序续页 | Guard 数值与首页一致，不向低位漂移 |
| PIV-04 | 空首页，无 assumption | 按第 2 节冻结的口径省略 Guard |
| PIV-05 | 空页，有 assumption | 原样返回 Guard |
| PIV-06 | 续页不传 assumption | `-32000` invalid params |
| PIV-07 | 篡改 block/pivot hash | `-32000` stale/assumption failure，不返回日志 |
| PIV-08 | 普通 scan 所有页 | 从不返回 `pivotGuard` |

#### 故障事件表

以 fake Store/FN 记录每次 DB scan、mapping、checkpoint、boundary、summary 和 `getLogs` 调用，断言顺序与次数：

| ID | 注入事件 | 必须断言 |
|---|---|---|
| CON-01 | `v0 != v1` | outer retry；Plan、Cursor owner、水位和 DB cache 全部重建 |
| CON-02 | FN `before != after`，DB version 不变 | 只 inner retry；已扫的 DB 不重读 |
| CON-03 | inner retry 期间链头前进 | 数字 checkpoint `H` 和 effective upper 不变 |
| CON-04 | 逆序不同 inner attempt 产生不同 FN 条数 | DB cache `Ensure(n)` 只增量扩展，exhausted 后不重查 |
| CON-05 | 首个 FN 窗口返回白名单 oversized | 指定窗口逐步缩小，结果不变；单 block/epoch 仍失败时返原错误 |
| CON-06 | 非白名单 FN 错误 | 不缩窗，立即返回原错误 |
| CON-07 | mixed boundary 首次错配、第二次收敛 | 只做一次带退避的 FN-only retry，DB scan 复用 |
| CON-08 | mixed boundary 连续两次稳定错配 | 返回 consistency error，不热循环、不提交混合 candidate |
| CON-09 | 纯 DB canonical read-set | FN checkpoint/boundary 调用数都为 0 |
| CON-10 | 纯 FN canonical read-set | 有 before/after，boundary 调用数为 0 |
| CON-11 | DB 日志 + FN assumption/Guard 查询 | 最终 usage 为 mixed，仍做 boundary |
| CON-12 | stale outcome 产生后 `v1` 改变 | 先 outer retry，不返回旧 generation 的 stale |
| CON-13 | 确定性 invalid cursor | 直接返回，不开启不必要的 fence retry |
| CON-14 | `before=A, after=B` | 必须重试或报错，不提交 A/B 混合结果 |
| CON-15 | `A→B→A` | 只记录为已知限制；不声称必然检出，不用该条件判失败 |

### T5：Core 路线 B 专项

| ID | 场景 | 必须断言 |
|---|---|---|
| CFX-B-01 | mixed 页 DB 分界已给出 BN | FN lower bound 复用分界，不额外解析 |
| CFX-B-02 | 纯 FN 首页 | 只解析所需的前一 pivot/endpoint |
| CFX-B-03 | Cursor 所在 block 无匹配日志 | 保留后续 block，不把整个 Segment 误判为空 |
| CFX-B-04 | 同 epoch 多 block 正序 | 与 BN key oracle 一致，跨 block 无重复遗漏 |
| CFX-B-05 | 同 epoch 多 block 逆序 | 与 reverse oracle 一致 |
| CFX-B-06 | FN batch 被 filter/反转/截断 | `TailPosition` 始终对应响应方向最后日志的真实 BN |
| CFX-B-07 | 同 hash/epoch 被多个边界使用 | attempt summary cache 命中，不扩大为逐日志 RPC |
| CFX-B-08 | `H > fnToEpoch` | checkpoint 保护所有 FN 依赖，但不扩大日志返回范围 |

E2E 校验器对每条 Core 日志使用 blockHash 查到真实 BN，不以页内位置或 epoch 内 ordinal 推测 BN。

### T6：原始 JSON-RPC、ACL、路由和响应超限

| ID | 用例 | 通过条件 |
|---|---|---|
| API-01 | Request/Filter/Range/Cursor/Assumption 每层分别增加未知字段 | 全部拒绝，不执行查询 |
| API-02 | 十进制数、负数、错误 hex quantity、null 必填对象 | 返回 invalid params |
| API-03 | invalid cursor/params/pivot/consistency/响应超限 | 业务错误 code 统一为 `-32000`，message 可区分类别 |
| API-04 | ACL 允许的 address | 四个 scanLogs 方法按配置通过 |
| API-05 | ACL 拒绝的 address | 四个方法都在查询前拒绝，不能绕过合约白名单 |
| API-06 | 无 address 的 topic/全量查询 | 按现有 ACL 口径处理，不误报 bad params |
| API-07 | 普通节点组与 logs 节点组指向两个可区分 stub | 四个方法只命中 `GroupCfxLogs/GroupEthLogs` |
| API-08 | RPC 不配置 Store | 返回 `scan logs rpc unavailable`，进程不 panic |
| API-09 | 单独启动低 `maxGetLogsResponseBytes` 的本地实例 | 整个请求报错，不返回伪短页或部分 logs |

API-07 要同时有 middleware 单测和黑盒 stub 计数，不仅靠检查 `switch` 代码。

### T7：现有 getLogs 回归

1. 再次执行 `go test -count=1 ./...`。
2. 配置 `TEST_CFX_FULL_NODE` 和 `TEST_CFX_INFURA_NODE`，执行 `go test -count=1 ./test -run '^TestGetLogs$' -v`。
3. 对 eSpace 使用与 E2E manifest 相同的固定过滤，直接比对 Full Node 和本地 Proxy 的 `eth_getLogs`。
4. 在资源允许时，运行现有 `confura test cfx` / `confura test eth` 数据校验器至少 30 分钟，保存日志。
5. 比较 scanLogs DDL 前后的固定 `getLogs` 结果和基本耗时，确认删除旧索引没有改变结果。

任一旧 API 结果变化或新增稳定错误都是 P0 失败。

### T8：本地性能测试

性能测试使用固定数字范围和预先生成的 Cursor 链，不把数据选择、冷启动、DNS 失败或链头漂移混入稳态结果。

| Profile | 并发 | 时长/请求 | 覆盖 |
|---|---:|---:|---|
| P1 冷启动 | 1 | 每 case 5 次 | 2 Space × 2 方向 × 3 来源，limit 100 |
| P2 串行基线 | 1 | 每 case 500 页 | 四类过滤，limit 100/1000 |
| P3 常规压力 | 4 | 10 分钟 | 普通/WithPivot 按 3:1，正逆序各半 |
| P4 峰值压力 | 16 | 10 分钟 | 纯 DB/FN/mixed 各 1/3，limit 100 |
| P5 大页 | 4 | 5 分钟 | 密集过滤，limit 1000，同时观察响应字节数 |

每个 Profile 记录：

- 客户端 QPS、p50/p95/p99、超时和 JSON-RPC 错误分类；
- 页条数、响应字节数、方向、DB/FN/mixed 比例；
- MySQL query latency、rows examined/returned、慢 SQL、CPU、IO、连接数和 lock wait；
- Confura CPU、RSS、goroutine、GC，FN window/shrink、outer/inner retry、boundary mismatch、DB cache query/reuse/extend；
- Core boundary reuse/parse、cursor summary 和 TailPosition 解析次数。

建议的本地初始门禁如下，正式执行前将最终数字锁定在 manifest；如已有更严格的服务 SLO，使用 SLO：

- 所有 Profile 数据正确性错误为 0，非预期业务错误为 0；
- P2 热身后纯 DB p95 `<= 500 ms`，纯 FN/mixed p95 `<= 2500 ms`；
- P4 不出现连接池耗尽、持续 timeout 或重试风暴，p95 不高于 P2 同 case 的 3 倍；
- 满页密集查询的实际 rows examined 建议 `<= 2 * limit`；稀疏查询单独记录，但不得 filesort 或全表扫描；
- 稳定 canonical 环境中 outer/inner retry、boundary mismatch 应接近 0，持续大于 0 必须解释；
- 长时运行 RSS 不单调增长，结束时不高于热身稳态的 120%。

本地数字用于发现回归和放大效应，不直接代替 staging/生产 SLO。

### T9：staging 只读复验

只有 T0–T8 全部通过后才进入 staging。在 staging 不执行 DDL、不改 mapping、不人为制造 reorg，只执行：

1. 从 96 个基础 E2E case 中抽取只读子集，两个 Space、两种方向、四类过滤和三类来源都至少一次；
2. WithPivot 正序续页、逆序固定 Guard、空页和 stale；
3. 未知字段、未授权 address、未配置 Store 的对照实例和响应超限；
4. 固定范围 `cfx_getLogs` / `eth_getLogs` 回归；
5. 最低并发的性能 smoke，观察 30 分钟指标。

staging 每个请求都保留 method、脱敏参数、时间、实例、响应码和 trace/request ID，方便与服务端指标对齐。

## 6. 建议执行顺序与时间

| 时间 | 工作 | 当日出口 |
|---|---|---|
| Day 0 | 协议口径、环境、数据集和水位冻结 | manifest 可用，服务健康 |
| Day 1 | T0 单测/覆盖盘点，T1 DDL | 单测和两库 DDL/EXPLAIN 通过 |
| Day 2 | T2 校验器、T3 96 个基础 case | 分页与 oracle 零差异 |
| Day 3 | T4 一致性故障、T5 Core 路线 B | 事件顺序与调用次数断言通过 |
| Day 4 | T6 API/安全，T7 回归 | 无 ACL/路由/旧 API 回归 |
| Day 5 | T8 性能与稳定性 | 本地性能基线和异常分析完成 |
| staging 窗口 | T9 只读验收 | 发布 Go/No-Go 报告 |

如果校验器或故障注入用例需要新增代码，可将 Day 2–3 拆到下一 Sprint，但不将本测试任务拆成一组独立任务。

## 7. 证据和测试报告

最小交付目录：

```text
artifacts/scanlogs/<run-id>/
├── env/            # revision、版本、脱敏配置、硬件
├── unit/           # go test / race 输出
├── ddl/            # Core/eSpace plan/add/verify/drop、EXPLAIN、退出码
├── data/manifest.json
├── e2e/            # case JSON/JUnit、首个分歧证据
├── fault/          # 故障时序、调用次数、fake 日志
├── api/            # 原始 JSON-RPC、ACL、路由计数
├── regression/     # getLogs 回归
├── perf/           # 负载配置、客户端统计、服务端指标
├── staging/        # 只读验收结果
└── conclusion.md   # 结论与发布建议
```

`conclusion.md` 至少包含：

- 被测 revision/镜像 digest、环境和时间窗口；
- P0/P1 case 总数、通过、失败、invalidated 和未执行数；
- DDL 物理表数、索引数和 EXPLAIN 结论；
- Core/eSpace oracle 条数、页数和 96 个基础 case 结论；
- fault injection 的 outer/inner/cache/boundary 调用次数摘要；
- 性能 p50/p95/p99、QPS、rows examined、超时和资源峰值；
- 已知限制，特别是 FN ABA 非保证项；
- 未解决问题、风险所有者、解决日期和回滚建议；
- 最终 `GO`、`CONDITIONAL GO` 或 `NO-GO` 结论。

## 8. Go/No-Go 标准

以下条件全部满足才能给出 `GO`：

1. Core/eSpace DDL 全流程、幂等性和失败退出行为通过。
2. 所有存量物理表索引正确；8 个逻辑 EXPLAIN 及各物理路由都命中预期索引且无 filesort。
3. 96 个基础 E2E case 全通过，拼接结果与固定 oracle 完全相等，重复/遗漏/非单调均为 0。
4. Cursor、Limit、Range、空页和 Pivot 边界用例全通过。
5. 所有可观察 DB/FN/boundary 变化都会重试或报错，不提交混合 candidate；DB cache/checkpoint/缩窗次数符合断言。
6. Core 同 epoch 多 block 正逆序、边界复用和 TailPosition 通过真实 BN 交叉校验。
7. 未知字段、默认 `-32000`、ACL、logs 组路由和响应超限全通过，不存在静默短页。
8. 现有 cfx/eth getLogs 无功能回归，性能没有无法解释的显著退化。
9. 本地性能门禁和 staging 只读验收通过。
10. 报告明确声明 FN ABA 边界，不将 `A→B→A` 误列为必检出保证。

任一 P0 失败、未解释的稳定 boundary mismatch、ACL/路由绕过、分页重复遗漏或 getLogs 回归都直接导致 `NO-GO`。`CONDITIONAL GO` 只能用于不影响正确性、安全和一致性的 P1 问题，且必须有所有者和确定的解决日期。
