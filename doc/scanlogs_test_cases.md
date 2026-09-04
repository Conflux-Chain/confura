# scanLogs 综合测试用例规格

> 本文是测试执行的主规格。manifest 只是把本文中的固定数据参数传给执行器，不是测试设计本身。
>
> 对应总体计划：[scanLogs 本地综合测试计划](scanlogs_local_test_plan.md)。

## 1. 用例记录规范

每个自动化结果或人工记录必须包含以下字段：

| 字段 | 含义 |
|---|---|
| `caseId` | 本文定义的稳定 ID；参数化用例包含各维度值 |
| `revision` | Git revision、二进制或镜像 digest |
| `fixtureId` | 固定数据集版本及 seed |
| `canonicalView` | 数字 from/to、两端 hash、DB watermark |
| `request` | 脱敏后的完整 JSON-RPC 请求 |
| `oracle` | `getLogs` 原始结果或 fake 期望事件序列 |
| `observed` | 响应、页链、调用轨迹、SQL/EXPLAIN 或性能指标 |
| `verdict` | `PASS`、`FAIL`、`INVALIDATED`、`SKIPPED` |
| `evidence` | artifact 相对路径和首个差异位置 |

统一判定规则：

- 只有 canonical 上界在执行前后发生变化时才允许记为 `INVALIDATED`，随后必须重跑；程序错误、超时和环境配置错误不能记为 invalidated。
- P0 用例不得 `SKIPPED`。所有日志身份按 `(blockHash, transactionHash, logIndex)` 比较，顺序按真实 `(blockNumber, logIndex)` 比较。
- 正序要求 key 严格递增，逆序要求严格递减；跨页、跨 DB/FN、同 epoch 多 block 均适用。
- 每个非空页 `len(logs) <= effectiveLimit`，`nextCursor` 等于响应方向的页尾真实位置；空页 `logs=[]` 且不返回 cursor。
- 与 oracle 比较的是完整规范化日志集合，不只比较条数、hash 或 cursor。

自动化层级缩写：`UT` 单元测试，`FIT` fake 故障集成，`MIT` 真 MySQL 集成，`E2E` 本地 RPC 黑盒，`STG` staging 只读验收。

## 2. 确定性测试数据

### 2.1 基础矩阵的数据来源

基础 E2E 不强制部署 LogEmitter。同步库中已有足量日志时，可以冻结 DB watermark、数字范围、address、topic0 和两端 canonical hash，使用 Full Node `getLogs` 生成 oracle。manifest 必须声明每个数据集的最低日志数；执行器在发起 scanLogs 前重新查询 oracle，不达标立即失败，不能以低数据量结果通过门禁。

当前冻结数据见 `scripts/scanlogs-local-test-cases.json`：24 个 gate 数据集按正/逆序、普通/pivot 展开为 96 次执行，另有 1 个 Core Route B supplemental 数据集和 2 个 shared-route smoke 数据集。该选择仅在记录的 watermark 和 canonical hash 稳定时有效；换库、恢复同步或链视图变化后应重新选数并生成 manifest。

自然数据不能稳定制造 cursor 夹点、空 mapping、pruned、stale assumption、reorg、boundary mismatch、ABA、响应超限等状态。这些专项用例仍使用参数化请求、独立测试库或 fake/stub，不能等待真实链随机出现故障。

### 2.2 可选 LogEmitter 基础 fixture

Core 和 eSpace 各部署两个合约 `A/B`，定义两个 topic0 `E0/E1`。在 DB 侧和 FN 侧分别产生下表日志；每组日志分布到多个高度，并至少有一个高度包含 3 条以上日志，以覆盖同 block 不同 `logIndex`。

| 组合 | DB 侧条数 | FN 侧条数 | mixed 总数 |
|---|---:|---:|---:|
| A + E0 | 20 | 20 | 40 |
| A + E1 | 20 | 20 | 40 |
| B + E0 | 20 | 20 | 40 |
| B + E1 | 20 | 20 | 40 |

由此每个来源段都满足：无过滤 80 条、address A 40 条、topic E0 40 条、A+E0 20 条。执行过程：

1. 清空可丢弃测试库并记录 fixture seed；部署 A/B，生成 DB 侧日志。
2. 等待 sync 将这些日志全部写入 DB，暂停 sync，记录固定 DB watermark `W` 及 hash。
3. 在链上继续生成 FN 侧日志，DB 保持在 `W`；记录固定 FN upper `H` 及 hash。
4. 为纯 DB 取完全位于 `W` 之前且包含上述 DB 日志的范围；纯 FN 取 `W` 之后的范围；mixed 取横跨 `W` 且包含两侧日志的范围。
5. 测试期间不恢复 sync。每个 case 前后检查上界 hash；发生 reorg 则废弃该轮 oracle。

另准备以下专用 fixture：

| ID | 数据构造 | 用途 |
|---|---|---|
| FX-EMPTY | 不存在的 address 和 topic0 | 空结果 |
| FX-EXACT | 某过滤恰好 14 条 | `limit=7` 整页结束 |
| FX-SPARSE | 大范围内仅首、中、尾各少量日志 | 稀疏扫描和空窗口 |
| FX-CURSOR | 同一 block 内连续至少 5 个 logIndex | cursor 排他性 |
| FX-CFX-MB | Core 同一 epoch 至少 2 个 block，各有匹配日志 | Route B 和真实 BN |
| FX-MIG | shared/dedicated 表各有匹配数据，迁移前、中、后状态可切换 | migration read path |
| FX-LARGE | 单页响应可跨越配置的 byte limit | 响应超限 |

使用 fixture 时必须执行自检 SQL 和 `getLogs`，确认每个数据集的条数、最小/最大 key、同 block 分布及 DB/FN 归属；不符合最低数量时禁止开始 E2E。自然数据 manifest 执行同等的 `getLogs` 数量资格检查。

## 3. DDL、索引和 SQL 用例

| ID | 前置条件与操作 | 必须断言 | 层级 |
|---|---|---|---|
| DDL-01 | 两个干净 schema 执行 `plan` | 只输出计划，不改变 `information_schema.statistics` | MIT |
| DDL-02 | 对所有现存分区执行首次 `add` | 每表索引存在且列序、唯一性正确；命令 0 退出 | MIT |
| DDL-03 | 紧接 DDL-02 再执行 `add` | 全部 skip/无变更，定义未漂移，0 退出 | MIT |
| DDL-04 | 在可丢弃副本执行 `drop` | 只删被替代旧索引，新索引和数据均保留 | MIT |
| DDL-05 | drop 后再次 `verify` | 不依赖旧索引仍全部通过 | MIT |
| DDL-06 | 制造同名但错列序索引后 add/verify | 非 0 退出，明确报告定义冲突，不覆盖该索引 | MIT |
| DDL-07 | 缺少一个预期物理表或给错分区数 | 非 0 退出并指出表名；不得继续 drop | MIT |
| DDL-08 | add/drop 不带执行开关 | 数据库无变化，只输出待执行 SQL | MIT |
| DDL-09 | 表中保留数据执行在线 add | 行数/checksum 不变，无长时间 metadata lock；记录耗时 | MIT |
| DDL-10 | 空表 verify | 校验索引定义，明确标记 EXPLAIN skipped，不把 skip 当失败 | MIT |

`SQL-{space}-{filter}-{direction}-{table}` 是参数化用例：`space ∈ {cfx,eth}`，`filter ∈ {none,address,topic0,address_topic0}`，`direction ∈ {forward,reverse}`，`table` 为该过滤实际路由到的每张非空物理表。每例用真实存在的 cid/tid、位于数据中间的 cursor 和有限有效范围执行 EXPLAIN。

| filter | SQL 必须包含 | `key` 必须为 | 禁止项 |
|---|---|---|---|
| none | `FORCE INDEX (idx_bn_li)` | `idx_bn_li` | `Using filesort`、`key=NULL`、`access=ALL` |
| address | `FORCE INDEX (idx_cid_bn_li)` | `idx_cid_bn_li` | 同上 |
| topic0 | `FORCE INDEX (idx_tid_bn_li)` | `idx_tid_bn_li` | 同上 |
| address_topic0 | `FORCE INDEX (idx_cid_tid_bn_li)` | `idx_cid_tid_bn_li` | 同上 |

正序和逆序均须通过。shared 表、contract dedicated 表、topic dedicated 表只要在 schema 中存在且非空，都要按实际路由执行，不能抽一张表代替表族。

## 4. 基础分页正确性用例

### 4.1 96 个发布门禁用例的精确定义

用例 ID：

```text
E2E-{space}-{source}-{filter}-{direction}-{variant}
space     = cfx | eth
source    = db | fn | mixed
filter    = none | address | topic0 | address_topic0
direction = fwd | rev
variant   = plain | pivot
```

上述笛卡尔积恰好定义 96 个独立 P0 用例，不允许用一个成功请求代表同维度的其它组合。输入固定如下：

| 维度 | 输入 |
|---|---|
| db | 范围完全不高于 `W`，且使用 fixture 的 DB 侧数据 |
| fn | 范围严格高于 `W`，且使用 fixture 的 FN 侧数据 |
| mixed | `from <= W < to`，两侧均至少一页匹配日志 |
| none | 不提供 address/topics |
| address | address=A，不提供 topics |
| topic0 | `topics[0]=E0`，不提供 address |
| address_topic0 | address=A 且 `topics[0]=E0` |
| fwd/rev | `reverse=false/true` |
| plain/pivot | 调用普通方法/WithPivotAssumption 方法 |

每例使用 `limit=7` 从首页扫到结束，执行步骤和 PASS 标准相同：

1. 将 tag 预解析为数字，冻结 from/to、端点 hash 和 DB watermark。
2. 对相同数字范围调用对应 `getLogs`，规范化得到 oracle；Core 根据 blockHash 查询真实 BN。
3. 首次 scan 不传 cursor；pivot 变体的首页不传 assumption。
4. 逐页原样传递 `nextCursor`；pivot 变体从第二页开始传首页/前页协议规定的 assumption。
5. 直到短页或空页，拼接所有日志。
6. 断言页内和跨页严格单调、身份无重复、每页不超 7；非空页 cursor 为页尾真实 key，空页无 cursor。
7. fwd 拼接结果完整等于 oracle，rev 完整等于 `reverse(oracle)`，遗漏、额外、重复均为 0。
8. plain 所有页无 guard；pivot 的 guard 还必须满足第 6 节对应规则。
9. 前后上界 hash 相等。若不同，本轮为 invalidated 并重跑，而不是通过。

每例证据至少包含 `request-pages.jsonl`、`scan-normalized.json`、`oracle-normalized.json`、`diff.json` 和 canonical view。

### 4.2 分页、Cursor、Limit、Range 和空页

| ID | 前置条件/输入与步骤 | PASS 标准 | 层级 |
|---|---|---|---|
| CUR-01 | FX-CURSOR；cursor 指向同 block 中间一条真实日志，正逆序各请求 | cursor 日志不返回；分别从严格后一条/前一条开始 | E2E |
| CUR-02 | cursor 使用同 block 不存在但夹在两条日志之间的 logIndex | 返回方向上的第一条，不要求 cursor 本身存在日志 | E2E |
| CUR-03 | mixed；cursor 分别置于 W 前最后日志、W 后首日志 | 只交给唯一 owner segment；合并无重复遗漏 | E2E+FIT |
| CUR-04 | cursor 低于下界 | `-32000` invalid cursor，无 DB/FN 部分结果 | E2E |
| CUR-05 | cursor 高于冻结上界 | 同 CUR-04 | E2E |
| CUR-06 | cursor 等于范围首/尾 key，正逆序各请求 | 严格排他；到方向末端时为空页 | E2E |
| CUR-07 | 连续响应相同 cursor（stub 注入） | 校验器立即失败并报告循环页，不无限请求 | FIT |
| LIM-01 | 分别省略 limit、传 `0x0` | 均按默认 100；结果和 oracle 一致 | E2E |
| LIM-02 | `1,7,100,maxLimit` 各扫完整范围 | 每页不超 limit，完整性不变 | E2E |
| LIM-03 | `maxLimit+1` | `-32000`，查询调用数 0 | E2E+UT |
| LIM-04 | FX-EXACT，limit=7 | 两个满页包含全部 14 条；允许第三次空页，不能漏最后一页 | E2E |
| RNG-01 | 数字 from/to 最小单点范围 | 只返回该范围日志，端点包含 | E2E |
| RNG-02 | from > to | `-32000` invalid params/filter，不调用数据源 | E2E |
| RNG-03 | upper 为明确未来高度 | 按冻结协议返回错误，不静默改写 upper | E2E |
| RNG-04 | 动态 tag 请求 | 每次请求仅解析一次，后续 retry 使用相同 numeric upper | FIT |
| RNG-05 | Core 范围跨度等于服务 `max_gap` | 接受；跨度为 `max_gap+1` 时返回过滤错误 | E2E |
| EMP-01 | FX-EMPTY 普通方法，正逆序 | `logs=[]`、无 cursor、无 guard | E2E |
| EMP-02 | FX-SPARSE，多次窗口为空但后方有日志 | 不提前结束，最终结果与 oracle 一致 | E2E+FIT |
| MAP-01 | mapping 表完全为空 | 退化为纯 FN；结果与 oracle 一致 | MIT+E2E |
| MAP-02 | earliest 存在但 latest 查询缺失 | retry 后 consistency error，不当作空 Store | FIT |
| MAP-03 | 请求下界早于 earliest | pruned error；不回退 FN，不返回部分结果 | MIT+E2E |

## 5. 数据来源、迁移路由和合并

| ID | 前置条件与操作 | 必须观测/断言 | 层级 |
|---|---|---|---|
| SRC-01 | 纯 DB 四过滤正逆序 | FN checkpoint/getLogs/boundary 调用均为 0 | FIT+E2E |
| SRC-02 | 纯 FN 四过滤正逆序 | DB log scan 为 0；before/after checkpoint 各按 attempt 调用；boundary 为 0 | FIT+E2E |
| SRC-03 | mixed 四过滤正逆序 | DB/FN 两侧都有日志；boundary 对齐；合并严格单调 | FIT+E2E |
| SRC-04 | mixed 但 DB 侧无匹配、FN 有匹配 | canonical read-set 仍按实际依赖判定；不得重复或错误短路 | FIT |
| SRC-05 | mixed 但 FN 侧无匹配、DB 有匹配 | 不因 FN 空结果丢弃 DB 页；一致性检查符合实际 read-set | FIT |
| MIG-01 | FX-MIG，address 数据仅在 shared | 查询 shared，结果等于 oracle | MIT |
| MIG-02 | address 迁移中，shared/dedicated 有互补数据 | migration guard 路径合并无重复遗漏 | MIT |
| MIG-03 | address 迁移完成，数据在 dedicated | 路由 dedicated，使用 `idx_bn_li`/适用 topic 索引 | MIT |
| MIG-04 | topic0 对应 MIG-01～03 | 三状态结果均与 oracle 一致 | MIT |
| MIG-05 | address+topic0 在 address/topic dedicated 路径切换 | 过滤语义不变，索引与实际表型匹配 | MIT |

## 6. PivotAssumption 用例

| ID | 输入与步骤 | PASS 标准 | 层级 |
|---|---|---|---|
| PIV-01 | 正序首页有数据，随后带 guard 翻页 | 每页 guard 对应协议指定页尾所在 canonical pivot；数据等于 oracle | E2E |
| PIV-02 | 逆序首页有数据 | guard 对应该首页最高位置日志的 canonical pivot | E2E |
| PIV-03 | 逆序连续 3 页 | 后续 guard 与首页 assumption 固定一致，不随页面向低位漂移 | E2E |
| PIV-04 | 空首页且无 assumption | 空 logs、无 cursor，guard 按当前协议省略 | E2E |
| PIV-05 | 空页且传有效 assumption | 原样返回 guard，不制造新的位置 | E2E |
| PIV-06 | cursor 非空但不传 assumption | `-32000` missing pivot assumption，数据源调用数 0 | E2E+UT |
| PIV-07 | assumption 的 hash 被改成同高度非 canonical hash | stale/assumption failure `-32000`，不返回日志 | E2E+FIT |
| PIV-08 | assumption 高度/epoch 与 hash 不匹配 | 同 PIV-07 | E2E+FIT |
| PIV-09 | 普通 scan 首页、续页、空页 | 始终不出现 `pivotGuard` | E2E |
| PIV-10 | 先形成 stale candidate，再改变 DB generation | 先 outer retry；不得提交旧 generation 的 stale | FIT |

## 7. 一致性、重试、缓存和动态缩窗

以下均使用可编程 fake Store/FN 和 fake clock。每例保存按时间排序的事件轨迹；调用次数是 PASS 条件，不只检查最终返回值。`O` 表示 outer attempt，`I` 表示该 outer 内的 inner attempt。

| ID | 注入脚本 | 必须观测/断言 | 层级 |
|---|---|---|---|
| CON-01 | O1: DB `v0=A,v1=B`; O2: `B,B` | outer retry 1 次；O2 重建 Plan、owner、水位、DB cache；只提交 O2 | FIT |
| CON-02 | DB version 稳定；I1 FN before=A/after=B；I2=B/B | 仅 inner retry；DB scan 总计 1 次并复用；提交 I2 | FIT |
| CON-03 | 首次解析动态 upper=H；retry 时节点头变为 H+N | 所有 inner attempt 的 checkpoint/effective upper 均为数字 H | FIT |
| CON-04 | 逆序 I1/I2 要求不同 DB 补充量 | DB cache `Ensure(n)` 只增量扩展；已读行不重查；exhausted 后查询数不再增 | FIT |
| CON-05 | FN 大窗口连续返回白名单 oversized，缩小后成功 | 窗口严格缩小至成功；拼接与一次性 oracle 相同，无重复遗漏 | FIT |
| CON-06 | 单 block/epoch 仍 oversized | 停止缩窗并原样返回 oversized，不死循环 | FIT |
| CON-07 | FN 返回非白名单错误 | 立即返回原错误；窗口调用仅 1 次 | FIT |
| CON-08 | mixed boundary 第一次错配、退避后收敛 | 仅一次 FN-only boundary retry；DB scan 复用；fake clock 观察到退避 | FIT |
| CON-09 | mixed boundary 连续两次同样错配 | consistency error；不提交 candidate；无第三次热循环 | FIT |
| CON-10 | 纯 DB canonical read-set | FN before/after/boundary 全为 0 | FIT |
| CON-11 | 纯 FN canonical read-set | before/after 均执行，boundary 为 0 | FIT |
| CON-12 | DB logs + FN assumption/guard 校验 | usage 判为 mixed，执行 boundary，不能按“结果全来自 DB”跳过 | FIT |
| CON-13 | 确定性 invalid cursor | 直接错误；outer/inner retry 均为 0 | FIT |
| CON-14 | before=A, after=B 且重试不收敛 | 返回 consistency error，不提交 A/B 混合结果 | FIT |
| CON-15 | checkpoint 读取失败一次后成功/持续失败 | 仅按配置次数 retry；成功提交稳定 attempt，耗尽返回原错误 | FIT |
| CON-16 | DB outer 和 FN inner 同时发生 | inner retry 只属于当前 O；outer 切换后 inner/cache 状态全部重新建立 | FIT |
| CON-17 | FN A→B→A | 记录为已知 ABA 限制；不得把“必须检出”写入断言或发布承诺 | 文档审计 |

## 8. Core Route B 与同 epoch 多 block

| ID | 前置条件/刺激 | 必须观测/断言 | 层级 |
|---|---|---|---|
| CFX-B-01 | mixed，DB boundary 已携带真实 BN | FN lower bound 直接复用；该 boundary 不再调用 summary 解析 | FIT |
| CFX-B-02 | 纯 FN 首页，无 DB boundary | 只解析形成 lower/upper 所必需的 pivot/endpoint，不逐日志解析 | FIT |
| CFX-B-03 | cursor 所在 block 无匹配，后续 block 有匹配 | 保留后续 block；segment 不被误判为空 | FIT+E2E |
| CFX-B-04 | FX-CFX-MB 正序扫全 | 按真实 BN/logIndex 严格递增，与 oracle 完全一致 | E2E |
| CFX-B-05 | FX-CFX-MB 逆序扫全 | 严格递减，等于 reverse oracle | E2E |
| CFX-B-06 | FN batch 依次经过 filter、reverse、limit 截断 | TailPosition 始终是最终响应方向最后日志的真实 BN/logIndex | FIT |
| CFX-B-07 | 同一 hash/epoch 被 cursor、boundary、guard 重复引用 | attempt summary cache 命中；每个唯一 key 至多一次解析 | FIT |
| CFX-B-08 | checkpoint H 高于请求 `fnToEpoch` | H 用于保护所有 FN 依赖；返回日志仍不超过请求 upper | FIT |
| CFX-B-09 | 一个 epoch 的 block ordinal 与真实 BN 顺序不可替代 | 校验器只接受 blockHash→summary 得到的 BN，不用 ordinal 猜测 | E2E/校验器 UT |

## 9. JSON-RPC、ACL、路由和响应限制

| ID | 请求/环境 | PASS 标准 | 层级 |
|---|---|---|---|
| API-01A..E | 分别在 request、filter、range、cursor、assumption 加未知字段 | `-32602` 或冻结的框架 invalid-params code；查询调用为 0 | E2E |
| API-02 | quantity 使用十进制、负数、前导零非法 hex、奇数 hex、溢出 | invalid params；不 panic | E2E |
| API-03 | 必填对象传 null，数组/对象/布尔类型错置 | invalid params；不执行查询 | E2E |
| API-04 | 分别触发 invalid cursor、stale、consistency、oversized | 业务错误统一框架默认 `-32000`，message 可区分类别 | E2E |
| API-05 | ACL 允许 address，调用四个 scan 方法 | 均通过 ACL 并进入 handler | E2E |
| API-06 | ACL 拒绝 address，调用四个 scan 方法 | 查询前拒绝；普通/WithPivot、Core/eSpace 均不可绕过 | E2E |
| API-07 | 无 address 的 none/topic0 查询 | 完全遵循既定 ACL 策略，不误判为 malformed filter | E2E |
| API-08 | 普通节点组和 logs 节点组指向可区分 stub | 四个方法仅命中 CfxLogs/EthLogs 组；普通 stub 计数 0 | FIT+E2E |
| API-09 | RPC 未配置对应 Store | `scan logs rpc unavailable`，进程持续健康，无 panic | E2E |
| API-10 | FX-LARGE，降低 max response bytes | 整个请求报 oversized；不得返回伪短页、部分 logs 或 cursor | E2E |
| API-11 | JSON-RPC batch 中混合成功、非法和 scan 请求 | 每个 id 独立响应，无串扰，顺序符合框架约定 | E2E |
| API-12 | 重复 JSON key、尾随垃圾、空 params、额外 params | 按严格 JSON/方法签名拒绝；记录实际框架 code | E2E |
| API-13 | context deadline/client cancel | 请求及时结束；连接、goroutine 和 DB query 不泄漏 | E2E+UT |

## 10. getLogs 回归用例

| ID | 操作 | PASS 标准 | 层级 |
|---|---|---|---|
| REG-01 | DDL 前后对固定 Core 四过滤调用 `cfx_getLogs` | 规范化结果完全一致 | E2E |
| REG-02 | DDL 前后对固定 eSpace 四过滤调用 `eth_getLogs` | 完全一致 | E2E |
| REG-03 | 执行现有 `TestGetLogs` 和全仓测试 | 无新增失败 | UT/E2E |
| REG-04 | max-gap 边界：`to-from <= max_gap` 与超出 1 | 前者正常，后者保持既有过滤错误 | E2E |
| REG-05 | 固定数据运行旧 cfx/eth validator 至少 30 分钟 | 无稳定错误、差异或资源泄漏 | E2E |

oracle 分块必须保证每个子范围自身不超过 getLogs 的 max-gap；分块端点闭区间不得重复计入边界日志，最终按身份去重只能用于检测脚本错误，不能掩盖服务重复。

## 11. 性能与稳定性用例

所有性能 case 先完成同参数正确性验证，计时区间不生成 oracle。保存客户端、Confura、MySQL 和 FN 四侧指标。

| ID | 负载 | 数据/断言 | PASS 标准 |
|---|---|---|---|
| PERF-01 | 并发 1，每组合冷启动 5 次 | 两 Space×两方向×三来源，limit=100 | 无错误；单独报告首次与后续延迟 |
| PERF-02 | 并发 1，每 case 500 页 | 四过滤，limit=100/1000 | 纯 DB 热身后 p95≤500ms；FN/mixed p95≤2500ms |
| PERF-03 | 并发 4，10 分钟 | plain:pivot=3:1，方向各半 | 数据错误和非预期业务错误为 0 |
| PERF-04 | 并发 16，10 分钟 | DB/FN/mixed 各 1/3 | 无池耗尽/重试风暴；p95≤同 case PERF-02 的 3 倍 |
| PERF-05 | 并发 4，5 分钟 | FX-LARGE 但未超 byte limit，limit=1000 | 响应完整，无静默截断 |
| PERF-06 | 稀疏大范围 | FX-SPARSE | 无 filesort/全表扫描；单独记录 rows examined |
| PERF-07 | 稳态 60 分钟 | 混合典型流量 | RSS 无单调增长；结束≤热身稳态 120%；goroutine 回落 |
| PERF-08 | oversized/consistency 连续负向压力 | 低并发受控错误 | 无 CPU 热循环、日志洪泛、连接泄漏 |

通用门禁：正确性错误 0；非预期 timeout 0；满页密集 DB 查询 `rows_examined <= 2*limit`；稳定 view 下 outer/inner retry 和 boundary mismatch 接近 0，非 0 必须关联 trace 解释。

## 12. staging 只读验收用例

staging 不执行 DDL、不暂停 sync、不注入 reorg。选择满足数据量的固定历史范围：

| ID | 操作 | PASS 标准 |
|---|---|---|
| STG-01 | 96 基础矩阵中选最小覆盖集：每个 Space/方向/来源/过滤/variant 至少出现一次 | 所有选中 E2E 断言通过 |
| STG-02 | pivot 正序续页、逆序固定 guard、空页、stale | PIV 对应断言通过 |
| STG-03 | 未知字段、ACL 拒绝、响应超限对照环境 | API 对应断言通过，无安全绕过 |
| STG-04 | 固定范围 cfx/eth getLogs 回归 | 与 FN oracle 完全一致 |
| STG-05 | 并发 1～4 性能 smoke 30 分钟 | 无错误激增；指标不超过已批准 SLO |

每个请求保留脱敏参数、UTC 时间、实例、HTTP/JSON-RPC code、trace/request ID。staging view 变化时重新冻结范围和 oracle，不将动态链头差异归因于 scanLogs。

## 13. 当前自然数据执行口径

- 以 `scripts/scanlogs-local-test-cases.json` 为当前冻结清单，不在通用 runner 中硬编码高度、address 或 topic。
- `tier=gate` 的 24 个数据集必须全部达到各自 `qualification.minLogs`；要求整页终止语义的用例还必须满足 `exactMultipleOfPageLimit`。
- `source=db/fn/mixed` 必须与冻结 watermark 的数值关系一致；mixed 数据的 oracle 必须在水位两侧都有匹配日志。
- `tier=supplemental` 和 `tier=smoke` 提供 Route B/shared-route 证据，但不增加 96 个基础发布门禁的分母。
- 每轮归档实际 manifest、完整页链、规范化 oracle/actual、顺序校验和前后 canonical hash；任一数据资格或 hash 检查失败时该轮不得作为证据。

不得为了让自然数据“通过”而降低门禁。自然数据无法控制的独立边界和故障语义仍按本文对应 CUR/PIV/MAP/CON/API 用例补测。

## 14. 发布判定

`GO` 必须同时满足：所有 P0 用例执行且通过；96 个基础 case 零差异；所有 DDL/SQL 计划通过；一致性事件的调用顺序和次数符合断言；ACL/路由/响应超限无绕过；getLogs 无回归；性能和 staging 门禁通过。

以下任一情况直接 `NO-GO`：重复/遗漏/非单调、cursor 或 guard 错误、稳定 boundary mismatch、提交不一致 candidate、ACL/路由绕过、静默短页、错误索引/filesort、旧 getLogs 回归、任一 P0 未执行。

FN `A→B→A` 只作为已知限制写入报告，不将“必须检出 ABA”作为测试通过条件，也不得对外声称系统保证检测所有 ABA。
