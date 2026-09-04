# scanLogs 游标分页查询 —— 开发实施规划（v5）

> 依据《scanLogs 游标分页查询方案》整理的实施规划，代码位置均以当前仓库为准。
> 状态：scanLogs 开发工作已完成，进入本地综合测试与发布验收阶段。

## 0. 目标与范围

新增以下 RPC，不修改现有 `getLogs`：

```text
cfx_scanLogs
cfx_scanLogsWithPivotAssumption
eth_scanLogs
eth_scanLogsWithPivotAssumption
```

统一采用：

- 排他 Keyset Cursor：`(blockNumber, logIndex)`；
- 支持正序和逆序扫描，默认正序；
- 返回 `nextCursor`，由客户端决定是否继续请求；
- 普通 `scanLogs` 不接收 PivotAssumption，也不进行跨页 PivotGuard 校验；
- `scanLogsWithPivotAssumption` 强制由客户端提供 PivotAssumption，并进行跨页 canonical view 校验；
- Core Space 和 eSpace 使用一致的 Params、Cursor、PivotAssumption、PivotGuard 和方向语义；
- 对 Cursor owner placement 后仍 eligible 的 Segment，正序按 DB→Full Node、逆序按 Full Node→DB 顺序消费；
- 使用 DB `reorgVersion`、FN 固定高度 checkpoint 前后校验以及混合页 DB/FN 边界 hash 对齐，检测请求期间可观察到的 canonical view 变化并重试；该机制是乐观校验，不是事务或快照，无法检测极小概率的 FN `A→B→A`（ABA），详见 §6.9；
- 服务端无状态，不保存 session、分区、表名或其他扫描状态。

复杂过滤以及 `blockHash/blockHashes` 查询继续使用原 `getLogs`。

**note**：`scanLogs` 系列不是标准 RPC 方法，若不写入对外公开文档（`doc/RPC_FEATURES.md`），无人知晓因此无需功能开关；上线节奏靠部署顺序控制（见阶段 6）。

---

## 1. 现状调研结论

### 1.1 getLogs 三层结构（scanLogs 复用骨架）

```text
rpc/cfx_api.go:271 GetLogs
  → rpc/log_filter.go     ParseLogFilterType / NormalizeLogFilter / ValidateLogFilter
  → rpc/handler/cfx_logs.go:54  CfxLogsApiHandler.GetLogs   ← reorgVersion 重试循环
      → getLogsReorgGuard:128   splitLogFilter（DB/FN 切分）→ ms.GetLogs + cfx.GetLogs
  → store/mysql/store.go:329    MysqlStore.GetLogs          ← 按 filter 路由到不同表
```

`rpc/handler/eth_logs.go:32` 是 eSpace 的同构实现。scanLogs 复用三层职责（API 解析校验 → Handler 编排 → Store 查询）和既有 FN/Store 能力，但不复用 getLogs 的 `splitLogFilter` 或单层 reorg guard：任务 #3 使用独立 Plan Builder、顺序 Segment Runner 和两层一致性重试。Store 层也**不能复用 `store.LogFilter`**：它没有 cursor/limit/direction，且 `find()`（`store/mysql/store_log_filter.go:222`）硬编码 `ORDER BY bn ASC` + `LIMIT MaxLogLimit+1` + bound checks。address/topic 等谓词在请求内保持不可变，只拆分 space-native range。

### 1.2 日志表路由与现有索引

| filter | 表 | 现有索引 |
|---|---|---|
| 无 address 无 topic0 | `logs_<i>`（bn 分区，`store_log.go`） | `idx_bn (bn)` |
| 单 address | `addr_logs_<hash%N>`（`store_log_addr.go`） | `idx_cid_bn (cid,bn)` |
| 单 address + topic0 | 同上 | `idx_cid_tid_bn (cid,tid,bn)` |
| 单 topic0 | `topic_logs_<hash%N>`（`store_log_topic.go`） | `idx_tid_bn (tid,bn)` |
| 大合约 | `clogs_<cid>_<i>`（`store_log_big_contract.go`） | `idx_bn (bn)`、`idx_tid_bn (tid,bn)` |
| 大 topic | `tlogs_<tid>_<i>`（`store_log_big_topic.go`） | `idx_bn (bn)` |

分区形态两类：

- **bn 分区表**：`logs_*`、`clogs_*`、`tlogs_*` —— 按 `bn_partitions` 元数据（`bnPartitionedStore`）路由，`searchPartitions`（`store_common_partition_bn.go:168`）返回升序、index 连续、bn 范围不重叠的分区列表；低于覆盖下界返回 `store.ErrAlreadyPruned`；逆序扫描直接倒序遍历其返回值。
- **哈希分区表**：`addr_logs_*`、`topic_logs_*` —— 按 address/topic 哈希分表（每 address/topic 一张表），无 bn 分区遍历，单表查询。

大合约/大 topic 的 shared→dedicated 迁移竞态由 `store/mysql/store_log_migration_reader.go` 的 `optimisticMigrationLogReader.readEach:102` 处理（查 shared 前后各校验一次 `isMigrationCompleted`），**scanLogs 必须复用这个协议**。

### 1.3 不能直接复用的差距

| 差距 | 位置 | 后果 |
|---|---|---|
| 无 `(bn, log_index)` 复合索引 | `store_log.go:22`、`store_log_addr.go:18`、`store_log_topic.go:17` 等 | 无法稳定 keyset 排序 |
| 无存量动态分表自动迁移 | `store/mysql/config.go:92`（初始化只建新库表） | 存量 `logs_*`/`addr_logs_*`/`topic_logs_*`/`clogs_*`/`tlogs_*` 需 ops DDL |
| 方法级 Full Node 路由只认 getLogs | `server_middleware.go:142-186` | 新方法不补 case 会走 `GroupCfxHttp/GroupEthHttp` 普通节点组（绕过 logs 组） |
| ACL 只认 getLogs | `util/acl/validator.go:233/303` | 新方法漏配会绕过合约白名单 |

### 1.4 一致性原语（全部复用，不新造）

- `confStore.GetReorgVersion()/createOrUpdateReorgVersion(dbTx)`（`store/mysql/store_conf.go:91/105`），在 `MysqlStore.Popn` 的同一事务内递增（`store/mysql/store.go:317`）。
- `epochBlockMapStore`（`store/mysql/store_map_epoch_block.go`）：`EarliestBlockMapping/LatestBlockMapping`（→ dbMinBn/dbMaxBn/dbMaxEpoch）、`BlockMapping`（DB Segment 端点的精确 epoch↔bn 转换）、`PivotHash(epoch)`。
- **eSpace 的 `epoch_block_map` 里 `epoch == blockNumber`、`pivot_hash == blockHash`**（`store/eth_data_adapter.go:14-19`），所以 `PivotHash(bn)` 就是 eSpace 的 canonical hash——两 space 的 canonical 校验统一到一个原语。
- 混合 read-set 的 DB/FN 边界身份：Core 用 DB `PivotHash(dbMaxEpoch)` 对比 FN 同 epoch pivot hash；eSpace 用 DB `PivotHash(dbMaxBn)` 对比 FN 同 block hash。边界对齐只证明两个稳定读视图在分界点一致，不能替代 FN checkpoint 前后检查，也不能消除 ABA。
- tag 解析：`util.ConvertToNumberedEpoch`（Core）、`util.NormalizeEthBlockNumber`（eSpace），见 `rpc/log_filter.go:151,83`。
- FN 校验用的 SDK 方法均已存在于 `sdk.ClientOperator`：`GetBlockSummaryByEpoch`（epoch pivot hash/BN 边界）、`GetBlockSummaryByBlockNumber`（Cursor BN→hash/epoch）、`GetBlockSummaryByHash`（Core TailPosition）。eSpace 用 `client.RpcEthClient.BlockByNumber`。

### 1.5 错误码机制（统一使用框架默认值）

`github.com/openweb3/go-rpc-provider`（v0.3.7）在 `json.go` 的 `errorMessage()` 中通过 `ErrorCode() int` 接口识别自定义错误码；普通 Go error 统一使用框架默认错误码 `-32000`。

**结论：scanLogs 不定制业务错误码。Handler 使用普通 error，并通过错误消息和 `errors.Is/As` 保留内部分类；JSON-RPC 响应统一由框架映射到默认错误码 `-32000`。业务分类必须是最外层表现，具体失败原因作为 cause，消息固定为 `category: cause`（例如 `invalid scan logs params: missing pivot assumption`），不能用 `errors.WithMessage(category, cause)` 反向包装。协议级 parse error、method not found 等仍沿用框架自身的标准错误码。**

---

## 2. 设计要点与对方案的偏离

1. **Filter 可选字段用指针**：`Address *cfxaddress.Address`、`Topic0 *types.Hash`、`EpochRange *CfxEpochRange`（内部端点为 `*types.Epoch`），而非方案的「值类型 + 全零值」。`cfxaddress.Address` / `types.Hash` 的零值判定易错；指针 + `omitempty` 的 JSON 线上表现与方案一致（字段缺省即未设置），且能区分「显式发送零值（= 按该值过滤）」与「未设置」。
2. **Core 外层冻结 epoch、FN attempt 内完整归一为 BN（路线 B）**：公开请求、DB 水位拆分和 checkpoint 保持 epoch 语义；DB 子范围通过稳定 mapping 转成 block range。只有实际执行的 FN Segment 在 `anchor-before/after` 内解析为 block range，且每次 inner retry 重建，不能把 FN canonical 边界缓存进 outer generation。
3. **Core Space PivotGuard 的 epoch 直接取自日志行**：`store.Log.Epoch` / `types.Log.EpochNumber` 都带 epoch，无需 bn→epoch 反查。
4. **Core Space FN 段不补逐日志 bn/blockID 字段**：物理 `FromBlock/ToBlock` 窗口保证全局 BN 边界；Cursor 只对当前 canonical cursorHash 比较 LogIndex，其他 hash 已由物理范围保证位于正确一侧。Core log 本身没有 block number，只为 batch 末尾 `TailPosition` 做一次 hash→BN 解析，不构造逐日志 `scanEntry`。
5. **scanLogs 全程关闭现有 getLogs bound checks**：不调用 `validateQuerySetSize/validateCount/suggestBlockRange`，**不会返回 `SuggestedFilterOversizedError`**。`limit` 严格约束 DB SQL 和最终响应量，但 FN 单窗口可能先返回多于 remaining 的日志再由本地截断；FN 工作量由 space-native 窗口、结果过大缩窗和请求总超时控制。
6. **不接 pruned 归档节点回退**：DB 段 `store.ErrAlreadyPruned` 直接透出（与 getLogs 不同，通过内部文档说明）。
7. **页面最多两个严格不重叠的 Segment**：正序为 DB→FN，逆序为 FN→DB；Planner 每个 outer generation 只调用一次 `classifyCursorOwner` 完成 owner placement，完整 Cursor 不得原样传给两个 Segment。Runner 按方向顺序消费，不做跨来源 merge/sort 或运行时去重；Segment 不重叠由 Planner 保证，Full Node 日志顺序与唯一性由 RPC 契约保证。
8. **`LIMIT limit` 不加 1**：`NextCursor != nil` 只表示下一次请求应该使用的位置，不承诺存在下一页；客户端靠 `len(logs) == effectiveLimit` 判断是否继续。实现严禁用 `limit+1` 探测下一页。
9. **FN numeric checkpoint 固定为最高 FN 依赖**：RPC normalization 先把动态 tag 和显式未来上界解析/截断成冻结的 effective numeric range；`H` 同时覆盖 FN Segment、FN PivotAssumption 以及 NextCursor/PivotGuard 所需的全部 canonical 查询，并在同一 outer generation 的 inner retries 中保持数值不变。Handler 不再自行读取活动 latest。每次 inner attempt 重新读取 H 的 before hash，但开始后不得再向上扩展 H。
10. **DB 与 FN 使用两层重试**：outer DB generation 固定 `v0`、水位、Plan 和可增量扩展的 DB 缓存；inner FN attempt 重建 FN view。FN hash 变化且 DB version 未变时只重试 FN，避免 FN 频繁 reorg 导致重复 DB 查询。
11. **仅实际同时依赖 DB+FN 时做边界 hash 对齐**：Core 比较 DB 水位 epoch 的 pivot hash，eSpace 比较 DB 水位 block hash。空结果查询也属于 canonical read-set；纯 DB 或纯 FN canonical read-set 跳过边界检查。
12. **一致性保证明确接受 FN ABA 容错**：固定高度 hash 双读可发现跨越 before/after 且最终停在另一视图的 reorg，不能发现中途 `A→B→A`。confirmed/safe 只降低 reorg/ABA 概率；finalized 在正常共识 finality 假设下防止普通 canonical reorg，但仍不是多 RPC 原子快照。严格消除一次 attempt 的 ABA 需要节点提供不可变 view token/原子范围查询。
13. **不设功能开关**：scanLogs 非标准 RPC、不公开文档，天然低曝光；上线节奏靠部署顺序控制（阶段 6）。

---

## 3. 阶段 0：协议冻结（编码前）

产出：四个 RPC 的完整 request/response JSON 示例、错误码表、正/逆序/空页/stale cursor 示例；与 SDK 维护方和调用方对线，确认无歧义后再进入编码。

| # | 冻结项 | 建议 |
|---|---|---|
| F1 | 数值编码 | `blockNumber / logIndex / limit / epochNumber` 全部使用 JSON-RPC hex quantity（如 `"0x3e8"`），Go 类型用 `hexutil.Uint64`。与 `cfx_getLogs/eth_getLogs` 风格一致（方案中的普通 `uint64` 会序列化成十进制） |
| F2 | Range JSON 结构 | `"epochRange": {"fromEpoch": "0x1", "toEpoch": "latest_state"}`；`"blockRange": {"fromBlock": "0x1", "toBlock": "latest"}`。缺省端点沿用 getLogs：Core 为 `latest_state`，eSpace 为 `latest` |
| F3 | 严格字段校验 | Params / Filter / PivotAssumption 使用自定义 `UnmarshalJSON` 拒绝未知字段。Go 默认反序列化会静默忽略 `blockHash`、多 topic 等未定义字段，导致客户端误以为过滤生效 |
| F4 | Limit 与重试 | `defaultLimit = 100`、`maxLimit = 1000`。**响应字节超限时返回错误，不能静默少返回**，否则破坏 `len(logs) < limit` 的结束协议 |
| F5 | 错误码 | `invalid cursor` / `invalid params` / `pivot assumption failed`（stale 语义）不定制错误码，统一由 JSON-RPC 框架返回默认错误码 `-32000`；调用方通过错误消息区分业务类别 |
| F6 | 边界范围 | 不支持 `includeTraceLogs` 合成日志；存在 DB 水位且请求早于 DB 最早水位时返回现有 pruned 错误；Store 尚无 earliest mapping（mapping 表为空）时，当前请求按纯 Full Node range 执行，不猜测 DB 覆盖范围；earliest 已存在而 latest 缺失属于跨 generation 读取或 Store 一致性错误，经过 `v0/v1` 后重试/报错；Handler 构造契约保证 Store 已配置，未配置应在服务初始化阶段失败，不进入请求路径；WithPivotAssumption 在日志非空或已提供 assumption 时返回 pivotGuard，首个空页且未提供 assumption 时省略；FN 窗口结果集过大时动态缩窗，单 block/epoch 仍失败返回 FN 原始错误 |
| F7 | 零值语义 | 指针方案：字段缺省 = 未设置；显式零地址 / 零 hash = 按该值过滤 |
| F8 | Cursor 与 effective range | Cursor 必须落在 RPC normalization 后的实际可读 canonical 范围内；高于冻结 effective upper 的未来 Cursor 统一返回 invalid cursor，不把它解释为正序空页或逆序整段重扫 |

### 3.1 请求示例

```json
// cfx_scanLogs（正序，续页）
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "cfx_scanLogs",
  "params": [{
    "filter": {
      "epochRange": { "fromEpoch": "0x5f5e100", "toEpoch": "latest_state" },
      "address": "cfx:type.contract:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "topic0": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    },
    "limit": "0x64",
    "cursor": { "blockNumber": "0x1e8480", "logIndex": "0x5" }
  }]
}
```

```json
// eth_scanLogsWithPivotAssumption（逆序，第二参数为 PivotAssumption）
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "eth_scanLogsWithPivotAssumption",
  "params": [
    {
      "filter": { "blockRange": { "fromBlock": "0x0", "toBlock": "latest" } },
      "limit": "0x64",
      "reverse": true
    },
    { "blockNumber": "0x1e8480", "blockHash": "0x..." }
  ]
}
```

### 3.2 响应示例

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "logs": [ /* 与对应空间 getLogs 的日志结构一致 */ ],
    "nextCursor": { "blockNumber": "0x1e8481", "logIndex": "0x3" },
    "pivotGuard": { "epochNumber": "0x...", "pivotBlockHash": "0x..." }
  }
}
```

结果字段规则：

```text
xxx_scanLogs 成功：pivotGuard = nil（JSON 中省略）
xxx_scanLogsWithPivotAssumption 成功：日志非空或输入 assumption 存在时 pivotGuard != nil；首个空页且无 assumption 时省略
logs 非空：nextCursor = 本页最后一条日志的 key
logs 为空：nextCursor = nil
```

---

## 4. 阶段 1：索引前置（生产耗时最长的关键前置）

### 4.1 model tag 改动（只影响新建表）

| 模型 | 新增索引 |
|---|---|
| `log`（`store_log.go:22`） | `idx_bn_li (bn, log_index)` |
| `AddressIndexedLog`（`store_log_addr.go:18`） | `idx_cid_bn_li (cid, bn, log_index)`、`idx_cid_tid_bn_li (cid, tid, bn, log_index)` |
| `TopicIndexedLog`（`store_log_topic.go:17`） | `idx_tid_bn_li (tid, bn, log_index)` |
| `contractLog`（`store_log_big_contract.go`） | `idx_bn_li (bn, log_index)`、`idx_tid_bn_li (tid, bn, log_index)` |
| `topicLog`（`store_log_big_topic.go`） | `idx_bn_li (bn, log_index)` |

清理删除旧索引（新复合索引是其前缀超集，原代码会走覆盖索引，不影响线上现有业务）。

### 4.2 存量表 DDL 脚本

新增 `script/scanlogs-index-ddl.sh`，根据 bn_partitions 以及配置项 AddressIndexedLogPartitions / TopicIndexedLogPartitions 枚举需要处理的存量分区，并确认对应物理分区表实际存在。

脚本负责为存量表创建新的 `(bn, log_index)` 复合索引，并在所有目标分区的新索引创建完成后删除旧 bn 索引：

```sql
ALTER TABLE logs_12
  ADD INDEX idx_bn_li (bn, log_index),
  ALGORITHM=INPLACE,
  LOCK=NONE;

ALTER TABLE logs_12
  DROP INDEX idx_bn,
  ALGORITHM=INPLACE,
  LOCK=NONE;
...
```

要求：

- DDL 使用 ALGORITHM=INPLACE, LOCK=NONE，不允许降级为阻塞 DDL；
- 脚本可重复执行：执行前检查索引是否存在及其列定义，已满足目标状态的操作直接跳过；
- 分两个阶段执行：先确保所有存量分区均已正确创建 idx_bn_li，全部成功后再删除旧 idx_bn；
- 对四类查询路由分别执行正序、逆序 EXPLAIN，共 8 组查询，确认 key = idx_bn_li，且 Extra 中不存在 Using filesort；
- 任一分区 DDL 或 EXPLAIN 验证失败时脚本返回非零退出码；
- 所有存量分区 DDL 及验证完成后，方可部署依赖新索引的 scanLogs 代码（见阶段 6）。

---

## 5. 阶段 2：存储层 Keyset Scan

### 5.1 类型定义

新增 `store/scan_log_filter.go`（与 `store.LogFilter` 平级，不改动现有类型）：

```go
// Store 内部物理过滤器；不向上泄漏到 Handler/RPC。
type ScanLogFilter struct {
    BlockFrom, BlockTo uint64
    Contract string
    Topic0   string
}

// store/scan_log_filter.go —— 排他 keyset cursor
type ScanCursor struct{ BlockNumber, LogIndex uint64 }

// store/scan_log_filter.go —— 扫描控制，与 RPC 层 Params 镜像
type ScanLogParams struct {
    Filter  ScanLogFilter
    Cursor  *ScanCursor  // nil 表示第一页
    Reverse bool
    Limit   int
}
```

`reverse`、`limit`、`cursor` 归属 RPC Request。`store.ScanLogFilter` 只属于 DB 执行层，不能被 Handler 当作通用请求模型。Core 明确区分 `CfxScanLogRequest` 与 `CfxScanLogParams`：前者是包含 epoch tag 的 JSON-RPC 入参，后者嵌入 Request 并额外携带 RPC 已归一化、冻结和 cap 后的 `types.RangeUint64`。Handler 只接收 Params，不再解析 `types.Epoch` 或重复执行 nil、符号位、溢出检查。Handler 在执行 DB Segment 时才通过 `cursor.toStoreCursor()` 转成 Store Cursor；Store Cursor 回到公开结果时使用 `newScanCursor`。执行 FN Segment 时才把公开 Filter 转成 SDK `LogFilter` / `FilterQuery`。

### 5.2 SQL 构造器

新增 `store/mysql/store_scan_log_filter.go`（对应方案的 §7.1/§7.2）：

```go
type scanLogFilter struct {
    TableName          string
    BlockFrom, BlockTo uint64
    ContractID         *uint64 // cid = ?
    Topic0ID           *uint64 // tid = ?（PrimaryIDTopicSchema 系表）
    Topic0Hash         *string // topic0 = ?（universal 表）
}
```

其他 `Cursor *store.ScanCursor`、`Reverse bool`、`Limit int` 可以通过函数参数传入参数中而非作为其成员变量。

谓词顺序：等值列（cid / tid / topic0）→ `bn BETWEEN ? AND ?` → cursor 排他谓词（正序 `(bn > ? OR (bn = ? AND log_index > ?))`，逆序 `<`）→ `ORDER BY bn ASC, log_index ASC`（逆序 `DESC, DESC`）→ `LIMIT ?`。**不做任何 bound check / query set 估算。**

### 5.3 路由与分区遍历

新增 `store/mysql/store_scan_log.go`：

```go
func (ms *MysqlStore[T]) ScanLogs(ctx context.Context, params store.ScanLogParams) ([]*store.Log, error)
```

路由逻辑镜参考 `MysqlStore.GetLogs`（`store/mysql/store.go:329`）：`Contract` → 合约系表、`Topic0` → topic 系表、都无 → universal 表。

分区遍历统一抽成辅助函数（**仅适用于 bn 分区表**；哈希分区表单表查询，无需遍历）：

```go
// 按方向遍历 searchPartitions 的结果，逐分区累计到 limit 即停止
func scanPartitions(ctx context.Context, partitions []*bnPartition, params store.ScanLogParams,
    query func(ctx context.Context, tableName string, remaining int) ([]*store.Log, error)) ([]*store.Log, error)
```

- 正序升序遍历；逆序倒序遍历。
- cursor 剪枝：正序跳过 `BnMax < cursor.BlockNumber` 的分区；逆序跳过 `BnMin > cursor.BlockNumber` 的分区（分区内部分由 SQL cursor 谓词排除）。
- 每个分区把 `[BlockFrom, BlockTo]` 与分区 bn 范围求交，`remaining = limit - len(collected)` 递减。
- 每个分区查询前做 `ctx.Done()` 检查（沿用 `store_log.go:190` 的写法）。

### 5.4 各表 scan 入口（与现有 `Get*Logs` 同文件并列）

| 表类型 | 方法 | 文件 |
|---|---|---|
| universal（bn 分区） | `logStore.ScanLogs` | `store_log.go` |
| address 索引（哈希分区） | `AddressIndexedLogStore.ScanAddressIndexedLogs`（topic0 先 `normalizeTopicsToIDs`） | `store_log_addr.go` |
| 大合约（bn 分区） | `bigContractLogStore.ScanContractLogs` | `store_log_big_contract.go` |
| topic 索引（哈希分区） | `TopicIndexedLogStore.ScanTopicIndexedLogs` | `store_log_topic.go` |
| 大 topic（bn 分区） | `bigTopicLogStore.ScanTopicLogs` | `store_log_big_topic.go` |

### 5.5 迁移守卫重构

把 `store_log_migration_reader.go:102 readEach` 的迁移守卫抽成泛型辅助函数供两条路径共用，避免重复实现该竞态协议：

```go
func readWithMigrationGuard[R any](ctx context.Context, isCompleted func() (bool, error),
    queryShared, queryDedicated func() (R, error)) (R, error)
```

现有 `readEach` 改为调用它；scan 路径用同一个函数包住 `ScanAddressIndexedLogs / ScanContractLogs`（以及 topic 的一对）。

该阶段完成后，纯 DB 查询应能独立满足严格排序、排他游标和 limit 语义。

---

## 6. 阶段 3：DB/FN 编排与一致性

### 6.1 分层结构与原生日志类型

- `rpc/handler/scan_logs.go`：两条链共享的 Plan Builder、BN Cursor 裁剪、block-window walker、DB cache 和一致性协调工具。
- `rpc/handler/cfx_scan_logs.go`：Core 公开类型、epoch-native Planner、FN attempt/summary cache、epoch→BN 物化、Reader、DB/FN Runner 与一致性编排。
- `rpc/handler/eth_scan_logs.go`：eSpace 公开类型、block-native Planner、Reader、DB/FN Runner 与一致性编排。

共享层只处理“一个页面最多两个有序 Segment”，不把两个 space 的日志抹平成 `[]*store.Log`：

- Core candidate 始终持有 `[]types.Log`；
- eSpace candidate 始终持有 `[]web3Types.Log`；
- DB 返回的 `*store.Log` 在各 space 的 DB reader 边界立即转换为对应原生类型；
- 不定义逐日志 `scanEntry`，不增加 `blockID/blockNumber` 辅助字段；
- 每个 Segment batch 只额外携带响应方向最后一个位置 `TailPosition *store.ScanCursor`。

DB cache 还保留与原生日志等长的私有 `keys []store.ScanCursor` sidecar。它不是响应字段，也不携带 FN view/hash：一是 Core 原生 `types.Log` 没有 BlockNumber，Store row 转换后无法从日志恢复 `(bn, logIndex)`；二是逆序 FN retry 可能改变本轮实际消费的 DB 前缀长度，必须用 `keys[n-1]` 得到当前 candidate 的尾 Cursor，而不能误用之前已缓存更长前缀的末尾。该 sidecar 只存在于 outer-generation DB cache，不重新引入逐日志 `scanEntry`。

`TailPosition` 是完成 Cursor 过滤、逆序反转和 remaining 截断后，batch 响应方向最后一条日志的 `(BlockNumber, LogIndex)`；它不包含 source/epoch/hash，也不是公开 `NextCursor`。页面完成全部 Segment 后，Coordinator 才选择 Plan 顺序中最后一个实际贡献日志的非空 batch 的 TailPosition 作为 `NextCursor`；所有 batch 均为空时为 nil。

### 6.2 范围冻结、Segment Plan 与 Cursor placement

RPC normalization 在进入 Handler 前一次性解析动态 tag，并把显式未来上界按节点语义截断为冻结的 effective numeric range；外层重试只重建 DB 水位和 Segment Plan，不重新解释 tag，也不让 effective upper 随 head 前进。Handler 必须信任这个冻结范围，不能再次调用 Core `GetStatus(latest_state)` 或 eSpace `latest`，否则 `latest_confirmed/safe/finalized` 请求会被错误地重新关联到更高的活动 head。

**Core Space 外层保持 epoch 坐标，FN 执行层使用 block 坐标**：

```text
requestedEpochs = [fromEpoch, toEpoch]
dbEpochs = requestedEpochs ∩ [dbMinEpoch, dbMaxEpoch]
fnEpochs = effectiveRequestedEpochs ∩ [dbMaxEpoch + 1, effectiveToEpoch]
```

- 请求低于 DB 最早覆盖水位仍返回 `store.ErrAlreadyPruned`，不静默裁掉低端。
- 只将非空 `dbEpochs` 的两个端点经 `BlockMapping` 精确转换成 DB block range；既然该 Segment 已由 DB 水位证明为非空覆盖，任一端点缺少 mapping、mapping 自身的 block range 无效或两个端点的 block range 逆序都属于存储一致性错误，不能静默当成空 Segment。
- `fnEpochs` 只保存在 outer generation；实际执行 FN Segment 时，在 checkpoint-before 之后物化为 BN。混合页下界可直接用 `dbMaxBn+1`，上界在 `fnToEpoch == H` 时复用 checkpoint pivot BN；纯 FN 起点用前一 epoch pivot BN+1，正序续页下界直接由 cursor.bn 替代。所有边界都只缓存于本次 inner attempt。
- 显式 `toEpoch` 高于活动链头时，由 RPC normalization 在调用 Handler 前完成 cap；若 cap 后 `from > to`，表示合法的未来空范围（原始范围合法性必须在 cap 前校验），不能再作为反向范围错误处理。

**eSpace 使用 block 坐标**：RPC 先冻结 effective block range，Handler 再按 `dbMaxBn` 切成不重叠 DB/FN block range。

mapping 表为空（`EarliestBlockMapping` 不存在）时，Planner 无法证明任何 DB 覆盖区间，因此不得猜测 DB/FN 分界；它生成一个覆盖 RPC 已冻结 effective range 的纯 FN Plan。earliest/latest 来自同一张表，稳定状态下应同时存在；已经读到 earliest 后 latest 却不存在，属于两次非事务读取跨越 Store generation 或 Store 不一致，必须经过 `v0/v1` 判定后重试/报错，不能伪装成普通空 Store。纯 FN 退化不会漏数或把未知 DB 状态当成已裁剪，只是放弃本次请求的 DB 加速，并允许 Full Node 自己返回历史不可用等错误。Handler 构造契约要求 Store 必须存在；未配置属于服务初始化错误，不在 `ScanLogs` 请求路径中处理。若 mapping 已存在而请求低于明确的 DB 最早水位，仍返回 `store.ErrAlreadyPruned`：这个分支表达的是已知的保留策略，而不是 mapping 尚未就绪。Core/eSpace Cursor 高于 RPC 冻结的 effective upper 时统一返回 invalid cursor。

Plan Builder 最多生成两个步骤，且已经按返回方向排列：

```text
无 Cursor：      forward DB → FN；reverse FN → DB
Cursor 属于 DB：forward DB(cursor) → FN(nil)；reverse DB(cursor)，跳过 FN
Cursor 属于 FN：forward FN(cursor)，跳过 DB；reverse FN(cursor) → DB(nil)
```

Planner 统一通过注释完整的 `classifyCursorOwner` 完成一次 Cursor 合法性检查和 owner 判定；该函数不修改 Cursor。完整请求 Cursor 绝不能原样传给两个 Segment。`cursor.bn <= dbMaxBn` 只能判定候选 DB owner，不能代替请求范围校验：DB owner 必须同时落在 DB block Segment；候选 FN Cursor 在 inner attempt 中通过 BN summary 取得 `cursorEpoch/cursorHash`，验证 epoch/BN 均落在物化前后的 FN Segment，再把物理首窗裁到 cursor.bn。FN canonical metadata 不能缓存在 outer plan 中。

### 6.3 顺序 Segment Runner 与 outer DB 缓存

Runner 只维护 `remaining = effectiveLimit - len(collected)`，按 Plan 顺序调用 Segment：

- 正序 DB→FN：DB 已填满 limit 时完全不打开 FN view；DB 不足时才读取 FN 剩余数量。
- 逆序 FN→DB：FN 已填满 limit 时完全不访问 DB；FN 不足时才调用 DB cache 的 `Ensure(remaining)`。
- Segment 已按不重叠范围和方向排列，Runner 只做 append，不做跨来源 merge/sort。
- `len(batch.Logs) < remaining` 表示该 Segment 已完整耗尽；不得用窗口数上限造成静默短页。

正序 DB→FN 的 DB batch 是 outer-generation-local 固定前缀：最多查询一次，所有 FN inner retries 都直接复用，不能按“重新执行完整 Plan”的字面含义重复调用 Store。

逆序页中 FN inner retry 后返回数量可能变化。DB cache 必须支持增量扩展：`Ensure(n)` 表示“缓存至少达到 n 条”的目标总量，不是本次新增量；第一次 DB 查询使用该 DB Segment 的外部排他 Cursor（通常为 nil），后续扩展使用缓存最后一条 `(bn, logIndex)` 作为内部排他 Cursor，只查询新增部分。某次扩展少于请求量时记录 `exhausted`，以后不再重复空查询。每次 inner candidate 按新的 remaining 消费缓存前缀；已有 cache 但本次 FN 已填满 limit 时不消费 DB，也不把本次 candidate 标记为 mixed；DB 查询或缓存消费即使得到空结果，也表示本页依赖 DB 的“无日志”结论，必须计入 canonical read-set。DB cache 仅在同一个 outer DB generation 内复用、最多保存 effectiveLimit 条；`v0/v1` 变化后全部丢弃。

### 6.4 窄 `FnSegmentReader` 边界

语义接口保持为单 Segment、单方法；实际代码使用 Core/eSpace 两个具体实现，不强行为不同日志/范围类型制造复杂 union：

```go
type fnSegmentReader[L any] interface {
    Scan(ctx context.Context, remaining int) (fnSegmentBatch[L], error)
}

type fnSegmentBatch[L any] struct {
    Logs         []L
    TailPosition *store.ScanCursor
}
```

返回契约为：`len(Logs) == 0` 当且仅当 `TailPosition == nil`；非空时 TailPosition 必须严格等于响应序最后一条日志的 key；`len(Logs) < remaining` 必须表示 Segment 确实耗尽。窗口数/缩窗次数由 Reader 通过注入的 metrics recorder 直接记录，不为可观测性临时扩大 correctness batch 接口。

Reader 在每次 inner attempt 中使用新的 FN view，并接收不可变的 block-native Segment spec（范围、address/topic、方向、可选 owner Cursor、窗口配置）。同一个 spec 必须可从头重放；canonical retry 后不能沿用上次窗口进度、BN 边界或 Core cursor metadata。

Reader 只负责：

- 在一个已经规划好的 FN Segment 内按方向遍历窗口；
- 应用该 Segment 独占的排他 Cursor；
- 识别白名单内的结果集过大错误并缩窗；
- 按 Full Node RPC 契约直接消费窗口结果；selector identity、日志范围、顺序和唯一性不在 Handler 内逐字段重复校验；
- 返回原生日志以及 batch 级 `TailPosition`。

Reader 不负责：DB/FN 拆分、Cursor owner、页面总 limit、DB cache、FN checkpoint 前后读、canonical retry、DB/FN 边界 hash、PivotAssumption/PivotGuard、最终 `NextCursor` 或 RPC 序列化。特别不能由 Reader 自己关闭 FN checkpoint，否则 TailPosition、NextCursor、PivotGuard 或边界辅助查询可能落到校验窗口之外。

eSpace FN Cursor 不额外查询 `BlockByNumber(cursor.bn)`：首个物理窗口直接以 cursor.bn 为闭区间边界，只对该 BN 的日志应用排他 logIndex。固定的更高 checkpoint `H >= cursor.bn` 已在整个 attempt 前后读取；在排除已接受 ABA 的 canonical ancestry 模型下，稳定的 H hash 同时固定低位 cursor block。Core 仍查询一次 cursor BN summary，因为 Core 原生 Log 没有 BlockNumber，必须取得 cursorHash 才能识别首窗中的 cursor block；这不是第二道 fence。

### 6.5 Core 路线 B：FN Segment 完整 BN 归一

Core 的业务范围仍以 epoch 冻结和拆分，但实际进入 Reader 前，在每个 inner attempt 中将真正会执行的 FN Segment 物化为一个 inclusive BN range。物化、Cursor summary、日志、TailPosition、Guard 和 boundary 全部位于同一 `anchor-before(H)` / `anchor-after(H)` 之间；inner retry 必须重新物化，不能复用旧 view 的边界。

端点按实际方向/Cursor 惰性解析，避免无意义 RPC：

| 场景 | `fnFromBn` | `fnToBn` | 不含共同 checkpoint 的准备 RPC |
|---|---|---|---|
| 跨 DB/FN、无 Cursor | `dbMaxBn + 1` | `pivotBn(fnToEpoch)`，通常复用 before(H) | 通常 0 |
| 纯 FN 首页 | `pivotBn(fnFromEpoch-1) + 1`（epoch 0 为 0） | `pivotBn(fnToEpoch)`，通常复用 before(H) | 通常 1 |
| 正序 FN Cursor | `cursor.bn` | `pivotBn(fnToEpoch)` | Cursor summary 通常 1 |
| 逆序 FN Cursor | 边界同上（跨水位免费，纯 FN 查前一 pivot） | `cursor.bn` | 混合通常 1，纯 FN 通常 2 |

当 PivotAssumption 高于日志范围导致 `H > fnToEpoch` 时，before(H) 的 pivot BN 不能冒充 FN 查询上界，必须通过 attempt cache 单独查询 `pivotBn(fnToEpoch)`。`pivotBn(fnFromEpoch-1)+1` 依赖 Core blockNumber 是全局执行序坐标、epoch pivot 是该 epoch 最后一块；即使节点留下数值空洞，多查询不存在的 BN 也不会混入前一 epoch 日志。所有 `+1` 必须做 uint64 溢出保护。

FN owner Cursor 的处理固定为：

1. 在任何 BN lookup 前要求 `cursor.bn <= checkpointPivotBn`，避免查询 checkpoint 保护范围之外的数据。
2. `GetBlockSummaryByBlockNumber(cursor.bn)` 得到当前 attempt 的 `cursorHash/cursorEpoch`；返回 summary 与 selector 的 identity 由 Full Node RPC 契约保证，Handler 只判断 cursor epoch/最终物化 BN range 是否属于请求 Segment。
3. 正序物理查询从 cursor.bn 开始，逆序物理查询到 cursor.bn 结束；Cursor block 仍包含在首窗中。
4. 首窗只对 `log.BlockHash == cursorHash` 的日志应用严格 `LogIndex > cursor.li`（正序）或 `< cursor.li`（逆序）；其他 hash 已由物理 BN 边界保证在正确一侧，一律保留。cursor block 对当前过滤没有日志时不需要查第一条日志的 BN，也不需要 ordinal fallback。

两 space 共用 block-window walker，负责正逆序窗口推进、结果过大缩窗、完整窗口成功后反转、remaining 截断和“只在首窗应用 Cursor”。Core Reader 仅负责构造 `FromBlock/ToBlock`、应用 cursorHash/LogIndex 排他条件，以及在截断完成后用最终日志的 blockHash 解析一个 `TailPosition`。Core 原生 Log 无 BN，因此无法在不逐块 RPC 的前提下逐条验证数值窗口；这里明确依赖节点的 `cfx_getLogs` block-range、顺序和唯一性契约，不为每条日志增加 BN 或 blockID，也不做结果驱动的重复验真。

### 6.6 outer DB generation 与 inner FN fence

一致性编排分为两层：

```text
outer DB generation
  v0 → DB 水位/mapping → DB assumption → Plan → DB cache

  inner FN attempt（按需懒开启）
    预计算固定 checkpoint H
    → anchor-before(H)
    → 按 Plan 构造完整 candidate
    → FN assumption / TailPosition / NextCursor / PivotGuard 辅助查询
    → 根据最终 canonical read-set 做必要的 DB/FN boundary hash check
    → 形成 provisional outcome（success / canonical-dependent error / boundary mismatch）
    → anchor-after(H)

  v1 → 统一 commit gate → 发布 success 或稳定 error
```

具体规则：

1. `v0` 必须早于 DB 水位、mapping、DB assumption 和任何 DB scan；每个 provisional outcome 最终都必须读取 `v1`，`v1 != v0` 时丢弃 DB cache、success/error outcome 和 Plan，重启 outer generation。
2. 只有实际发生 FN canonical read 时才打开 inner attempt；空的 FN 日志结果同样算一次 FN 依赖。
3. 第一次 FN canonical read 前，预先计算最高依赖 `H`：Core 取最高相关 epoch，eSpace 取最高相关 block；它至少覆盖实际 FN Segment 上界、落在 FN 的 PivotAssumption，以及 Core 候选 FN Cursor 的可读上界验证。`H` 在该 outer generation 的全部 inner retries 中保持同一个数值，不随 latest/head 前进；NextCursor/PivotGuard 只能引用不高于这些已知依赖的日志或 assumption，因此不得在 after 前动态抬高 H。
4. 请求上界高于 FN latest 时，RPC normalization 必须先把实际范围截到冻结的 numeric latest，再交给 Handler；这既保持“未来上界不报错”的节点行为，也防止 `getLogs` 在 attempt 期间读到 `H` 以上新产生的数据。Handler 不允许重新读取 latest 或扩展该范围。
5. Core `anchor-before/after` 读取 `H` epoch 的 pivot hash；eSpace 读取 `H` block 的 canonical hash。Core anchor summary 同时可复用其 pivot BlockNumber，但 PivotGuard 只使用 epoch number + pivot block hash，不需要通过 `GetBlockSummaryByBlockNumber` 反查 epoch。
6. 日志、Core Cursor metadata、FN assumption、batch TailPosition、输出 Guard 和边界 hash 等所有 FN canonical 查询必须位于 before/after 之间；先完成所有可能引入 DB/FN view usage 的日志和辅助查询，再根据最终 canonical read-set 决定 boundary，最后才能读取 after。
7. success、stale assumption 等 canonical-dependent outcome 以及 boundary mismatch 共用同一个 commit gate：先闭合适用的 FN after，再读取 DB `v1`。`anchor-after != anchor-before` 时丢弃 provisional outcome；此时若 DB version 未变则复用 DB cache 重试 inner，已变则重启 outer。after 稳定但 `v1 != v0` 也必须重启 outer；只有两层均稳定后才允许发布 success、返回稳定 error，或处理稳定 boundary mismatch。Cursor 与冻结 numeric range 不相交属于确定性的请求错误，直接返回 `invalid cursor`，不进入 fence 或 retry。
8. inner retry 形态参考现有 getLogs 的超时检查，但不包含 suggestion retry；上下文超时不返回部分结果。
9. stale assumption 等依赖 canonical view 的错误不得在中途直接返回；应保存为 provisional error 并通过上述 after + v1 commit gate。Cursor block 高于 checkpoint 或不属于请求 epoch/block range 时直接返回 `invalid cursor`，不得因 checkpoint/hash 变化重试。Full Node 方法成功返回时，selector identity（按 hash/epoch/BN 返回对应区块）以及 getLogs 的范围、顺序和唯一性视为节点 RPC 契约，Handler 不逐字段重复验真；节点兼容性由集成/E2E 测试验证。普通网络错误和请求总超时可直接返回。

### 6.7 DB/FN 边界 hash 对齐

边界对齐仅在本页的 canonical read-set 实际同时依赖 DB 和 FN 时执行，不按最终日志是否非空判断：执行过的空 Segment、DB/FN assumption、Cursor 解析、TailPosition 和 PivotGuard 查询都算依赖；只规划但因 limit 已满而未执行的 Segment 不算。

- Core：比较 DB `PivotHash(dbMaxEpoch)` 与 FN 在 `dbMaxEpoch` 的 canonical pivot hash。
- eSpace：比较 DB `PivotHash(dbMaxBn)` 与 FN 在 `dbMaxBn` 的 canonical block hash。

该查询必须放在全部其他可能引入 view usage 的查询之后、FN `anchor-after` 之前，并在每次 inner retry 重做。boundary mismatch 先作为 provisional outcome 保存，仍需读取 anchor-after：after 已变化时按普通 FN reorg 丢弃 outcome 并 inner retry；只有 after 稳定时才按以下规则裁决：

- DB version 已变化：立即重启 outer generation；
- DB version 未变化：DB 当前索引的是 `latest_confirmed`，DB 自身 reorg/ABA 概率远低于 latest FN，因此先退避并仅重放一次 FN、复用 DB cache；如果边界仍稳定错配，可能是 DB 仍保存 reorg 前已确认 view、尚未随 indexer 前进。此时继续重放 FN 无法收敛，必须立即返回明确 consistency error，由调用方稍后重试，不能在 inner loop 中一直等到请求总超时。每轮仍检查 DB version，一旦变化再重启 outer；不得返回 DB 链 A + FN 链 B 的混合 candidate。

纯 DB canonical read-set 跳过 FN checkpoint 和 boundary；纯 FN canonical read-set 执行 checkpoint、跳过 boundary。来源按最终 canonical usage 判定，例如 DB 日志 + FN assumption 仍是 mixed，不能按“日志来自 DB”跳过。

### 6.8 PivotAssumption、NextCursor 与 PivotGuard

输入 PivotAssumption 在所属 attempt 中只做一次精确校验，但由相应一致性层保护：

- assumption 高度在 DB 水位内：每个 outer generation 用 DB `PivotHash()` 校验一次，由 `v0/v1` 覆盖，不做第二次相同查询；
- assumption 高度在 DB 水位外：纳入固定 checkpoint `H`，每个 inner attempt 在 before/after 之间校验一次；即使 assumption 高于日志查询范围，也由 `H >= assumption` 覆盖，不再额外二次校验。这里依赖 canonical ancestry 不变量：在排除 ABA 的模型下，稳定的 H hash/pivot 身份固定其低位 canonical 祖先；若 assumption 高于其他 FN 依赖，它自身就成为 H；
- 不一致时记录 `ErrScanLogsAssumptionFailure` provisional outcome，经适用的 after 和最终 v1 均稳定后才返回；临时无法验证时返回底层错误，不得降级为无校验。

页面候选日志完成后、`anchor-after` 之前生成 `NextCursor/PivotGuard` 所需的所有 canonical 数据；只有 FN after 与 DB v1 均通过后才发布结果：

- 正序：logs 非空时 Guard 取最后一条日志；logs 为空时原样转换已校验的 assumption；
- 逆序：第一页且 logs 非空时 Guard 取第一条日志，固定已覆盖的最高 checkpoint；续页或空页原样回传 assumption；
- Core Guard 使用日志的 `EpochNumber` 和该 epoch 的 canonical pivot hash；
- eSpace Guard 使用日志的 `BlockNumber/BlockHash`；
- 普通 scan 不返回 Guard；WithPivotAssumption 在日志非空或已提供 assumption 时返回 Guard，首个空页且无 assumption 时省略。

### 6.9 FN 窗口、错误识别与 ABA 边界

FN Reader 使用 space-native 窗口：Core 按 epoch，eSpace 按 block。窗口初始大小使用配置值，按方向从低到高或从高到低遍历，直到达到 remaining limit 或确认整个 Segment 耗尽。

仅对生产 Full Node 已确认的“结果集过大”错误执行缩窗，识别规则采用稳定错误码 + 精确消息白名单；不得对所有 FN 错误盲目缩窗。窗口可缩到单 epoch/block，仍失败时返回 FN 原始错误，第一版不做 receipt 兜底。白名单必须以当前生产 Core/eSpace 节点的实际错误样本固化测试。

inner FN fence 是乐观一致性校验，不是事务、原子锁或快照，也不应实现成包揽 Planner/DB/Pivot 的大型 `canonicalAttempt` 类型。代码必须包含等价于以下内容的详细注释：

```text
固定高度 hash 的 before/after 校验可以发现 A→B 且最终停在 B 的 reorg，
但无法发现查询期间发生、收尾前又回到原视图的 A→B→A（ABA）。
在该极小概率场景中，before/after 和 PivotGuard 可能属于 A，
而 GetLogs 或辅助查询曾读取 B。DB/FN boundary hash 对齐具有同样限制。
这是当前无节点 snapshot/view token 条件下明确接受的容错范围。
```

因此验收保证必须表述为：**在 FN attempt 期间不存在未被双读发现的 ABA，且节点对固定数字范围返回一致结果的前提下，页面不会混合可观察到的 canonical view；所有可观察到的 hash/version 变化都会触发重试或错误。**

希望降低风险的调用方应显式选择 Core `latest_confirmed` 或 eSpace `safe`，它们只降低 reorg/ABA 概率而不消除；Core `latest_finalized` / eSpace `finalized` 在正常共识 finality 假设下防止普通 canonical reorg，但多次 RPC 仍不构成节点原子快照。不能在服务端把默认 latest 请求静默截到这些高度，否则短页可能被客户端误判为扫描结束。严格防止一次多 RPC attempt 的 ABA，长期仍需节点提供不可变 view token 或带 pivot/view assumption 的原子范围分页 RPC；JSON-RPC batch 不是原子快照，不能替代 before/after。

---

## 7. 阶段 4：RPC、配置和外围接入

### 7.1 公开类型

公开类型直接定义在 `rpc/handler/scan_logs.go`、`cfx_scan_logs.go` 和 `eth_scan_logs.go`，数字字段全部 `hexutil.Uint64`。RPC API 已单向依赖 Handler，因此直接复用这些类型，不再创建 `rpc/scanlogs_types.go` 或中间 DTO：

```go
type ScanLogCursor struct {
    BlockNumber hexutil.Uint64 `json:"blockNumber"`
    LogIndex    hexutil.Uint64 `json:"logIndex"`
}

// Core Space
type CfxEpochRange struct {
    From *types.Epoch `json:"fromEpoch,omitempty"`
    To   *types.Epoch `json:"toEpoch,omitempty"`
}

type CfxScanLogFilter struct {
    EpochRange *CfxEpochRange      `json:"epochRange,omitempty"`
    Address    *cfxaddress.Address `json:"address,omitempty"`
    Topic0     *types.Hash         `json:"topic0,omitempty"`
}

type CfxScanLogRequest struct {
    Filter  CfxScanLogFilter `json:"filter"`
    Limit   hexutil.Uint64   `json:"limit"`
    Cursor  *ScanLogCursor   `json:"cursor,omitempty"`
    Reverse bool             `json:"reverse,omitempty"`
}

// 仅供 RPC normalization 后传入 Handler，不直接参与 JSON 解码。
type CfxScanLogParams struct {
    *CfxScanLogRequest
    EpochRange citypes.RangeUint64
}

type CfxPivotAssumption struct {
    EpochNumber    hexutil.Uint64 `json:"epochNumber"`
    PivotBlockHash types.Hash     `json:"pivotBlockHash"`
}
type CfxPivotGuard CfxPivotAssumption

type CfxScanLogResult struct {
    Logs       []types.Log     `json:"logs"`
    NextCursor *ScanLogCursor  `json:"nextCursor,omitempty"`
    PivotGuard *CfxPivotGuard  `json:"pivotGuard,omitempty"`
}
```

eSpace 对称：`EthScanLogFilter{BlockRange, Address *common.Address, Topic0 *common.Hash}`、`EthPivotAssumption{BlockNumber, BlockHash}` 等。

**所有 JSON-RPC 请求类型实现严格 `UnmarshalJSON`（拒绝未知字段，F3）**：`CfxScanLogRequest`、Filter、Range 和 PivotAssumption 先解码到 `map[string]json.RawMessage` 校验键白名单，再解码到内部 shadow 结构。Handler-only 的 `CfxScanLogParams` 不参与 JSON 解码，无需实现 `UnmarshalJSON`。

### 7.2 RPC 方法

在 `rpc/cfx_api.go` / `rpc/eth_api.go` 新增方法：

```go
func (api *cfxAPI) ScanLogs(ctx context.Context, req CfxScanLogRequest) (*CfxScanLogResult, error)
func (api *cfxAPI) ScanLogsWithPivotAssumption(
    ctx context.Context, req CfxScanLogRequest, a CfxPivotAssumption) (*CfxScanLogResult, error)
```

go-rpc-provider 按方法名自动映射为 `cfx_scanLogs` / `cfx_scanLogsWithPivotAssumption`（ethAPI 同理），无需改 `rpc/apis.go`，方法挂在已注册的 `cfx` / `eth` namespace 上。

各方法体：

1. 参数校验（`limit <= maxLimit`、filter 组合只允许方案 §2.4 的四种、range 上下界顺序、WithPivotAssumption 的 assumption 结构合法）。
2. 范围冻结：EpochRange / BlockRange 缺省时用默认范围；tag 用 `util.ConvertToNumberedEpoch` / `util.NormalizeEthBlockNumber` 解析成数字，**在进入 handler 的 attempt 循环之前解析一次，循环内不再解析**。Core RPC 将 `CfxScanLogRequest` 转成 `CfxScanLogParams{CfxScanLogRequest: &req, EpochRange: normalizedRange}` 后传给 Handler；Handler 不再读取 `req.Filter.EpochRange` 做二次转换。eSpace 传冻结的 block range。
3. RPC normalization 一次性取得所需的 Core `latest_state/latest_confirmed/latest_finalized` 或 eSpace `latest/safe/finalized` 数字，并对显式未来上界完成 cap；随后把冻结的 effective range 原样传给 Handler。全部 outer/inner retries 复用该范围，Handler 不再访问活动 latest；纯 DB 请求也不会因 Handler 规划产生额外 FN head RPC。
4. Core API 只把 tag 归一化成 numeric epoch range，不把完整 epoch range 转成 BN。Handler 的 outer Plan 按 DB epoch 水位切分后，只物化实际非空 DB epoch Segment：`fromMapping := BlockMapping(dbFromEpoch)`、`toMapping := BlockMapping(dbToEpoch)`；必须精确取得两个端点，并验证各自的 block range 有效且端点间没有逆序，最终只查询 `[fromMapping.BnMin, toMapping.BnMax]`。条件不满足一律返回存储一致性错误，不能静默当空或查询到 Segment 外。FN Segment 保持 epoch range。
5. address / topic0 归一化成 store 层字符串（Core 用 base32、eSpace 用 `addr.String()`，与 `store.ParseCfxLogFilter / ParseEthLogFilter` 一致）。
6. 调 handler；结果用现有的 `uniformCfxLogs / uniformEthLogs` 归一（保证 `logs` 序列化成 `[]` 而非 `null`）。
7. 响应字节超限（沿用 `maxGetLogsResponseBytes`）→ 返回错误，不静默截断，提示用户减少 limit 绕过问题。

### 7.3 错误定义

Handler 使用普通 error 定义业务分类，不实现 `ErrorCode()`。上下文包装必须保留 `Unwrap()` 链，供服务内部通过 `errors.Is/As` 判断；对外统一使用 JSON-RPC 框架默认错误码 `-32000`：

```go
var (
    ErrScanLogsInvalidCursor = errors.New("invalid scan logs cursor")
    ErrScanLogsInvalidParams = errors.New("invalid scan logs params")
    ErrScanLogsAssumptionFailure = errors.New("pivot assumption failed")
)
```

分类错误使用统一的 `scanLogsError{category, cause}` 构造：`Error()` 输出 `category: cause`，`Is()` 匹配分类 sentinel，`Unwrap()` / `Cause()` 返回具体原因。`canonicalDependentError` 只标记错误必须经过 canonical fence 后才能发布，其内部仍采用相同的分类错误结构。已经分类的错误在向上传播时不得再被普通 `errors.WithMessage` 包到外层；普通 Store、Full Node、超时和编码错误仍按原方式补充上下文。典型消息包括：

```text
scan logs rpc unavailable: api handler not configured
invalid scan logs params: missing pivot assumption
invalid scan logs cursor: cursor is outside scan range
pivot assumption failed: expected pivot ... got ...
inconsistent canonical views: mixed boundary mismatch after 1 retry
```

### 7.4 方法级路由补齐（关键，易漏）

`rpc/server_middleware.go:142-186` 的 `getEthClientFromProviderWithContext` / `getCfxClientFromProviderWithContext` 按 method 选择节点组。必须新增：

```go
case rpcMethodCfxScanLogs, rpcMethodCfxScanLogsWithPivotAssumption:
    grp = node.GroupCfxLogs
// eth 同理 → node.GroupEthLogs
```

并新增 `isCfxScanLogsRpcMethod` / `isEthScanLogsRpcMethod` 助手与四个 `rpcMethod*` 常量。漏掉会走 `GroupCfxHttp / GroupEthHttp` 普通节点组。

### 7.5 ACL

`util/acl/validator.go:233/303` 的 `cntAddrParsers` 补四个新方法名 → 新 parser（从 scan filter 里取单个 address）。漏掉会让受合约白名单限制的 key 通过 scanLogs 绕过限制。

### 7.6 配置

`rpc/handler/cfx_logs.go:29 MustInitFromViper` 扩展读取（同一处初始化，避免新增 init 入口）：

```yaml
requestControl:
  scanLogs:
    defaultLimit: 100          # Limit == 0 时使用
    maxLimit: 1000             # 超过则 invalid params
    fullnodeWindowSize: 1000    # FN 单窗口大小：Core 单位为 epoch，eSpace 单位为 block
```

同步在 `config/config.yml` 的 `requestControl` 注释块补带说明的示例。

### 7.7 指标

复用 `metrics.Registry.RPC` / `metrics.Registry.Store`，至少覆盖：

```text
scanLogs 方向（正序/逆序）
有效 limit 分布
返回条数
数据来源（纯 DB / 纯 FN / DB+FN 混合）
FN 窗口数
FN 缩窗次数
DB outer retry 次数（db version）
FN inner retry 次数（checkpoint）
DB/FN boundary mismatch / convergence timeout 次数
正序 DB batch 复用、逆序 DB cache 查询/扩展/复用次数
Plan Segment 数量、Cursor owner 和实际 canonical view usage
Core FN 边界复用/解析 RPC、Cursor summary 次数
Core TailPosition BN 解析次数和失败次数
stale cursor 次数
耗时
```

---

## 8. 阶段 5：测试与验收

### 8.1 测试矩阵

| 维度 | 场景 |
|---|---|
| Space | Core、eSpace |
| 方向 | 正序、逆序 |
| 来源 | 纯 DB、纯 FN、跨 DB/FN 水位 |
| 过滤 | 无过滤、address、topic0、address+topic0 |
| Cursor | 第一页、同区块续页、跨区块、DB/FN 水位边界、请求超范围、高于 frozen latest、DB/FN owner、outer retry 后 owner 变化 |
| 分区 | 单分区、跨分区、大合约、大 topic、迁移状态切换 |
| Range | 数字、动态 Tag、缺省、`to > latest`、inner retry 期间 head 前进但冻结上界不变 |
| Pivot | 正常、stale、空页、逆序 guard 固定 |
| Core 路线 B | 混合页免费边界、纯 FN 首页、正/逆序 Cursor 惰性端点、`H > fnToEpoch`、epoch 0、cursor block 无匹配日志、过滤条件变化、同 epoch 多 block 正逆序、summary cache |
| 一致性 | V0/V1 变化、FN checkpoint 变化、纯 DB/FN read-set 跳过、辅助查询最后引入 mixed usage、boundary 对齐/错配、统一 outcome commit gate、两层 retry、重试耗尽 |
| DB cache | 正序固定前缀跨 inner retry 复用；逆序 FN 数量变化、Ensure 目标总量、exhausted、outer retry 失效、旧 cache 存在但当前 FN 填满时仍为纯 FN usage |
| FN 异常 | 错误码+消息白名单命中后缩窗、非白名单错误不缩窗、单 block/epoch 仍失败 |
| 一致性边界 | `anchor-before=A、anchor-after=B` 的跨 fence A→B 必须检测；before/after 同为 A 的 A→B→A 记录为当前模型不保证检测的容错范围 |
| 协议 | 未知字段被拒（F3）、字节超限报错 |
| Store readiness | 无 earliest/latest mapping、非空 DB epoch Segment 缺失有效 mapping；Store 未配置由服务初始化测试覆盖，不属于 Handler 请求测试 |

### 8.2 核心验收标准

1. 在固定 canonical view 的稳定测试环境中，将所有分页结果拼接后与全量 oracle 完全一致：全程无重复、无遗漏，排序严格单调。
2. 每页 `len(logs) <= effectiveLimit`；非空页 `nextCursor` 永远等于本页最后一条日志的 key；空页为 `nil`。
3. 普通 scan 不返回 pivotGuard；WithPivot 在日志非空或已提供 assumption 时返回正确 guard，首个空页且无 assumption 时省略。
4. 在不包含 checkpoint ABA 的故障模型下，任何 DB version 变化、FN checkpoint 最终变化或混合 read-set 的 boundary 不一致都必须阻止 candidate 提交；系统只能重试、在 fence 稳定后返回 `stale cursor`，或返回一致性/超时错误。测试不把 FN `A→B→A` 设为必检出条件，该情形按 §6.9 作为已接受限制记录。
5. scanLogs 业务错误不实现 `ErrorCode()`，对外统一返回框架默认错误码 `-32000`；消息以业务分类开头并遵循 `category: cause`，`errors.Is` 命中分类，`errors.Cause` 到达具体原因。
6. 现有 cfx/eth getLogs 回归测试全部不变。
7. EXPLAIN 证明四类过滤正/逆序均无 filesort，实际 rows examined 接近 limit。
8. 端到端校验器逐页扫到底 vs getLogs 比对；并交叉验证 Core 的 cursor.blockNumber 与 `GetBlockSummaryByHash` 真实 bn 一致。
9. FN checkpoint 变化且 DB version 未变时，Store scan 不得重复；DB version 变化时 Plan、Cursor owner 和 DB cache 必须全部重建。
10. 所有 FN assumption、Core cursor、TailPosition、NextCursor、PivotGuard 和 boundary canonical 查询必须发生在同一个固定 `H` 的 before/after 之间；所有 DB mapping/Guard 查询必须早于 `v1`。
11. success 与 canonical-dependent error 使用同一 after + v1 commit gate；模拟 stale 已产生但 `v1` 随后变化时必须 outer retry，不得返回旧 generation 的错误；确定性的 invalid cursor 必须直接返回且不触发 fence retry。

### 8.3 测试落地

- **store 层单测**（sqlite in-memory + gorm AutoMigrate，模式照 `store/mysql/store_conf_test.go`）：
  - `store_scan_log_filter_test.go`：正/逆序 SQL 的 cursor 排他性（同 bn 内按 log_index 切分）、limit 生效、等值谓词组合。
  - `store_scan_log_test.go`：跨分区累计到 limit 即停、分区 cursor 剪枝、逆序倒序遍历分区、`ErrAlreadyPruned` 透出。可直接复用 `searchPartitionsFromMetadata`（`store_common_partition_bn.go:190`）构造分区元数据，无需真实建表。
  - 迁移守卫：`readWithMigrationGuard` 重构不改变现有 `store_log_migration_reader_test.go` 的行为，再补 scan 路径用例。
- **Plan Builder 纯函数单测**：0/1/2 Segment、正逆序顺序、DB/FN Cursor owner、只有首 Segment 携带 Cursor、Core 仅 DB 子段转 BN、FN upper cap、future Cursor、空 mapping/not-ready、非空 DB Segment mapping 异常、outer 水位前移后重新 placement。
- **FnSegmentReader/Bounds 单测**：Core 路线 B 的混合免费边界、纯 FN 前一 pivot 边界、正/逆序 Cursor 惰性端点、checkpoint/to 复用、`H > fnToEpoch`、cursor block 无匹配日志、同 epoch 多 block、inner retry 重建 attempt metadata、`cursor.bn > checkpointPivotBn` 时不做 BN lookup、TailPosition 在过滤/反转/截断后常数次解析；eSpace block Cursor；两 space 的 `TailPosition nil iff empty`、short iff exhausted、结果过大白名单缩窗以及非白名单错误不缩窗。Full Node selector identity、日志范围/顺序/唯一性不做重复单元校验，由节点契约及 E2E 验证。
- **Runner 与 DB cache 单测**：直接 append、remaining 递减、前段填满跳过后段、空段 usage、TailPosition 覆盖；正序固定 DB batch 在多次 FN retry 中 Store 只查一次；逆序 `Ensure(n)`、exhausted 后不重复空查询、FN 数量变化时增量扩展、旧 cache 存在但当前 FN 填满时 usage 仍为纯 FN、outer retry 时失效。
- **一致性事件顺序单测**（窄 client + fake clock）：FN checkpoint 变化只 inner retry、DB version 变化 outer retry、stable boundary mismatch 带退避 inner retry，且 Store 只查一次、纯 read-set 跳过、DB assumption 只查一次、FN assumption 被最高 H 覆盖、辅助查询最后引入 mixed 后仍执行 boundary、boundary 位于全部 usage 查询之后、stale 与 success 共用 after+v1 gate、invalid cursor 不进入 fence，以及所有 FN 辅助查询位于 before/after 内。
- **Pivot/finalization 单测**：NextCursor 取值、PivotGuard 正序/逆序/空页规则，验证 after/v1 通过后只做无 I/O 的结果组装。
- **端到端一致性校验**：在 `cmd/test`（`test/cfx_validate.go:762` / `test/eth_validate.go:551` 已有 getLogs 校验器）旁加 scanLogs 校验器。
- **手工验证**：用 curl 按正序 / 逆序 / WithPivotAssumption / 错误码四类路径过一遍（示例见 §3.1）。

---

## 9. 阶段 6：灰度发布（纯运维节奏，无功能开关）

scanLogs 非标准 RPC 且不公开文档，不需要 `enabled` 开关；上线节奏完全由部署顺序控制：

1. 合入 model tag（新分区表自带 `(bn, log_index)` 索引）。
2. 生产存量表 online DDL（低峰逐表执行，DDL 生成器产出语句）。
3. 检查所有表索引和查询计划（EXPLAIN 四类过滤 × 正/逆序）。
4. 只读 staging 验证。
5. 单实例/小流量部署代码，先用现有 rate-limit 策略对新方法做 QPS 限流观察。
6. 再放开 `WithPivotAssumption` 变体。
7. 观察 outer/inner retry、boundary mismatch、DB cache 复用、Core BN 边界解析/Cursor summary/TailPosition、stale、缩窗、慢 SQL 和响应大小指标。
8. 全量开放。

回滚 = 下线代码，已建索引保留（无副作用）。

---

## 10. 建议的 PR 拆分与工期

| PR | 内容 | 可独立验证 | 工期 |
|---|---|---|---|
| 1 | 协议类型、严格解析、错误码、配置 | `go build ./...` | 1–2 人日 |
| 2 | 索引 model tag、DDL 生成器、存储层 keyset scan、迁移守卫重构 | `go test ./store/...` | 2–3 人日 |
| 3 | typed Plan Builder、顺序 Runner、Core/eSpace FN Reader、outer DB cache、两层 retry、boundary/Pivot 一致性 | `go test ./rpc/...` | 3–5 人日 |
| 4 | RPC 接入、路由、ACL、指标、完整测试、内部文档 | `cmd/test` 跑真实节点 | 2–3 人日 |

合计约 **8-13 人日**，不包含生产索引构建等待时间。

---

## 11. 风险与遗留

1. **索引 DDL 是最大的上线风险**：索引没建完的分区表上 scanLogs 会 filesort，大范围逆序扫可能很慢；必须先跑完 DDL 再部署代码（靠部署顺序控制，无功能开关）。
2. **Core 路线 B 依赖 block-range 契约和 BN 边界不变量**：`cfx_getLogs(FromBlock/ToBlock)` 必须按全局执行 BN 顺序返回，epoch pivot BN 是该 epoch 的执行上界，下一 epoch 的最小 BN 可由前一 pivot+1 作为安全 inclusive 下界。Core Log 不带 BN，无法逐条本地验证数值窗口；E2E 必须持续验证边界、顺序、Cursor hash 过滤和 TailPosition 的真实 BN。若节点未来改变这些契约，应回退为 `GetBlocksByEpoch(from)+首块 summary` 的范围内边界解析，而不是恢复结果驱动 ordinal 逻辑。
3. **严格字段校验的兼容影响**：拒绝未知字段可能误伤宽松客户端，阶段 0 需与 SDK 维护方对线确认。
4. **逆序 + 稀疏 FN 窗口**：逆序首页从冻结上界开始，日志稀疏时可能连续拉多个空窗口。第一版使用固定窗口并受请求总超时约束；不得因内部窗口数上限返回伪装成 Segment 耗尽的短页。如未来增加安全上限，达到上限必须返回明确错误。
5. **响应体积**：`maxLimit = 1000` 需配合「字节超限报错」的保证；`defaultLimit = 100` 保守起步。
6. **`NextCursor` 语义**：严禁用 `limit+1` 探测下一页，否则破坏「末页恰好等于 limit 时多发一次空请求」的协议。
7. **FN ABA 是已接受的一致性限制**：before/after 和 boundary 对齐都不是原子快照，无法检测同一 checkpoint 的 `A→B→A`。DB 数据来自 `latest_confirmed`，所以 DB reorg 及 DB ABA 概率比 latest FN 更低，但不是数学上的原子快照保证。该限制必须写入代码注释和运行文档；confirmed/safe 只能降低概率，finalized 在正常共识假设下提供最稳妥语义，严格解决仍依赖节点 view token/原子 RPC。
8. **DB/FN 边界可能暂时稳定错配**：DB 索引尚未追上 FN 新 canonical view 时，双方各自稳定但 boundary 不同。实现只允许一次带退避的 FN-only 重试；仍错配且 DB version 稳定时立即返回 consistency error，等待调用方在 indexer catch-up 后重试，不能热循环、等满总超时或静默拼接。
9. **嵌套重试可能放大调用量**：FN 缩窗、FN canonical inner retry、DB outer retry 共存，必须共享一个请求总超时并分别计量。outer DB cache 每个 generation 最多缓存 effectiveLimit 条，只在当前请求内复用，不得升级成跨请求缓存。
10. **Core TailPosition 有常数 RPC 成本**：每个非空 Core FN batch 至多增加一次 blockHash→BN summary 查询，逆序随后由 DB 提供页尾时可能未被最终使用；第一版优先保持单方法 Reader 契约，并通过 attempt summary cache 和指标观察成本，禁止扩展成逐日志查询。

## 12. 任务规划

### 12.1 粒度原则

共 7 条平级任务：
- 开发：4 条
- 测试：1 条
- 部署：2 条

开发任务原则上一条对应一个主 PR，但不是机械强制：
- 每个 PR 必须能够独立编译并通过其模块测试。
- 前三个开发 PR 不暴露外部 RPC，可以安全地逐层合入。
- 第四个 PR 原子完成 RPC 暴露、节点路由和 ACL。
- 如果实际编码中某个任务必须拆成多个 PR，应先确认拆分边界，并保证每个 PR 都是可编译、可验证的准备性提交。
- 不为了满足“一任务一 PR”而制造临时接口、空实现或不可独立验收的代码。
- 测试和部署任务以测试报告、执行记录和门禁结论为交付物，不对应 PR。

这样设计既保留了数据库、Store、Handler、RPC 四个清晰的开发模块，又将大量实现细节收进任务 Todo；数量足够少，也能在 Sprint 之间独立移动

### 12.2 开发任务

#### 开发 1：数据库索引与 DDL 工具
任务名：
[scanLogs][开发] 数据库复合索引与存量 DDL 工具
任务内 Todo：
- 为所有日志模型增加 (bn, log_index) 相关复合索引
- 清理被新索引覆盖的旧索引定义
- 实现存量表枚举
- 实现幂等索引检查
- 实现两阶段 ADD/DROP
- 强制 INPLACE / LOCK=NONE
- 实现正逆序 EXPLAIN 验证
- 增加新建表和脚本测试
验收重点：新建表索引正确；脚本可重复执行；失败返回非零；不允许阻塞 DDL。
原则上对应一个 PR。

#### 开发 2：Store 层 Keyset Scan
任务名：
[scanLogs][开发] Store 层 Keyset Cursor 扫描
任务内 Todo：
- 定义 ScanLogFilter、ScanCursor、ScanLogParams
- 实现正逆序 Keyset SQL
- 实现 bn 分区方向遍历和 Cursor 剪枝
- 接入 universal/address/topic 表
- 接入大合约和大 topic 表
- 重构并复用迁移守卫
- 实现 MysqlStore.ScanLogs 路由
- 覆盖分区、Cursor、Limit、迁移和 pruned 测试
验收重点：严格排序、排他 Cursor、无重复遗漏、LIMIT limit、不做 bound check、ErrAlreadyPruned 直接透出。
原则上对应一个 PR。该 PR 会相对大一些，但职责仍然统一：提供完整的 Store 扫描能力。

#### 开发 3：Core/eSpace Handler 与一致性编排
任务名：
[scanLogs][开发] Core/eSpace Handler 与 DB/FN 一致性编排
任务内 Todo：
- 定义 Core epoch request/candidate 与 eSpace block request/candidate，DB reader 边界转换原生日志
- 实现最多两个 Segment 的 typed Plan Builder，Core 只将 DB epoch 子段映射为 BN
- 实现一次 Cursor owner placement/局部裁剪，保证只有裁剪后的首 Segment 携带 Cursor
- 实现正序 DB→FN、逆序 FN→DB 的顺序 Runner，直接 append 而不做跨来源 merge/sort
- 实现 outer-attempt-local 的 DB cache：正序固定前缀跨 inner retry 复用；逆序支持 `Ensure(n)`、缓存末尾内部 Cursor 增量扩展和 exhausted 状态
- 区分 cache 已加载与当前 candidate 实际 DB usage，FN 填满时不因旧 cache 错判 mixed
- 定义窄 `FnSegmentReader` 与 batch `TailPosition` 契约
- 实现 Core FN attempt-local epoch→BN 边界物化、summary cache、H pivot BN 上界保护和 block-window Reader
- 实现 eSpace block-window Reader 和排他 `(blockNumber, logIndex)` Cursor
- 固化生产 Core/eSpace 结果集过大错误码+消息白名单，并实现动态缩窗
- 实现 outer DB generation：`v0/v1`、水位/Plan 重建和 DB cache 失效
- 实现 inner FN fence attempt：固定最高 checkpoint、before/after、FN-only retry
- 在全部辅助查询完成后，根据最终 canonical usage 实现 DB/FN boundary hash 对齐及 mismatch 分流
- 实现 DB/FN PivotAssumption 分流，确保 stale 只在对应 fence 稳定后提交
- 在 FN after 和 DB v1 之前完成 TailPosition、NextCursor、PivotGuard 所需的 canonical 查询，并让 success/error 共用 after+v1 commit gate
- 写明 FN `A→B→A` 无法检测的容错边界及 confirmed/finalized 建议
- 实现超时、canonical-dependent provisional error 以及契约化 Full Node 调用错误处理
- 覆盖 Plan、Reader、Runner、DB cache、两层 retry、boundary、Pivot 和两种方向/三种来源的单元测试
验收重点：Core/eSpace 对外分页语义一致但保持各自 native range/log 类型；对所有可观察到的 DB version、FN checkpoint 和 mixed boundary 变化均不提交 candidate；FN-only retry 不重复扫描 DB，DB version 变化必定重建 Plan 并失效 cache；普通 scan 不返回 PivotGuard，WithPivot 在日志非空或已提供 assumption 时返回 PivotGuard；明确不承诺检测 FN checkpoint 的 ABA。
原则上对应一个 PR。将公共框架、Core 和 eSpace 放在一起，可以避免为了拆分而制造不完整的中间接口，也方便同时评审两种 Space 的语义一致性。

#### 开发 4：RPC 接入及外围能力
任务名：
[scanLogs][开发] RPC 接入、访问控制与可观测性
任务内 Todo：
- 增加 Core/eSpace 公开请求和响应类型
- 实现严格 JSON 字段校验
- 定义业务错误消息和内部分类，不实现自定义 ErrorCode
- 增加 Limit、FN 窗口等配置
- 实现参数校验、动态 Tag 解析和范围冻结
- 接入四个 RPC 方法
- 实现响应体积超限保护
- 补齐方法级 Full Node logs 节点路由
- 补齐 ACL address parser
- 增加方向、来源、窗口、重试、stale、耗时等指标
- 补充内部使用说明和发布 Runbook
- 覆盖默认错误码、ACL、路由和序列化测试
验收重点：四个 RPC 可正常调用；未知字段被拒；不能绕过 ACL；使用正确的 logs 节点组；响应不被静默截断。
原则上对应一个 PR。RPC 方法、ACL 和节点路由必须原子合入，避免出现接口已经暴露但安全控制或路由尚未补齐的中间状态。

### 12.3 测试任务

任务名：
[scanLogs][测试] scanLogs 综合测试与发布验收
任务内 Todo：
- 在测试库完整演练 DDL
- 验证四类过滤正逆序共 8 组 EXPLAIN
- 验证索引命中且无 Using filesort
- 实现或运行 Core/eSpace E2E 分页校验器
- 将分页结果与固定 canonical view 下的 getLogs oracle 比对
- 验证无重复、无遗漏、严格单调
- 验证 Cursor、空页、Limit 和范围边界
- 验证 Pivot 正常、stale、逆序固定和空页
- 验证纯 DB、纯 FN 和跨水位
- 验证 DB outer / FN inner 两层 retry、固定 numeric checkpoint、DB scan 复用和动态缩窗
- 验证混合 read-set boundary 对齐、稳定错配退避和纯 DB/纯 FN canonical read-set 跳过规则
- 验证 Core 路线 B 的边界复用/解析、同 epoch 多 block 正逆序及 TailPosition
- 验证 FN ABA 限制已按文档口径处理，不把 A→B→A 设为必检出条件
- 验证未知字段、框架默认错误码、ACL 和响应超限
- 执行现有 cfx/eth getLogs 回归
- 在 staging 执行只读验收
- 输出测试结论和发布建议
各开发任务仍需随 PR 提交模块单元测试；这条测试任务负责跨模块、端到端和 staging 验收。
如果测试工作跨 Sprint，整条任务移动即可，不需要移动一组测试子任务。

本任务的本地环境、数据集、用例矩阵、执行顺序、证据留存和 Go/No-Go 标准见 [scanLogs 本地综合测试计划](./scanlogs_local_test_plan.md)。

### 12.4 部署任务

####  部署 1：生产索引准备
任务名：
[scanLogs][部署] 生产日志表索引升级
任务内 Todo：
- 确认测试库 DDL 和 EXPLAIN 已通过
- 低峰逐表执行 ADD INDEX
- 监控锁等待、复制延迟、CPU、IO 和慢 SQL
- 确认所有目标表均已创建新索引
- 全部成功后删除旧索引
- 在生产抽查 8 组 EXPLAIN
- 建立“允许部署 scanLogs 代码”的索引门禁结论
这条任务只负责数据库变更，可以比应用代码提前完成并独立跨 Sprint 流转。

#### 部署 2：应用灰度与全量发布
任务名：
[scanLogs][部署] 应用灰度、全量发布与发布后复核
任务内 Todo：
- 确认生产索引门禁通过
- 部署只读 staging 版本
- 单实例开放普通 Core/eSpace scanLogs
- 配置现有 QPS 限流
- 观察错误率、耗时、outer/inner retry、boundary mismatch、DB cache 复用、Core BN 边界解析/Cursor summary/TailPosition、慢 SQL 和响应大小
- 开放 WithPivotAssumption 变体
- 观察 stale、PivotGuard、boundary mismatch 和 canonical consistency
- 形成全量发布 go/no-go 结论
- 逐步扩大流量至全量
- 执行生产 E2E 抽样和发布后指标复核
- 异常时下线代码，保留已建索引
灰度、WithPivot 开放和全量发布是同一发布流程的连续检查点，放在一条任务的 Todo 中更合适。
