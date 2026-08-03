# Conflux 内置合约事件日志同步系统设计文档

https://shimo.im/docs/m5kvdYYNGEfJ0v3X


## 目录

- [1. 项目概述](#1-项目概述)
  - [1.1 背景](#11-背景)
  - [1.2 目标](#12-目标)
  - [1.3 术语说明](#13-术语说明)
- [2. 系统架构](#2-系统架构)
- [3. 内置合约事件定义](#3-内置合约事件定义)
  - [3.1 Staking 合约](#31-staking-合约)
  - [3.2 SponsorWhitelistControl 合约](#32-sponsorwhitelistcontrol-合约)
  - [3.3 AdminControl 合约](#33-admincontrol-合约)
- [4. 数据同步模块](#4-数据同步模块)
  - [4.1 方案选型](#41-方案选型)
  - [4.2 数据存储设计](#42-数据存储设计)
  - [4.3 Trace 同步与解析](#43-trace-同步与解析)
  - [4.4 Reorg 处理](#44-reorg-处理)
  - [4.5 Catch-Up 机制](#45-catch-up-机制)
- [5. 数据查询模块](#5-数据查询模块)
  - [5.1 方案选型](#51-方案选型)
  - [5.2 RPC 接口设计](#52-rpc-接口设计)
  - [5.3 Reorg 检测](#53-reorg-检测)
- [6. 数据聚合模块](#6-数据聚合模块)
  - [6.1 接口设计方案](#61-接口设计方案)
  - [6.2 返回策略逻辑](#62-返回策略逻辑)
  - [6.3 交互流程与一致性](#63-交互流程与一致性)

---

## 1. 项目概述

### 1.1 背景

由于 Conflux 1.0 链的历史原因，内置合约（Builtin Contracts）在执行时不产生标准的 Event Logs，导致无法通过标准的 `getLogs` RPC 接口查询这些事件。

为解决这一问题，需要设计一个系统：**通过同步链上的 Traces 数据来组装并对外提供内置合约的事件日志**。

### 1.2 目标

| 目标 | 说明 |
|:-----|:-----|
| 数据同步 | 同步并存储内置合约的 Traces 数据，组装成标准 Event Logs 格式 |
| 查询服务 | 通过扩展 `getLogs` 接口提供内置合约事件查询功能 |
| 数据一致性 | 保证数据的一致性和实时性，正确处理 Reorg 场景 |
| 高可用 | 提供高性能、可靠的同步与查询服务 |

### 1.3 术语说明

| 术语 | 说明 |
|:-----|:-----|
| 内置合约 (Builtin Contract) | Conflux 链上预部署的系统合约，如 Staking、SponsorWhitelistControl 等 |
| Trace | 交易执行过程中的调用轨迹，包含内部调用信息 |
| Epoch | Conflux 的区块确认周期 |
| Pivot Block | 每个 Epoch 中的主链区块 |
| Reorg | 区块链重组，已确认的区块被撤销 |

---

## 2. 系统架构

**核心组件：**

```text
┌───────────────────────────────┐
│        RPC Aggregator         │
│     (Confura rpc-proxy)       │
└───────────────┬───────────────┘
                │
        ┌───────┴────────┐
        ▼                ▼
┌──────────────────┐  ┌─────────────────┐
│  Confura getLogs │  │ Builtin Logs RPC│
│  (普通合约日志)    │  │ (内置合约日志)    │
└──────────────────┘  └──────┬──────────┘
                             │
                             │
    ┌───────────────┐        │
    │ Builtin Logs  │◀───────┘
    │     DB        │
    │    (MySQL)    │
    └───────┬───────┘
            │
            ▼
    ┌─────────────────┐
    │ Trace Sync      │
    │   Service       │
    │ (独立同步服务)    │
    └───────┬─────────┘
            │
            ▼
    ┌─────────────────┐
    │ Conflux Fullnode│
    │(trace_epoch RPC)│
    └─────────────────┘
```

---

## 3. 内置合约事件定义

### 3.1 Staking 合约

**合约地址**：[cfx:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaajrwuc9jnb](https://confluxscan.org/address/cfx:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaajrwuc9jnb?tab=contract-viewer)

| 函数 | 事件名 | 参数 | Event Signature |
|:-----|:-------|:-----|:----------------|
| `deposit(uint256 amount)` | `Deposit` | `user` (indexed), `amount` | `Deposit(address,uint256)` |
| `withdraw(uint256 amount)` | `Withdraw` | `user` (indexed), `amount` | `Withdraw(address,uint256)` |
| `voteLock(uint256 amount, uint256 unlockBlockNumber)` | `VoteLocked` | `user` (indexed), `amount`, `unlockBlockNumber` | `VoteLocked(address,uint256,uint256)` |

### 3.2 SponsorWhitelistControl 合约

**合约地址**：[cfx:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaaegg2r16ar](https://confluxscan.org/address/cfx:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaaegg2r16ar?tab=contract-viewer)

#### Sponsor 相关事件

| 函数 | 事件名 | 参数 | Event Signature |
|:-----|:-------|:-----|:----------------|
| `setSponsorForGas(address contractAddr, uint upperBound)` | `SponsorGas` | `sponsor` (indexed), `contractAddr` (indexed), `upperBound` | `SponsorGas(address,address,uint256)` |
| `setSponsorForCollateral(address contractAddr)` | `SponsorCollateral` | `sponsor` (indexed), `contractAddr` (indexed) | `SponsorCollateral(address,address)` |

#### 白名单管理事件

| 函数 | 事件名 | 参数 | Event Signature |
|:-----|:-------|:-----|:----------------|
| `addPrivilegeByAdmin(address contractAddr, address[] addresses)` | `WhitelistAddedByAdmin` | `admin` (indexed), `contractAddr` (indexed), `users` | `WhitelistAddedByAdmin(address,address,address[])` |
| `removePrivilegeByAdmin(address contractAddr, address[] addresses)` | `WhitelistRemovedByAdmin` | `admin` (indexed), `contractAddr` (indexed), `users` | `WhitelistRemovedByAdmin(address,address,address[])` |
| `addPrivilege(address[] users)` | `WhitelistAdded` | `sponsor` (indexed), `users` | `WhitelistAdded(address,address[])` |
| `removePrivilege(address[] users)` | `WhitelistRemoved` | `sponsor` (indexed), `users` | `WhitelistRemoved(address,address[])` |

### 3.3 AdminControl 合约

**合约地址**：[cfx:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2mhjju8k](https://confluxscan.org/address/cfx:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2mhjju8k?tab=contract-viewer)

#### 管理员相关事件

| 函数 | 事件名 | 参数 | Event Signature |
|:-----|:-------|:-----|:----------------|
| `setAdmin(address contractAddr, address newAdmin)` | `AdminChanged` | `admin` (indexed), `contractAddr` (indexed), `newAdmin` | `AdminChanged(address,address,address)` |

#### 合约销毁事件

| 函数 | 事件名 | 参数 | Event Signature |
|:-----|:-------|:-----|:----------------|
| `destroy(address contractAddr)` | `ContractDestroyed` | `admin` (indexed), `contractAddr` (indexed) | `ContractDestroyed(address,address)` |

---

## 4. 数据同步模块

由于内置合约的事件日志需要通过 Traces 来组装，而 Traces 的获取相对较重，需要合理设计同步服务架构。

**参考**：[Conflux 内置合约列表](https://confluxscan.org/contracts)

### 4.1 方案选型

| 维度 | 方案 A：主同步流程内联 | 方案 B：同服务内异步协程 | 方案 C：独立同步服务 |
|:-----|:----------------------|:------------------------|:--------------------------|
| **开发复杂度** | 低：直接复用主同步逻辑 | 中：需要设计异步队列、worker 与状态管理 | 中：需要新 repo/服务，但逻辑更清晰 |
| **运维复杂度** | 低：单服务 | 中：单服务但配置与资源管理更复杂 | 中：多一个服务需部署与监控 |
| **故障隔离** | ❌ 无隔离，trace 失败影响主同步 | ⚠️ 部分隔离，进程级故障仍影响主同步 | ✅ 完全隔离，独立故障域 |
| **性能可扩展性** | ❌ 难扩展，容易拖慢主同步 | ⚠️ 可一定程度扩展，但受进程资源限制 | ✅ 可独立扩容与优化 trace 处理能力 |
| **同步灵活性** | ❌ 必须与主同步同速推进 | ⚠️ 可异步处理，但节奏仍受主同步约束 | ✅ 可独立调速、延迟处理、补历史数据 |
| **Reorg 处理** | ✅ 最简单，天然一致 | ⚠️ 需要感知 canonical chain 并协调 | ⚠️ 需设计高度/hash 协调与回滚机制 |
| **代码可维护性** | ❌ 逻辑混杂，主流程复杂化 | ⚠️ 需严格边界，否则容易演变为内联 | ✅ 关注点分离，长期维护成本最低 |

**结论**：采用 **方案 B（同服务内异步协程）**，减少开发复杂度。

### 4.2 数据存储设计

共享同一个 MySQL 实例，表结构分离。

> 💡 **说明**：考虑到内置合约交易数量有限（约 20 万）和合约数量少，采用单表存储。后续可根据数据量增长进行分表优化。

#### 表结构：`internal_contract_logs`

| 字段名 | 类型 | 说明 |
|:-------|:-----|:-----|
| `id` | `BIGINT` | 自增主键 |
| `epoch` | `BIGINT` | Epoch 高度（用于 Reorg 处理） |
| `block_number` | `BIGINT` | 区块高度 |
| `block_hash` | `CHAR(66)` | 区块哈希 |
| `tx_hash` | `CHAR(66)` | 交易哈希 |
| `tx_index` | `INT` | 交易在区块中的位置（排序用） |
| `log_index` | `INT` | 虚拟日志索引 |
| `address` | `CHAR(42)` | 内置合约地址 |
| `topic0` ~ `topic3` | `VARCHAR(66)` | 事件签名及 Topics |
| `data` | `TEXT` | 事件数据 |

**索引设计**：

```sql
CREATE INDEX idx_epoch ON internal_contract_logs(epoch);
CREATE INDEX idx_block_number ON internal_contract_logs(block_number);
CREATE INDEX idx_topic0_block ON internal_contract_logs(topic0, block_number);
CREATE INDEX idx_address_topic0_block ON internal_contract_logs(address, topic0, block_number);
CREATE INDEX idx_address_block ON internal_contract_logs(address, block_number);
```

#### 表结构：`epoch_block_map`

采用（每 5000 万个 epoch) 分区表，用于存储同步进度及 pivot hash 进行 Reorg 检测。

| 字段名 | 类型 | 说明 |
|:-------|:-----|:-----|
| `epoch` | `BIGINT` | 主键，Epoch 高度 |
| `bn_min` | `BIGINT` | 区块范围（最小） |
| `bn_max` | `BIGINT` | 区块范围（最大） |
| `pivot_hash` | `VARCHAR(66)` | Pivot 区块哈希 |

**索引设计**：

```sql
CREATE UNIQUE INDEX idx_epoch ON epoch_block_map(epoch);
CREATE INDEX idx_bn_range ON epoch_block_map(bn_min, bn_max);
```

#### 表结构：`sync_status`

单行表、用于存储全局同步状态和 Reorg 版本号。

| 字段名 | 类型 | 说明 |
|:-------|:-----|:-----|
| `id` | `INT` | 主键，固定为 1（单行表） |
| `reorg_version` | `BIGINT` | Reorg 版本号，每次 Reorg 递增 |
| `latest_synced_epoch` | `BIGINT` | 最新同步的 Epoch 高度 |
| `latest_synced_block` | `BIGINT` | 最新同步的 Block 高度 |
| `latest_pivot_hash` | `VARCHAR(66)` | 最新同步的 Pivot Hash |
| `updated_at` | `TIMESTAMP` | 更新时间 |	

### 4.3 Trace 同步与解析

### 4.3.1 数据源选择

* 使用 **`trace_epoch`** RPC 逐个获取整个 epoch 下所有 block 的 Trace。

* 优点：减少请求次数，捕获合约内部调用（Internal Tx）到内置合约的情况。

### 4.3.2 过滤策略

* 内置合约地址是固定的。

* 在解析 Trace 时，只通过 `Action.to` 或 `Action.from` 过滤这些特定地址的 `Call` 操作。忽略所有其他普通合约的 Trace，降低处理压力。

#### 4.3.3 状态确认

内置合约调用产生合成事件需同时满足以下条件：

| 条件 | 说明 |
|:-----|:-----|
| ① 交易成功 | 交易整体执行成功（`tx.status == Success`） |
| ② 当前调用成功 | 内置合约调用的 `trace.result.outcome == Success` |
| ③ 调用链完整成功 | 所有祖先调用（Parent Traces）的 `outcome` 均为 `Success` |

**示例场景**：

```text
场景：A → B → C → 内置合约1 (成功)
                → D (失败导致 C revert)

判断：
├── 内置合约1 调用本身成功 ✓
├── 但其父调用 C 失败 ✗
└── 结论：不产生事件
```

**实现要点**：

1. **构建调用树**：解析 `trace_epoch` 返回的 traces，构建父子关系树
2. **向上回溯校验**：对每个内置合约调用，向上遍历所有祖先节点
3. **任一失败即跳过**：只要调用链中存在任意失败节点，该内置合约调用不产生事件

### 4.3.4 映射逻辑

   * 需要建立一个 **"Function -> Event"** 的映射表将函数调用成功映射成事件日志。

   * **核心逻辑**

      * **输入**: Trace 中的 `Input Data` (ABI 编码数据)。

      * **解码**: 解析 Function Selector (前4字节) 和 Parameters。

      * **转换**: 构造对应的 Event Topics 和 Data。

* **示例 (Staking 合约)**:

```text
用户调用: deposit(uint256 amount)
Trace Input: 0x... (selector) + amount (data)

合成 Log:
├── address: Staking 合约地址
├── topics[0]: keccak256("Deposit(address,uint256)")
├── topics[1]: caller_address (从 Trace.from 获取，作为 indexed 参数)
├── logIndex: 按照解析 Trace 的解析顺序计数
└── data: amount (作为非 indexed 参数)
```

### 4.3.5 配置设计

因为就是固定的三个内置合约，除了合约地址可以配置外，其他的都可以写死在代码中。
假设我们以一个内置合约（比如 Staking 合约）为例。 场景：用户调用 `deposit(uint256 amount)`，我们需要合成一个 `Deposit(address indexed user, uint256 amount)` 事件。


### 4.4 Reorg 处理

**检测机制**

同步器维护 `last_synced_epoch` 和 `last_pivot_hash`。

每次同步新 Epoch N 时：

1. 获取 Epoch N 的 `Parent Hash` / `Pivot Hash` 信息。
2. 对比 DB 中 Epoch N-1 的 `pivot_hash`。
3. 如果一致：继续同步。
4. 如果不一致：触发回滚。从 DB 中删除 Epoch >= N-1 的所有 logs 和 block mappings，指针回退，重新同步。

### 4.5 Catch-Up 机制

在 Catch-Up 阶段（历史数据同步）：

- 只同步到 latest_finalized 高度，无需考虑 Reorg
- 使用 WorkerPool 从多个 Full Node 并行调用 trace_epoch
- 解析后通过 Batch 写入 数据库

## 5. 数据查询模块

### 5.1 方案选型

| 维度 | 方案 A：合并到现有 getLogs | 方案 B：独立 RPC 服务 | 方案 C：SDK Client 直读数据库 |
|:-----|:--------------------------|:---------------------|:-----------------------------|
| **开发复杂度** | 低：复用现有解析与返回结构 | 中：需要实现完整 RPC 接口与校验逻辑 | 中：需要封装 SDK 并管理连接池 |
| **运维复杂度** | 低：无新增服务 | 中：多一个 RPC 服务需部署与监控 | 低：无新增服务，但需管理 DB 权限 |
| **故障隔离** | ❌ 查询异常可能影响普通合约日志查询 | ✅ 内置合约查询故障不影响现有 getLogs | ✅ 完全独立，不影响其他服务 |
| **性能可扩展性** | ❌ 查询逻辑变复杂，影响整体性能 | ✅ 可独立扩容，按访问模式优化 | ✅✅ 性能最优，无 RPC 开销 |
| **Reorg 处理** | ⚠️ 需与普通 logs 共享一致性语义 | ✅ 可单独设计 | ⚠️ 需在 SDK 层实现一致性校验 |
| **代码可维护性** | ❌ getLogs 语义被污染 | ✅ 职责单一，逻辑高度内聚 | ⚠️ Schema 变更需同步更新 SDK |
| **安全性** | ✅ 通过现有 RPC 鉴权 | ✅ 可独立设计鉴权机制 | ⚠️ 需暴露数据库凭证，仅限内部使用 |
| **适用场景** | 外部用户 + 内部服务 | 外部用户 + 内部服务 | ⚠️ 仅限内部可信服务 |
| **接入成本** | 低：使用现有 SDK | 中：需适配新 RPC 方法 | 中：需引入专用 SDK 依赖 |

### 5.2 方案分析

#### 方案 A：合并到现有 getLogs
- **优点**：开发成本最低，用户无感知
- **缺点**：污染现有接口语义，故障不隔离
- **适用**：不推荐

#### 方案 B：独立 RPC 服务 ⭐ 外部推荐
- **优点**：故障隔离好，接口语义清晰，可独立扩容
- **缺点**：需要额外部署和维护 RPC 服务
- **适用**：外部用户、第三方应用、需要标准化接口的场景

#### 方案 C：SDK Client 直读数据库 ⭐ 内部推荐
- **优点**：内部封装 SQL 查询、性能最优，无网络开销，查询灵活
- **缺点**：需暴露数据库凭证，与 Schema 耦合
- **适用**：内部可信服务

鉴于内置合约日志主要由 Aggregator 消费，且两者通常部署在同一内网环境：

1. 主要路径 (SDK): 封装一个 BuiltinLogReader SDK。Confura 直接引用该 SDK，提供最低的查询延迟。
2. 未来扩展 (RPC): 如果未来需要向第三方开放该数据或者考虑服务扩容，选择独立 RPC 服务。

### 5.3 SDK Client 设计（方案 C）

#### 5.3.1 SDK 接口定义

```go
// LogFilter 查询过滤条件
type LogFilter struct {
    FromBlock, ToBlock uint64
    FromEpoch, ToEpoch uint64
    Addresses []common.Address
    Topics    [][]common.Hash
}

type BuiltinLogsClient interface {
    // GetLogs 查询内置合约日志
    GetLogs(ctx context.Context, filter LogFilter) (*[]types.Log, error)
}
```

### 5.3 数据一致性保障

利用 Reorg Version 进行乐观锁检查，防止查询期间发生 Reorg 返回“脏数据”。

**查询流程**

```text
1. 获取当前 reorg_version (V1)
   └── SELECT reorg_version FROM sync_status WHERE id = 1

2. 检查查询范围是否在已同步范围内
   └── 若 toBlock > latest_synced_block，返回错误

3. 根据 log filter 条件组装 SQL 查询
   └── 执行查询获取 logs

4. 再次获取 reorg_version (V2)
   └── SELECT reorg_version FROM sync_status WHERE id = 1

5. 比较版本号
   ├── V1 == V2：数据一致，返回结果
   └── V1 != V2：查询期间发生 Reorg，重试（最多 3s 超时）
```

## 6 数据聚合

在 Confura 的 `rpc-proxy` 层进行 Event Logs 的聚合操作，合并原生 Log 和合成 Log。

### 6.1 接口设计方案

| 方案 | 实现方式 | 兼容性 | 行为显式性 | 易用性 | 扩展性 |
|------|----------|--------|------------|--------|--------|
| A. 扩展参数 | 在 log filter 增加 includeBuiltin 参数 | 中低 | 高 | 中 | 高 |
| B. 新 RPC 方法 | 提供 cfx_getLogsWithBuiltin | 低 | 高 | 中高 | 高 |
| C. 地址白名单触发 | 根据 filter.address 自动启用 | 高 | 中低 | 高 | 中 |
| D. URL/Header 触发 ⭐ | URL 或 Header 附加参数 （如 parse_trace) | 高 | 高 | 高 | 高 |

>💡 推荐方案 D：灵活且兼容性好，可与其他方案结合使用。

### 6.2 查询模式

**仅支持纯内置合约查询，不支持混合模式。**

| 场景 | Address 条件 | 行为 |
|:-----|:-------------|:-----|
| 纯内置合约 ✅ | 所有地址均为内置合约 | 调用内置合约服务，返回结果 |
| 纯普通合约或不指定地址 | 指定地址均为普通合约 | 走原有 `cfx_getLogs` 逻辑 |
| 混合地址 ❌ | 包含内置 + 普通合约 | **报错**，不支持混合查询 |

>⚠️ 注意：Address 条件为纯内置合约时，需在文档中说明此特殊行为。