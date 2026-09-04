# scanLogs 本地测试环境与执行 Runbook

本 Runbook 将 [scanLogs 本地综合测试计划](../doc/scanlogs_local_test_plan.md) 落成可人工执行、也可被 CI 调用的步骤。自动化入口是：

```bash
./scripts/scanlogs-local-test.sh help
```

脚本不负责启动或停止 MySQL、Confura、sync 和 Full Node。服务生命周期由执行人控制；脚本负责预检、DDL、RPC 用例、E2E oracle 比对、回归和证据归档。

扩展正确性用例也由同一入口执行：`e2e-boundary` 动态派生 cursor、guard、范围、limit 和空页请求，`api-blackbox` 执行 JSON-RPC 参数负向矩阵，`consistency` 校验并运行 CON 故障映射。三者均已纳入 `all-readonly`，无需人工分别运行。

## 1. 文件和安全边界

| 文件 | 用途 |
|---|---|
| `scripts/scanlogs-local-test.sh` | 分阶段测试执行器 |
| `scripts/scanlogs-local-test.env.example` | 非敏感测试变量模板 |
| `scripts/scanlogs-local-cases.example.json` | E2E 数据集格式示例 |
| `scripts/scanlogs-index-ddl.sh` | 存量日志表索引 DDL/EXPLAIN 工具 |
| `artifacts/scanlogs/<run-id>/` | 每次测试的证据目录，不应提交到 Git |

安全规则：

- DDL 只针对可恢复、可丢弃的测试库；先确认数据库名，再执行 ADD/DROP。
- `ddl-add` 必须设置 `SCANLOGS_ALLOW_DDL=yes`。
- `ddl-drop` 和 `ddl-cycle` 必须额外设置 `SCANLOGS_ALLOW_DROP=yes`。
- 本地测试可直接在权限为 `0600`、不提交 Git 的 env 文件中配置 MySQL 用户名密码；脚本不会把密码写入 artifacts。
- Full Node URL 如果包含 token，不要把 env 文件或完整命令输出提交到仓库。
- `all-readonly` 不执行 ADD/DROP，但会读取数据库、调用 RPC 并运行测试。

## 2. 前置依赖

本机需要：

- Go 1.23.x；
- MySQL 8.x 服务端及 `mysql` client；
- `curl`、`jq`、Git、Bash；
- 可访问的 Core Space 和 eSpace Full Node；
- Full Node 必须保留 manifest 覆盖的历史日志；纯 FN/mixed 范围需要日志/归档节点支持；
- 足够磁盘保存两套日志索引数据和测试证据。

检查：

```bash
go version
mysql --version
curl --version
jq --version
bash --version
```

macOS 自带 Bash 3.2 即可运行脚本。建议额外安装 `shellcheck`，但它不是运行依赖。

## 3. 准备 MySQL 8 测试库

建议准备两组数据库：

| 数据库组 | 初始状态 | 用途 |
|---|---|---|
| DDL 演练库 | 从上线前版本或 staging 脱敏快照恢复，保留旧索引 | 必须真实执行 ADD、幂等 ADD、EXPLAIN 和 DROP 旧索引 |
| 功能/E2E 库 | 当前代码同步出的可用日志数据 | 纯 DB、纯 FN、mixed、正确性和性能测试 |

如果 DDL 演练库升级完成后也能满足 E2E 数据要求，两组可以复用；否则使用两个 env 文件分别指向两组数据库，并在同一个 Run ID 下执行。用当前代码新建的空库会直接带有新复合索引，只能验证“新建表索引正确”，不能算作“存量 DDL 完整演练”。

### 3.1 使用已有 MySQL

创建两个独立数据库：

```sql
CREATE DATABASE confura_cfx_scanlogs_test CHARACTER SET utf8mb4;
CREATE DATABASE confura_eth_scanlogs_test CHARACTER SET utf8mb4;
```

不要复用生产库名，也不要对唯一的数据副本执行 `ddl-drop`。

DDL 验收优先恢复上线前的脱敏数据库快照。恢复后先运行 `ddl-plan`，输出中必须实际包含待执行的 `ADD INDEX` 和旧索引 `DROP INDEX`；如果全部显示 skip，说明数据库已经是新 schema，不能覆盖存量升级路径。

### 3.2 使用仓库 Docker Compose 的 MySQL

只启动依赖，不启动 `conflux/confura:latest` 应用镜像：

```bash
docker compose up -d db influxdb
docker compose port db 3306
```

第二条命令返回宿主机映射端口，例如 `0.0.0.0:49153`。直接使用用户名密码连接：

```bash
mysql --host=127.0.0.1 --port=<映射端口> --user=root --password=root -e 'SELECT VERSION();'
```

创建数据库并验证连接：

```bash
mysql --host=127.0.0.1 --port=<映射端口> --user=root --password=root -e '
CREATE DATABASE IF NOT EXISTS confura_cfx_scanlogs_test CHARACTER SET utf8mb4;
CREATE DATABASE IF NOT EXISTS confura_eth_scanlogs_test CHARACTER SET utf8mb4;
SELECT VERSION();
'
```

## 4. 构建当前代码

必须测试当前 checkout，不使用未重新构建的远端镜像：

```bash
make build
./bin/confura --version
```

记录当前 revision：

```bash
git rev-parse HEAD
git status --short
```

工作区有未提交修改时可以测试，但最终报告必须保存 `git status --short`，并说明被测内容。

## 5. 配置并启动本地服务

### 5.1 必需配置

可以使用仓库现有配置加载机制，以 `INFURA_` 环境变量覆盖 `config/config.yml`。至少配置：

```bash
# Core/eSpace 数据源
export INFURA_CFX_HTTP=<CORE_FULLNODE_RPC>
export INFURA_ETH_HTTP=<ESPACE_FULLNODE_RPC>

# Node manager 的普通组和日志组
export INFURA_NODE_URLS=<CORE_FULLNODE_RPC>
export INFURA_NODE_LOGNODES=<CORE_FULLNODE_RPC>
export INFURA_NODE_ETHURLS=<ESPACE_FULLNODE_RPC>
export INFURA_NODE_ETHLOGNODES=<ESPACE_FULLNODE_RPC>

# 两个 Store。下面仅为本机示例，端口替换为实际 MySQL 端口。
export INFURA_STORE_MYSQL_ENABLED=true
export INFURA_STORE_MYSQL_DSN='root:root@tcp(127.0.0.1:<MYSQL_PORT>)/confura_cfx_scanlogs_test?parseTime=true'
export INFURA_STORE_PERSISTENCE_TYPES=log

export INFURA_ETHSTORE_MYSQL_ENABLED=true
export INFURA_ETHSTORE_MYSQL_DSN='root:root@tcp(127.0.0.1:<MYSQL_PORT>)/confura_eth_scanlogs_test?parseTime=true'
export INFURA_ETHSTORE_PERSISTENCE_TYPES=log

# RPC 通过本地 node manager 路由到日志节点组
export INFURA_NODE_ROUTER_NODERPCURL=http://127.0.0.1:22530
export INFURA_NODE_ROUTER_ETHNODERPCURL=http://127.0.0.1:28530

# scanLogs 配置
export INFURA_REQUESTCONTROL_SCANLOGS_MAXLIMIT=1000
export INFURA_REQUESTCONTROL_SCANLOGS_FULLNODEWINDOWSIZE=1000
```

如果 Full Node URL 或 MySQL DSN 含密钥，将这些变量放到仓库外、权限受控的启动文件中。测试脚本不会采集这些值。

### 5.2 首次同步

分别启动同步进程：

```bash
./bin/confura sync --db
./bin/confura sync --eth
```

它们会初始化表结构并同步日志。该方式适合准备功能/E2E 库；当前代码会直接创建新索引，不替代旧 schema DDL 演练。等待两个数据库都出现 `epoch_block_map` 和日志物理表。另一个终端观察水位：

```bash
mysql --host=127.0.0.1 --port=<MYSQL_PORT> --user=root --password=root confura_cfx_scanlogs_test -e '
SELECT epoch,bn_min,bn_max,pivot_hash FROM epoch_block_map ORDER BY epoch DESC LIMIT 1;
'

mysql --host=127.0.0.1 --port=<MYSQL_PORT> --user=root --password=root confura_eth_scanlogs_test -e '
SELECT epoch,bn_min,bn_max,pivot_hash FROM epoch_block_map ORDER BY epoch DESC LIMIT 1;
'
```

当数据库已有足够数据后，使用 `Ctrl-C` 正常停止两个 sync 进程。测试期间保持 sync 停止，以固定 DB/FN 水位。不要停止 MySQL、node manager 或 RPC。

如果需要测试“outer retry 时 DB 水位变化”，使用 fake 测试通道；不要在 E2E 正确性比对期间同时运行 sync。

### 5.3 启动 node manager 和 RPC

四个终端分别运行：

```bash
./bin/confura nm --cfx
./bin/confura nm --eth
./bin/confura rpc --cfx
./bin/confura rpc --eth
```

默认端口：

| 服务 | 默认地址 |
|---|---|
| Core node manager | `http://127.0.0.1:22530` |
| eSpace node manager | `http://127.0.0.1:28530` |
| Core RPC | `http://127.0.0.1:22537` |
| eSpace RPC | `http://127.0.0.1:28545` |

快速人工检查：

```bash
curl -sS http://127.0.0.1:22537 \
  -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"cfx_getStatus","params":[]}' | jq

curl -sS http://127.0.0.1:28545 \
  -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' | jq
```

## 6. 创建测试执行配置

复制模板到仓库外或被 Git 忽略的位置：

```bash
cp scripts/scanlogs-local-test.env.example /tmp/scanlogs-local-test.env
cp scripts/scanlogs-local-cases.example.json /tmp/scanlogs-local-cases.json
chmod 0600 /tmp/scanlogs-local-test.env /tmp/scanlogs-local-cases.json
```

编辑 env，至少填写：

- Core/eSpace 本地 Proxy URL；
- Core/eSpace Full Node URL；
- 两个数据库名及 `SCANLOGS_MYSQL_HOST/PORT/USER/PASSWORD`；
- case manifest 的绝对路径；
- 实际 address/topic 分区数。

整次验收共用一个 Run ID：

```bash
export SCANLOGS_RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
```

后续所有命令都带同一个 env 文件和 Run ID：

```bash
TEST_RUNNER='./scripts/scanlogs-local-test.sh'
TEST_ENV='/tmp/scanlogs-local-test.env'

"$TEST_RUNNER" --env-file "$TEST_ENV" --run-id "$SCANLOGS_RUN_ID" init
"$TEST_RUNNER" --env-file "$TEST_ENV" --run-id "$SCANLOGS_RUN_ID" preflight
```

预检必须确认：工具存在、MySQL 两个库连接成功、两个 Proxy 和两个 Full Node 可达。

## 7. 执行单测和可控故障测试

```bash
"$TEST_RUNNER" --env-file "$TEST_ENV" --run-id "$SCANLOGS_RUN_ID" unit
"$TEST_RUNNER" --env-file "$TEST_ENV" --run-id "$SCANLOGS_RUN_ID" fault
```

`unit` 执行：

1. `go build ./...`；
2. Store/Handler/ACL 定向单测；
3. 定向 `-race`；
4. `go test ./...`。

`fault` 定向运行 retry、boundary、DB cache、FN shrink、Pivot 和 Core 路线 B 测试。若综合测试计划中的 CON-01～CON-15 仍有场景没有对应自动化用例，应先补测试再继续，不用真实链 reorg 代替。

所有日志和退出码保存到：

```text
artifacts/scanlogs/<run-id>/unit/
artifacts/scanlogs/<run-id>/fault/
```

## 8. DDL 全流程

### 8.1 读取当前水位

```bash
"$TEST_RUNNER" --env-file "$TEST_ENV" --run-id "$SCANLOGS_RUN_ID" watermarks
```

### 8.2 只读计划

```bash
"$TEST_RUNNER" --env-file "$TEST_ENV" --run-id "$SCANLOGS_RUN_ID" ddl-plan
```

人工检查输出中的数据库名、物理表数量、ADD/DROP 语句和分区数。任何目标不符合预期都先停止。

完整 DDL 演练时，初始 plan 必须至少包含一条真实 ADD 和一条旧索引 DROP。没有实际变更的 plan 只能作为“已升级状态复核”，不能作为存量升级演练证据。

### 8.3 ADD、幂等和 EXPLAIN

确认目标是可丢弃测试库后：

```bash
export SCANLOGS_ALLOW_DDL=yes

SCANLOGS_DDL_PHASE_LABEL=add-1 \
  "$TEST_RUNNER" --env-file "$TEST_ENV" --run-id "$SCANLOGS_RUN_ID" ddl-add
SCANLOGS_DDL_PHASE_LABEL=add-2-idempotent \
  "$TEST_RUNNER" --env-file "$TEST_ENV" --run-id "$SCANLOGS_RUN_ID" ddl-add
SCANLOGS_DDL_PHASE_LABEL=verify-before-drop \
  "$TEST_RUNNER" --env-file "$TEST_ENV" --run-id "$SCANLOGS_RUN_ID" ddl-verify
```

第二次 ADD 必须全部 skip。`ddl-verify` 对每个非空物理表执行相应的正/逆序 EXPLAIN。验收：

- 8 个逻辑组合全部命中预期复合索引；
- 所有存在的物理表族都被覆盖；
- `Extra` 不包含 `Using filesort`；
- 脚本退出码为 0。

### 8.4 DROP 旧索引并复验

只有 ADD 和 verify 已通过才执行：

```bash
export SCANLOGS_ALLOW_DROP=yes

SCANLOGS_DDL_PHASE_LABEL=drop \
  "$TEST_RUNNER" --env-file "$TEST_ENV" --run-id "$SCANLOGS_RUN_ID" ddl-drop
SCANLOGS_DDL_PHASE_LABEL=verify-after-drop \
  "$TEST_RUNNER" --env-file "$TEST_ENV" --run-id "$SCANLOGS_RUN_ID" ddl-verify
```

也可以在全新可丢弃库一次运行完整循环：

```bash
SCANLOGS_ALLOW_DDL=yes SCANLOGS_ALLOW_DROP=yes \
  "$TEST_RUNNER" --env-file "$TEST_ENV" --run-id "$SCANLOGS_RUN_ID" ddl-cycle
```

`ddl-cycle` 会为第二次 ADD 和 DROP 前后 verify 使用不同证据文件，避免覆盖。

## 9. 准备 E2E case manifest

脚本将每个 enabled case 自动展开为：

```text
正序/逆序 × 普通/WithPivot = 4 个分页测试
```

要完成 96 个基础 P0 case，manifest 需要：

```text
2 Space × 3 来源 × 4 过滤 = 24 个 enabled case
```

### 9.1 三类来源

读取已暂停 sync 后的 DB latest mapping：

- 纯 DB：`to <= DB latest`；
- 纯 FN：`from > DB latest`；
- mixed：`from <= DB latest < to`。

Full Node 的固定上界必须高于 DB latest。所有范围都使用数值 hex quantity，不使用 `latest` 等动态 tag。

### 9.2 四类过滤

每个来源准备：

1. 无 address、无 topic0；
2. 单 address；
3. 单 topic0；
4. 单 address + topic0。

可以先对候选固定范围执行一次 Full Node `getLogs`，从结果中选出 address 和第一个 topic。要求 `limit=7` 时至少跨两页，最好三页；mixed case 必须在水位两侧都有匹配日志。

Core case 示例：

```json
{
  "name": "cfx-mixed-address",
  "enabled": true,
  "space": "cfx",
  "source": "mixed",
  "filter": {
    "epochRange": {
      "fromEpoch": "0x...",
      "toEpoch": "0x..."
    },
    "address": "cfx:..."
  }
}
```

eSpace case 示例：

```json
{
  "name": "eth-db-topic0",
  "enabled": true,
  "space": "eth",
  "source": "db",
  "filter": {
    "blockRange": {
      "fromBlock": "0x...",
      "toBlock": "0x..."
    },
    "topic0": "0x..."
  }
}
```

脚本会把 scanLogs filter 转换为对应 `getLogs` filter，获取 Full Node oracle。测试前后会读取固定上界 hash；上界变化时 case 被判定为 invalidated，需要重跑，而不是记为 scanLogs 数据错误。

Core Base32 地址在比较前会统一为 `network:payload` 小写形式，因此 Full Node 的 verbose 地址（例如 `CFX:TYPE.CONTRACT:...`）与 Proxy 的 short 地址（例如 `cfx:...`）按同一语义比较；其余日志字段仍逐字段严格比较。

大范围 case 不会被一次性传给 Full Node。校验器按 `SCANLOGS_ORACLE_WINDOW`（默认 1000）将 `getLogs` oracle 拆成连续、无重叠的数值区间，再按顺序拼接。若 Full Node 的 `max_gap` 小于默认值，在 env 中调小该变量。

历史 `getLogs` oracle 使用独立的 `SCANLOGS_ORACLE_TIMEOUT_SECONDS`（默认 60 秒）；scanLogs 分页仍使用较严格的 `SCANLOGS_RPC_TIMEOUT_SECONDS`（默认 15 秒）。若无过滤历史查询仍超时，应优先减小 `SCANLOGS_ORACLE_WINDOW` 或缩小 case 范围，而不是无限增大超时。

同一分窗规则也用于 getLogs regression：Full Node 与 Proxy 分别按完全相同的窗口查询、拼接并进行语义比较，避免节点 `max_gap` 把大范围回归误判为失败。

## 10. RPC smoke、负向和 E2E

### 10.1 四方法 smoke

```bash
"$TEST_RUNNER" --env-file "$TEST_ENV" --run-id "$SCANLOGS_RUN_ID" rpc-smoke
```

该阶段调用：

- `cfx_scanLogs`；
- `cfx_scanLogsWithPivotAssumption`；
- `eth_scanLogs`；
- `eth_scanLogsWithPivotAssumption`。

### 10.2 原始 JSON-RPC 负向检查

```bash
"$TEST_RUNNER" --env-file "$TEST_ENV" --run-id "$SCANLOGS_RUN_ID" rpc-negative
```

自动检查未知字段被拒绝，以及超过默认 maxLimit 的业务错误使用框架默认 `-32000`。

ACL、stale、响应体超限和 logs 组双 stub 路由仍应按综合测试计划 T6 使用独立配置实例执行；这些场景依赖部署配置，不能由一个通用 env 安全推断。

### 10.3 分页与 oracle 比对

```bash
"$TEST_RUNNER" --env-file "$TEST_ENV" --run-id "$SCANLOGS_RUN_ID" e2e
```

每个 case 自动验证：

- 正序拼接等于 `getLogs` oracle；
- 逆序拼接等于反转后的 oracle；
- 普通和 WithPivot 两种方法；
- 页大小不超过 limit；
- 非空页一定有 Cursor，空页无 Cursor；
- Cursor 不停滞；eSpace 精确等于页尾 `(blockNumber, logIndex)`，Core 校验页尾 `logIndex`（Core 公共日志不暴露物理 `blockNumber`）；
- eSpace 拼接结果按 `(blockNumber, logIndex)` 严格单调；Core 校验日志身份唯一并通过完整 canonical oracle 保证顺序、无遗漏和无重复；
- 普通方法不返回 Guard；
- 空页不返回 Guard；
- 逆序 WithPivot Guard 在续页保持固定；
- 固定上界 hash 在测试前后不变。

失败证据位于：

```text
artifacts/scanlogs/<run-id>/e2e/<case>/<direction>-<variant>/
```

其中包含 oracle、逐页原始响应、拼接结果和首个完整 diff。

### 10.4 需要独立配置的专项

以下用例无法由通用 env 安全自动修改服务端配置，按表执行并把原始请求/响应保存到 `api/`：

| 专项 | 操作 | 预期 |
|---|---|---|
| stale | 完成 WithPivot 首页，复制 Guard 并修改 hash 最后一位，再携带原 Cursor 请求续页 | code `-32000`，message 包含 pivot assumption failure/mismatch，不返回部分 logs |
| 响应超限 | 另启 Proxy，将 `INFURA_REQUESTCONTROL_RESOURCELIMITS_MAXGETLOGSRESPONSEBYTES` 设为很小值，对密集范围请求较大 limit | 整体报错，不返回伪短页 |
| ACL | 使用只允许地址 A 的测试 key，分别请求 A、地址 B，以及普通/WithPivot 四个方法 | A 通过，B 在查询前拒绝，不能绕过白名单 |
| logs 组路由 | 普通节点组和 logs 节点组分别指向两个带请求计数的 stub | 四个 scanLogs 方法只增加 logs stub 计数 |
| 空 Store | 另启未配置对应 Store 的 Proxy | 返回 `scan logs rpc unavailable`，进程保持健康 |

stale 手工续页请求形态：

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "eth_scanLogsWithPivotAssumption",
  "params": [
    {
      "filter": {
        "blockRange": {"fromBlock": "0x...", "toBlock": "0x..."}
      },
      "limit": "0x7",
      "cursor": {"blockNumber": "0x...", "logIndex": "0x..."}
    },
    {"blockNumber": "0x...", "blockHash": "0x...被篡改..."}
  ]
}
```

Core 使用 `epochNumber/pivotBlockHash` 替代 eSpace 的 `blockNumber/blockHash`。

## 11. getLogs 回归

```bash
"$TEST_RUNNER" --env-file "$TEST_ENV" --run-id "$SCANLOGS_RUN_ID" regression
```

该阶段：

1. 再跑 `go test ./...`；
2. 对 manifest 中所有 enabled case，比对 Full Node 与本地 Proxy 的 `cfx_getLogs` / `eth_getLogs`；
3. 如果设置 `SCANLOGS_RUN_LEGACY_GETLOGS=1`，额外运行仓库现有 `TestGetLogs`。启用时必须在执行脚本的父 shell 中导出：

```bash
export TEST_CFX_FULL_NODE=<CORE_FULLNODE_RPC>
export TEST_CFX_INFURA_NODE=http://127.0.0.1:22537
```

## 12. 一次执行所有只读阶段

环境、数据库索引和 manifest 都准备好后，可以运行：

```bash
"$TEST_RUNNER" --env-file "$TEST_ENV" --run-id "$SCANLOGS_RUN_ID" all-readonly
```

它依次执行：preflight、unit、fault、watermarks、DDL plan、RPC smoke、RPC negative、E2E、getLogs regression 和报告生成，不执行 ADD/DROP。

## 13. 性能测试拆解

正确性测试全通过后再测性能。使用综合测试计划 T8 的 P1～P5 profile。建议复用同一 manifest，另写 Go/k6/vegeta 驱动，只调用 scanLogs，不在计时区间重复生成 oracle。

执行顺序：

1. 预先跑 E2E，保存所有第一页请求和 Cursor 链；
2. 预热 2 分钟；
3. 并发 1，分别测纯 DB/FN/mixed、正/逆序、limit 100/1000；
4. 并发 4 运行 10 分钟；
5. 并发 16 运行 10 分钟；
6. 停止压测后再跑一次 E2E，确认压力期间没有产生数据错误；
7. 导出客户端 p50/p95/p99、QPS、错误率和响应字节；
8. 导出 MySQL rows examined、慢 SQL、CPU/IO/连接/锁；
9. 导出 Confura CPU/RSS/GC、window/shrink、outer/inner retry、boundary mismatch 和 DB cache 指标。

性能数据保存到 `artifacts/scanlogs/<run-id>/perf/`。不得只记录平均延迟。

## 14. staging 只读验收

本地全部通过后，将 env 中四个 RPC endpoint 替换为 staging 地址，数据库凭据仅需只读。不要在 staging 运行：

- `ddl-add`；
- `ddl-drop`；
- `ddl-cycle`；
- 人工修改 mapping/reorgVersion；
- 故障 stub。

运行 `preflight`、`rpc-smoke`、`rpc-negative`、E2E 子集、`regression` 和低并发性能 smoke。staging artifacts 使用新的 Run ID，并在最终报告中引用本地 Run ID。

## 15. 生成结论

```bash
"$TEST_RUNNER" --env-file "$TEST_ENV" --run-id "$SCANLOGS_RUN_ID" report
```

报告路径：

```text
artifacts/scanlogs/<run-id>/conclusion.md
```

脚本会汇总各 phase 的退出码。执行人还需补充 DDL 表数量、E2E case 总数、性能、staging、已知 FN ABA 限制和最终 `GO` / `CONDITIONAL GO` / `NO-GO` 结论。

任何分页重复/遗漏、ACL 或路由绕过、稳定 boundary mismatch、响应静默截断、DDL/EXPLAIN 失败、getLogs 回归都必须判为 `NO-GO`。

## 16. 常见失败处理

| 现象 | 处理 |
|---|---|
| preflight 连接不到 MySQL | 检查 MySQL host、映射端口、用户名、密码和数据库名 |
| scanLogs 返回 unavailable | RPC 没有配置对应 Store，检查 `*_MYSQL_ENABLED/DSN` |
| scanLogs 路由失败 | 检查 `node.logNodes/ethLogNodes` 和本地 node manager URL |
| DDL 报目标表缺失 | 核对 `bn_partitions`、实际物理表和分区配置，不手工跳过 |
| E2E oracle 过大 | 缩小固定范围或使用更具体的 address/topic0 |
| canonical upper changed | 本 case 作废，选择 finalized/confirmed 范围后重跑 |
| mixed case 实际只有 DB 或 FN 日志 | 更换范围/过滤，保证水位两侧都有匹配日志 |
| 第二次 ADD 仍执行 DDL | 索引未正确创建或表清单变化，停止 DROP 并检查日志 |
| 逆序 Guard 漂移 | 保存逐页响应，按 PIV-02/PIV-03 判 P0 失败 |
| `all-readonly` 中途停止 | 修复失败 phase，沿用同一 Run ID 单独重跑该命令 |
