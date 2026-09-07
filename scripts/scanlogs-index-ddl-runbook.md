# scanLogs 索引升级脚本运行说明

## 运行前门禁

1. 所有可能创建日志分区的 Confura 实例已经部署包含新 GORM 复合索引定义的版本。
2. 在与生产同构的 staging 数据库完成全流程演练。
3. 准备只允许当前运维账号读取的 MySQL client 配置文件，例如：

   ```ini
   [client]
   host=mysql.example.internal
   port=3306
   user=confura_ddl
   password=REDACTED
   ssl-mode=REQUIRED
   ```

4. 确认 Core/eSpace 各自的数据库名、address 分区数和 topic 分区数。
5. 在低峰期执行；脚本逐表串行运行，可用 `--pause-seconds` 在表之间暂停。

## Core Space 示例

```bash
./scanlogs-index-ddl.sh \
  --database confura_cfx \
  --address-partitions 100 \
  --topic-partitions 10 \
  --defaults-extra-file /secure/confura-cfx-mysql.cnf \
  --mode plan

./scanlogs-index-ddl.sh \
  --database confura_cfx \
  --address-partitions 100 \
  --topic-partitions 10 \
  --defaults-extra-file /secure/confura-cfx-mysql.cnf \
  --mode add --execute

./scanlogs-index-ddl.sh \
  --database confura_cfx \
  --address-partitions 100 \
  --topic-partitions 10 \
  --defaults-extra-file /secure/confura-cfx-mysql.cnf \
  --mode verify

./scanlogs-index-ddl.sh \
  --database confura_cfx \
  --address-partitions 100 \
  --topic-partitions 10 \
  --defaults-extra-file /secure/confura-cfx-mysql.cnf \
  --mode drop --execute
```

eSpace 使用相同顺序，替换数据库名、连接配置文件和实际分区数。

`verify` 和 `drop` 的前置验证会通过 `IGNORE INDEX` 临时排除待删除旧索引，模拟旧索引删除后的优化器环境。只有模拟结果仍命中目标复合索引且不存在 `Using filesort` 时，才允许执行 DROP；该提示只影响 EXPLAIN，不会修改业务查询或索引可见性。

## DDL 超时与重试

ADD/DROP 默认在同一 MySQL 会话中先执行 `SET SESSION lock_wait_timeout = 10;`，再执行 DDL。即使客户端配置启用了 `force`，脚本也会禁用它，确保 SET 失败时不会继续执行 DDL。`plan` 输出同样包含 SET；手工执行时必须在同一会话执行 SET 和对应 DDL。

可通过以下参数调整：

- `--lock-wait-timeout N`：元数据锁等待秒数，默认 10，范围 1–999999999。
- `--ddl-max-attempts N`：每条 DDL 最大尝试次数（含首次），默认 5，范围 1–999999999。
- `--ddl-retry-interval N`：锁等待超时后的重试间隔秒数，默认 10，范围 0–999999999。

参数使用十进制整数，不接受前导零。重试间隔与成功执行后的 `--pause-seconds` 表间暂停独立。例如在 add/drop 命令中附加 `--lock-wait-timeout 10 --ddl-max-attempts 5 --ddl-retry-interval 10`。

仅 MySQL `ERROR 1205 (HY000)` 触发重试；其他错误（包括客户端崩溃）立即退出。达到尝试上限后脚本非零退出，不处理后续表。日志保留尝试次数、错误和重试间隔。

该超时限制元数据锁等待时间，不限制索引构建总时长，也不是整条 DDL 的总耗时上限。它缩短等待中的 DDL 对 RPC proxy SELECT 的阻塞窗口，不能保证完全没有短暂阻塞。

在 staging 演练时，用另一个会话开启事务并读取目标表、保持事务未提交，再运行 ADD/DROP。确认锁等待约 10 秒后超时并按间隔重试；释放事务后应成功。保持锁直至重试耗尽时，应以非零状态退出且不操作后续表。此验证只应在 staging 执行。

## 失败处理

- `plan`/`verify` 失败：没有执行 DDL，修复报告的问题后重跑。
- `add` 失败：已完成的 ADD 保留；脚本幂等，修复原因后重新执行 `add`。
- `drop` 前置验证失败：不会执行任何 DROP。
- `drop` 执行中失败：部分旧索引可能已经删除；新索引会保留，修复原因后重新执行 `drop`。
- 不手工删除同名但定义不一致的索引，先确认实际 schema 和数据路由。

## 交付校验

```bash
sha256sum scanlogs-index-ddl.sh
bash -n scanlogs-index-ddl.sh
python3 scanlogs-index-ddl-test.py
```

保留四个阶段的完整标准输出、标准错误和退出码作为生产升级记录。
