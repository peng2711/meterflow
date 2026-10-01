# MySQL 本地验证与负载测试

## 环境与口径

- 日期：2026-10-01；CPU：Intel Core i9-13900K，系统可见 32 逻辑处理器；内存：125 GiB；磁盘：NVMe，ext4。
- 服务：Java 17.0.20.1、Spring Boot 3.5.6、JVM `-Xms256m -Xmx512m`、Hikari 最大连接数 16。
- 数据库：同机 MySQL 8.0.46，InnoDB，默认持久化配置：`innodb_flush_log_at_trx_commit=1`、`sync_binlog=1`、开启 binlog。服务器默认 `REPEATABLE-READ`，用量写入事务显式使用 `READ_COMMITTED`。
- 客户端：同机 Python 3.10 标准库 HTTP 客户端，每个工作线程复用一条长连接；正式计时前先发送 2,000 次不计时的预热请求。
- 请求：每轮新建租户；每个请求编号上报 1 单位，另有 20% 的重复请求。`requests_per_second` 是包含重试在内的 HTTP 请求吞吐，不是成功扣减数。
- 检查：每轮核对 HTTP 错误数、首次记录数、回放数、租户余额与事件账本总和。所有轮次均 0 错误、0 重复扣减、账本一致。
- 每种配置跑 3 轮，表中为中位数。服务、数据库、客户端同机，无真实上游，数值只说明这个本地负载模型，不代表线上容量。

## 结果

32 个并发客户端（同一客户端条件下的对比）：

| 写入方式 | 单租户 | 单租户 P99 | 8 租户 | 8 租户 P99 |
|---|---:|---:|---:|---:|
| 改造前：每条上报一个事务 | 71.9 req/s | 571 ms | 315.5 req/s | 200 ms |
| 合并提交，`batch-size=1`（对照） | 69.2 req/s | 872 ms | 329.9 req/s | 241 ms |
| **合并提交，默认 `batch-size=100`** | **762.7 req/s** | **65 ms** | **585.8 req/s** | **115 ms** |

128 个并发客户端：

| 写入方式 | 单租户 | 单租户 P99 | 8 租户 | 8 租户 P99 |
|---|---:|---:|---:|---:|
| 合并提交，`batch-size=1`（对照） | 69.2 req/s | 2,403 ms | 357.8 req/s | 577 ms |
| **合并提交，默认 `batch-size=100`** | **1,523.4 req/s** | **169 ms** | **1,486.8 req/s** | **172 ms** |

- 改造前与 `batch-size=1` 每轮 1,200 次请求；合并提交默认配置每轮 12,000 次请求，避免高吞吐下一轮不到一秒、测量窗口过短。
- 128 并发时，Python 客户端进程的 CPU 占用约 140%，在 GIL 限制下已接近上限；服务端 Java 约 2 个核、MySQL 不到 1 个核。因此 128 并发一栏是这个客户端能测出的下限，不是服务端上限。
- 8 租户在 32 并发时低于单租户：每个租户只分到约 4 个客户端，每批最多约 4 条。下文的落盘统计解释了原因。

原始数据：[`20261001-group-commit/`](20261001-group-commit/)。文件名中的 `per-charge-lock` 是改造前版本，`batch-size-1` 是对照组，`group-commit` 是默认配置，`diagnostic-no-fsync` 是下面的定位实验。

## 预留与结算

`--mode reserve` 让每次调用先预留 2 单位、再结算 1 单位，一次调用是两个 HTTP 请求。单租户、32 并发、每轮 12,000 次调用（含 20% 重试），3 轮中位数：

| 写入方式 | 完整调用/秒 | HTTP 请求/秒 | 单次调用 P99（两个请求之和） |
|---|---:|---:|---:|
| `batch-size=1`（每个操作一个事务） | 31.2 | 62.4 | 1,459 ms |
| **合并提交（默认）** | **300.6** | **601.2** | **184 ms** |

逐个操作提交时，一次调用要两次落盘，约 1 / (2 × 14 ms) ≈ 35 次/秒，实测 31。合并提交后预留与结算和直接上报共用租户队列，同样受益。每轮结束时冻结额度归零，账本与冻结对账一致。`batch-size=1` 这组每轮 1,200 次调用。

加入预留后，直接上报的同条件测试为 727.4 req/s（3 轮 744 / 727 / 690），比加入前的 762.7 低约 5%。每个批次多了一次按 `requestId` 查预留的查询，约占单批耗时的 1%，其余差异在轮间波动范围内。原始数据：`reserve-commit-*.json`、`group-commit-with-reservations-32c-1tenant.json`。

## 定位过程

1. **排除客户端。** 原脚本每个请求新建一条 TCP 连接。改为长连接后单租户仍为约 72 req/s，说明瓶颈在服务端。
2. **测量提交成本。** 用 `mysql` 客户端在同一实例上连续执行 200 次自动提交的单行 `INSERT`，平均每次 14.06 ms；临时设置 `innodb_flush_log_at_trx_commit=2`、`sync_binlog=0` 后降为 0.17 ms。可见每次提交的耗时几乎全部是等待磁盘落盘。
3. **验证瓶颈。** 在关闭落盘的配置下运行改造前的版本：单租户从 72 升至 1,542 req/s，8 租户从 316 升至 4,666 req/s（数据见 `diagnostic-no-fsync-*`）。测完立即恢复默认配置。记账服务不能接受宕机时丢失已确认的用量，所以这只用于定位，不作为优化方案。
4. **结论。** 租户行锁要持有到提交结束，一个租户每秒最多提交约 1 / 14 ms ≈ 70 次。锁内 SQL 合计不到 1 ms，减少往返最多省下几个百分点。有效的方向是让一次提交承载多条上报，见[事务设计](../design.md)中的“同租户合并提交”。

改造后，用 `SHOW GLOBAL STATUS` 统计一轮（1,200 次请求、32 并发）期间的 `Innodb_os_log_fsyncs` 与 `Com_commit`：

| 配置 | req/s | redo 落盘次数/秒 | 每次事务提交承载的上报数 |
|---|---:|---:|---:|
| `batch-size=1`，单租户 | 71 | 92 | 1.0 |
| `batch-size=100`，单租户 | 775 | 91 | 15.2 |
| `batch-size=100`，8 租户，8 个批处理线程 | 715 | 64 | 2.0 |
| `batch-size=100`，8 租户，2 个批处理线程 | 433 | 74 | 4.3 |
| `batch-size=100`，8 租户，1 个批处理线程 | 306 | 82 | 5.1 |

每秒落盘次数基本是磁盘决定的常数，吞吐取决于每次落盘能带上多少条上报。单租户由应用层合并；多个租户则靠多个批处理线程并行提交，由 MySQL 的组提交把不同租户的事务合进同一次落盘。减少批处理线程虽然让单批更大，却失去了跨租户的组提交，吞吐反而下降，所以默认保留 8 个线程。这张表每种配置只跑一轮，数值有波动，只用于说明趋势。

## 运行指标视角

加入 [Prometheus 指标](../design.md#运行指标)后，每种场景先预热 500 次请求，再在一轮 12,000 次请求的前后各抓取一次 `/actuator/prometheus`，取差值：

| 场景 | req/s | 平均每批上报数 | 批次分布 | 单批事务耗时 P50 / P99 | 排队等待 P50 / P99 |
|---|---:|---:|---|---:|---:|
| 32 并发，单租户 | 753 | 16.0 | 99% 的批次为 11–20 条 | ≤ 22 / ≤ 45 ms | ≤ 14 / ≤ 34 ms |
| 32 并发，8 租户 | 607 | 2.2 | 41% 的批次只有 1 条 | ≤ 28 / ≤ 62 ms | ≤ 22 / ≤ 50 ms |
| 128 并发，单租户 | 1,733 | 47.2 | 64% 的批次为 21–50 条，最大 99 | ≤ 22 / ≤ 62 ms | ≤ 15 / ≤ 50 ms |

单租户时吞吐约等于“平均每批上报数 ÷ 单批耗时”：16 条 ÷ 约 21 ms ≈ 760 req/s，与实测一致。8 租户时每个租户只分到约 4 个客户端，批次小，所以吞吐低于单租户，与前面落盘计数的结论一致。耗时取自直方图桶的上界，只是近似值；每种场景一轮，用于说明机制，不替代上面的多轮结果。原始数据：[`metrics-batch-size.json`](20261001-group-commit/metrics-batch-size.json)。

## 复现

启动 MySQL（`docker-compose.yml` 使用 MySQL 8.4，本次记录实际测试版本为 8.0.46）：

```bash
docker compose up -d mysql
export DB_URL='jdbc:mysql://127.0.0.1:3307/meterflow?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true'
export DB_USER='meterflow'
export DB_PASSWORD='meterflow-local'
export ADMIN_PASSWORD='请替换为本地密码'
./mvnw spring-boot:run
# 对照组：每条上报单独一个事务
# ./mvnw spring-boot:run -Dspring-boot.run.arguments=--meterflow.usage.batch-size=1
```

另开终端、设置相同的 `ADMIN_PASSWORD`，运行：

```bash
python3 scripts/load_test.py --tenants 1 --workers 32 --unique 10000 --retries 2000
python3 scripts/load_test.py --tenants 8 --workers 32 --unique 10000 --retries 2000 --warmup 0
# 预留 + 结算
python3 scripts/load_test.py --mode reserve --tenants 1 --workers 32 --unique 10000 --retries 2000
```

默认跑 3 轮，`--rounds` 可调整，`--output` 保存完整 JSON。磁盘不同，落盘耗时差别很大：服务器级 SSD 带掉电保护时落盘可能不到 1 ms，此时改造前后的差距会小得多。

真实 MySQL 集成测试使用独立的空库。Docker Compose 演示环境可先创建测试库并授权（口令仅用于本地演示）：

```bash
docker compose exec -T mysql mysql -uroot -proot-local-only < scripts/create_test_db.sql
```

随后设置环境变量并执行测试：

```bash
export METERFLOW_MYSQL_TEST_URL='jdbc:mysql://127.0.0.1:3307/meterflow_test?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true'
export METERFLOW_MYSQL_TEST_USER='meterflow'
export METERFLOW_MYSQL_TEST_PASSWORD='meterflow-local'
./mvnw test
```

测试会清空该库中的 `tenants`、`api_keys`、`usage_events` 表，应只指向专用测试库。测试用户需要建表与修改表结构的权限，供 Flyway 迁移和故障注入测试添加临时约束。

## 早期结果

`mysql-8.0.46-local-20261001.json` 与 `mysql-8.0.46-local-8tenants-20261001.json` 是改造前、客户端每个请求新建连接、各跑一轮时的记录（单租户 69.15 req/s，8 租户 241.75 req/s），保留作历史对照，已被上表取代。
