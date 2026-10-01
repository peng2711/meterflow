# MeterFlow：多租户 API 用量与配额平台

[![CI](https://github.com/peng2711/meterflow/actions/workflows/ci.yml/badge.svg)](https://github.com/peng2711/meterflow/actions/workflows/ci.yml)

一个独立的 Java 后端项目。它接收可信上游服务上报的 API 用量，按租户扣减配额，并提供密钥管理、幂等重试和账本对账。项目没有接入知应 Agent，也没有复制知应的订单、退款或工单代码。

## 为什么做这个题目

[LiteLLM](https://docs.litellm.ai/docs/) 展示了虚拟密钥、用量追踪和预算控制等真实需求；[Spring PetClinic](https://github.com/spring-projects/spring-petclinic) 展示了 Spring 应用可运行、可测试的组织方式。MeterFlow 只参考这些需求和工程做法，代码与数据模型独立实现。

## 核心设计

并发正确性与取舍详见[事务设计](docs/design.md)。

对照 OpenMeter、Lago、LiteLLM 和 Apache ShenYu 的功能边界及后续改进优先级，见[开源项目调研与改进路线](docs/open-source-review-2026-10.md)。

```text
管理员 Basic Auth -> 创建租户 / 发放密钥 / 撤销密钥 / 查询账本
可信上游 X-Api-Key -> 请求校验 -> 进入该租户队列
批处理线程 -> 租户行锁 -> 逐条幂等检查 / 配额检查 -> 批量插入不可变用量事件 + 更新余额（同一事务，一次提交）
管理员 -> 对账接口 -> 比较租户累计值与事件 SUM(units)
```

- **租户隔离**：所有用量事件以 `tenant_id` 归属；同一 `requestId` 在不同租户可独立使用。
- **密钥管理**：生成 256 位随机 API 密钥，仅在发放时返回一次；数据库保存 SHA-256 摘要，支持撤销。管理员使用独立的 Basic Auth。
- **并发扣减**：事务中使用 `SELECT ... FOR UPDATE` 锁定租户行；在锁内检查重复请求与剩余配额。数据库唯一约束 `(tenant_id, request_id)` 是第二道保护。
- **同租户合并提交**：压测定位到单租户吞吐受限于每次提交的磁盘落盘（约 14 ms），而非锁内 SQL。同一租户在上一批提交期间到达的请求合并进下一个事务，一次落盘承载多条上报；不设等待窗口，不放宽落盘策略，不变量仍由行锁和数据库约束保证。
- **事务隔离**：用量写入显式使用 `READ_COMMITTED`，确保等待租户锁后的重试能读到已提交的事件；余额额外受数据库 `used_units <= quota_units` 约束保护。
- **幂等语义**：相同请求编号和内容再次上报时返回原事件且不重复扣减；同编号不同模型或用量返回 `409`。
- **可审计**：事件表只追加，管理员可查看最近 50 条事件，并检查累计值与事件账本是否一致。

这是**用量接收与配额控制服务**，不代理模型请求，也不自行核验上游报告的用量；因此不能把它描述为生产计费系统或模型网关。当前配额为租户的累计上限，没有自动月度重置、分布式限流或真实支付链路。

## 技术栈

Java 17、Spring Boot 3.5、Spring JDBC、Spring Security、Bean Validation、Flyway、MySQL 8。默认用 H2 文件库快速启动；已在 MySQL 8.0.46 做集成测试，Compose 提供 MySQL 8.4。测试使用 JUnit 5、Spring Boot Test 和 H2 的 MySQL 兼容模式。

## 快速运行

需要 JDK 17+。项目自带 Maven Wrapper（Maven 启动脚本）；在 `meterflow` 目录运行：

```bash
mkdir -p data
export ADMIN_PASSWORD='请替换为本地密码'
./mvnw spring-boot:run
```

默认使用 `./data/meterflow` H2 文件库，服务仅绑定 `127.0.0.1:8080`。仅本地演示时可省略密码变量，默认密码为 `local-demo-only`；使用 MySQL 时必须设置 `ADMIN_PASSWORD`。对外部署需通过 TLS 入口保护管理员的 Basic Auth 凭据。

切换 MySQL：

```bash
docker compose up -d mysql
export DB_URL='jdbc:mysql://localhost:3307/meterflow?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true'
export DB_USER='meterflow'
export DB_PASSWORD='meterflow-local'
export ADMIN_PASSWORD='请替换为本地密码'
./mvnw spring-boot:run
```

`docker-compose.yml` 中的数据库口令只用于本地演示。应用启动时 Flyway 自动建表。

## 最短演示流程

```bash
# 1. 管理员创建租户，保存响应中的 id
curl -u "admin:$ADMIN_PASSWORD" -H 'Content-Type: application/json' \
  -d '{"name":"team-a","quotaUnits":100}' \
  http://localhost:8080/admin/tenants

# 2. 把 TENANT_ID 换成上一步的 id；密钥只返回这一次
curl -u "admin:$ADMIN_PASSWORD" -X POST \
  http://localhost:8080/admin/tenants/TENANT_ID/keys

# 3. 把 API_KEY 换成上一步的 apiKey
curl -H 'Content-Type: application/json' -H 'X-Api-Key: API_KEY' \
  -d '{"requestId":"demo-001","model":"example-model","units":17}' \
  http://localhost:8080/v1/usage

# 4. 重复第 3 步，replayed 应为 true，usedUnits 仍为 17
curl -u "admin:$ADMIN_PASSWORD" \
  http://localhost:8080/admin/tenants/TENANT_ID/reconciliation
```

其他接口：`GET /admin/tenants/{id}`、`GET /admin/tenants/{id}/events`、`DELETE /admin/keys/{keyId}`、`GET /actuator/health`。

## 验证

```bash
./mvnw test
```

集成测试覆盖：重复请求与冲突、配额不足、跨租户隔离、密钥撤销、并发扣减、同批次内逐条判定、单租户并发突发、故障回滚、HTTP 鉴权和输入校验；批处理器另有单元测试覆盖合并、队列满拒绝和整批失败。默认运行 H2 测试；设置 `METERFLOW_MYSQL_TEST_*` 环境变量后，还会运行真实 MySQL 集成测试，方法见[负载测试文档](docs/benchmarks/README.md)。

本地 MySQL 8.0.46、默认持久化配置、32 个并发客户端、各 3 轮取中位数：单租户从每条一个事务的 71.9 req/s（P99 571 ms）提升到合并提交后的 762.7 req/s（P99 65 ms），8 租户从 315.5 提升到 585.8 req/s，全部轮次 0 错误、账本一致。定位过程、对照组与原始数据见[负载测试文档](docs/benchmarks/README.md)。这是同机回环的本地结果，提升幅度取决于磁盘落盘耗时，不代表线上容量。
