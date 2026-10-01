# MeterFlow 开源项目调研与改进路线

调研时间：2026-10-01。本文以当前代码为准，对照项目的官方文档和仓库，目的是确定 MeterFlow 适合解决的问题，以及达到可公开使用、可复现、可贡献的程度还缺什么。外部项目的功能与文档会变化，发布前应重新核对。

## 产品定位

**建议定位：供可信业务服务调用的多租户用量账本与配额服务。** MeterFlow 负责认证上报方、记录可追溯用量、维护账户余额，并提供调用前预留额度与调用后结算的能力。上游仍负责实际 API 或模型调用。

当前 `POST /v1/usage` 只接受调用完成后的 `units`，并不能核实数值。若上游先消耗资源，再收到 `QUOTA_EXCEEDED`，资源已经消耗而事件没有记入账本。因此目前的“配额”只约束**成功入账的用量**；它还不是对上游资源消耗的严格预算上限。这个边界应在 README 和演示中始终明确。[当前写入流程](../src/main/java/dev/peng/meterflow/UsageService.java)、[接口定义](../src/main/java/dev/peng/meterflow/Contracts.java)

## 同类项目对照

| 项目 | 官方资料中的核心做法 | MeterFlow 值得借鉴的部分 | 不宜直接照搬的部分 |
| --- | --- | --- | --- |
| [OpenMeter](https://openmeter.io/docs/metering/overview) | 事件计量、时间窗口聚合、主体归属；[事件](https://openmeter.io/docs/metering/events/usage-events)携带 `id`、`source`、`time`、`subject`，按 `id + source` 去重；[额度权益](https://openmeter.io/docs/billing/entitlements/entitlement)支持使用周期与余额查询。 | 明确事件来源和发生时间，保留可重放的去重键；提供按时间段、租户和模型查询用量；明确周期配额语义。 | 其 [Kafka/ClickHouse 流处理架构](https://openmeter.io/docs/metering/events/how-it-works)服务于更大规模的计量分析，当前体量无需引入。 |
| [Lago](https://github.com/getlago/lago) | 从事件到计量、定价、额度、账单、支付的完整链路；使用 `transaction_id` 做事件幂等，支持批量上报和可观测运行。 | 将幂等键、批次原子性和对账做成清晰的 API 契约；提供可复制的本地部署与示例。 | 发票、税务、支付和多种定价模型属于完整计费平台，会冲淡本项目的后端主线。 |
| [LiteLLM](https://docs.litellm.ai/docs/proxy/users) | 代理请求，按密钥、团队和用户设置预算与速率限制；[预算预留](https://docs.litellm.ai/docs/proxy/users)在调用上游前占用估计额度，返回后按实际用量结算。 | 如果声称严格预算，就需要调用前预留和调用后结算，或明确采用软限制；后续可增加密钥权限与有效期。 | LiteLLM 是模型代理，MeterFlow 不必实现模型路由、适配各供应商响应或完整网关。 |
| [Apache ShenYu](https://github.com/apache/shenyu/blob/master/README.md) | Java 网关，涵盖代理、插件、鉴权、限流、可观测性等。 | 借鉴清晰的模块边界、管理接口与运行指标。 | 协议转换、服务发现和插件生态属于网关领域，不是 MeterFlow 现阶段要竞争的方向。 |

## 当前项目已有的基础

- [事务设计](design.md)：租户行锁、唯一约束、`READ_COMMITTED` 与同事务写入事件/余额；并发重试、配额边界和故障回滚有集成测试。
- [密钥与租户](../src/main/java/dev/peng/meterflow/TenantService.java)：随机密钥只返回一次，库内保存摘要，可撤销；上报按租户归属。
- [可审计性](../src/main/java/dev/peng/meterflow/AdminController.java)：可查看最近事件和执行只读对账。
- [可复现实验](benchmarks/README.md)：记录了硬件、请求口径、定位过程与原始结果。同租户合并提交后，32 并发下单租户约 763 req/s、8 租户约 586 req/s（改造前约 72 与 316）；本机回环、无真实上游，不能当线上容量承诺。

## 改进优先级

### P0：发布前必须明确和补齐

1. **修正额度语义。** 已完成：README 区分直接上报（只约束入账）与预留结算（严格预算），后者实现了 `reserve → commit/release`、到期回收、幂等重试与对账，见[预留与结算](design.md#预留与结算)。
2. **建立真正的开源仓库。** 已发布公开仓库，采用 [MIT 许可证](../LICENSE)，并加入在 MySQL 8.4 上运行全部测试的 [CI 工作流](../.github/workflows/ci.yml)；仍缺少 `CONTRIBUTING.md`、`SECURITY.md` 与版本发布说明。[GitHub 社区健康指南](https://docs.github.com/en/communities/setting-up-your-project-for-healthy-contributions)、[GitHub 安全策略指南](https://docs.github.com/en/code-security/how-tos/report-and-fix-vulnerabilities/configure-vulnerability-reporting/add-security-policy)、[许可证说明](https://choosealicense.com/no-permission/)
3. **让使用者一键复现。** 现有 Compose [只启动 MySQL](../docker-compose.yml)，应用需手动配置并运行；补应用镜像、应用与数据库的 Compose 编排、环境变量模板、完整演示脚本、OpenAPI 文档和版本化变更说明。CI 应跑 H2 测试和 MySQL 集成测试，并验证迁移。验收：全新环境按 README 的固定步骤可完成启动、创建租户、写入、重试、对账。

### P1：形成有辨识度的后端能力

4. **事件模型与周期配额。** 现有事件只有 `requestId/model/units/createdAt`，[数据库约束](../src/main/resources/db/migration/V1__create_metering_tables.sql)按租户与 `requestId` 去重。增加来源、事件发生时间和单位类型；明确时间戳缺失、晚到、同编号不同内容、跨来源重复的处理。随后提供按租户/模型/时间区间分页查询与月度配额。迁移时保留旧记录语义，避免将接收时间误作事件发生时间。[OpenMeter 事件格式](https://openmeter.io/docs/metering/events/usage-events)
5. **权限边界。** 管理端现在是单一 [Basic Auth 管理员](../src/main/java/dev/peng/meterflow/SecurityConfig.java)，上报密钥没有作用域、到期时间或轮换流程。加入租户管理员与平台管理员的最小权限划分、密钥到期/轮换、管理操作审计；公开部署要求 TLS 与安全的凭据管理。
6. **运行可观测性。** 当前仅暴露健康检查，[配置](../src/main/resources/application.yml)未暴露业务指标。增加成功、重试、冲突、配额拒绝计数与请求耗时；日志带请求编号与租户标识，避免记录密钥原文；给出监控面板或最小告警示例。[Lago 仓库](https://github.com/getlago/lago)列有 API、队列、工作进程与事件指标，适合作为指标分类参考。

### P2：有需求后再扩展

7. **热点租户性能。** 已完成多轮测试与定位：瓶颈是每次提交的落盘耗时而非锁内 SQL，已用[同租户合并提交](design.md)让一次落盘承载多条上报。若仍不够，再评估额度分段预留或分片账户，并给出超发上界与对账方案。不要只为简历加入 Redis 或消息队列。
8. **定价和收费。** 仅在明确要做计费产品后加入定价版本、货币精度、补记/冲正和账单周期；在此之前用 `units` 而非金额描述系统，避免与 Lago 的完整计费能力混淆。

## 推荐下一步

先做 P0 的**额度语义与预留/结算设计**，同时完成许可证选择、CI、完整 Compose 和 API 文档。之后做事件时间与周期配额。这样既保留当前事务正确性优势，也能把“严格额度”从演示说法变成可验证的系统能力；对应的简历描述必须以真实完成和测试结果为准。
