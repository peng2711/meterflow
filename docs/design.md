# 事务与并发设计

## 不变量

1. 每个 `(tenant_id, request_id)` 最多对应一条用量事件、一条预留；同一编号不能既直接上报又走预留。
2. `used_units >= 0`、`reserved_units >= 0`，且 `used_units + reserved_units <= quota_units`。
3. 账户 `used_units` 等于该租户用量事件的 `SUM(units)`，`reserved_units` 等于状态为 `RESERVED` 的预留之和；事件、预留与余额同事务提交或回滚。
4. 同一个请求编号与相同内容重试时不再扣减；同编号不同内容返回冲突。

数据库以唯一约束和 `CHECK` 约束守住前两项，服务代码与 MySQL 故障注入测试共同验证第三、四项。余额对账接口是检测偏差的只读入口，不会静默修改账本。

对账在一个 `REPEATABLE READ` 只读事务中读取余额、事件总和与未结预留之和，这些读取来自同一个 MVCC 快照。事件、预留与余额总在同一事务中提交，所以快照内它们必然对应，不需要锁租户行；即使事件很多、`SUM` 较慢，也不会挡住正在提交的批次。

## 写入顺序

```text
请求线程：根据 API 密钥摘要查租户 -> 放入该租户的待处理队列 -> 等待结果（默认最多 10 秒）
批处理线程：从队列取出至多 batch-size 条，在一个事务中：
  -> 锁定租户行 SELECT ... FOR UPDATE
  -> 一次查询确认本批用到的密钥仍未撤销
  -> 一次查询取出本批 requestId 已有的事件
  -> 按到达顺序逐条判定：
       密钥已撤销：401
       已存在且内容相同（含本批更早的同编号）：回放原事件
       已存在但内容不同：409
       剩余额度不足：429
       否则：记为新事件，扣减内存中的剩余额度
  -> 多行 INSERT 写入新事件，UPDATE 一次账户累计值
  -> 提交事务，再把每条的结果交还给对应请求
```

逐条判定的结果，与每条请求各自开一个事务、按同样顺序执行的结果相同；单条被拒不影响同批其他请求。事务因数据库错误回滚时，本批全部请求返回错误且都没有入账，客户端用同一 `requestId` 重试是安全的。请求线程等待超时则返回 `503 USAGE_PENDING`：这条上报可能稍后才提交，同样用原 `requestId` 重试即可得到确定结果。

同租户的写入争用一行锁，不同租户可并行。撤销密钥也先锁定对应租户行，故撤销与用量写入按获取行锁的顺序生效。

MySQL 默认的 `REPEATABLE READ` 可能在事务首次读取时建立快照。若批处理等待租户锁期间其他事务提交了新事件，后续普通查询未必能看到它们，于是可能碰到唯一约束错误。写入事务显式使用 `READ_COMMITTED`，让锁后的查询读取当时已提交的数据。MySQL 对 [`FOR UPDATE` 锁定读](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html) 和 [隔离级别](https://dev.mysql.com/doc/refman/8.4/en/innodb-transaction-isolation-levels.html) 的文档说明了这两种读取语义。

## 同租户合并提交

**为什么需要。** 持久化提交要等 redo log 与 binlog 落盘（`innodb_flush_log_at_trx_commit=1`、`sync_binlog=1`）。在测试机上一次提交约 14 ms，而锁内的几条 SQL 合计不到 1 ms。行锁一直持有到提交结束，所以一个租户每秒最多完成约 1/14ms ≈ 70 次提交；减少锁内 SQL 往返几乎无效，放宽落盘策略又会在宕机时丢失已确认的用量。测量过程见[负载测试文档](benchmarks/README.md)。

**怎么做。** 让一次提交承载多条上报（应用层组提交）。每个租户一条有界队列，同一时刻至多一个批处理任务在处理它：
- 没有等待窗口。空闲租户的第一条请求立即单独成批；上一批提交期间到达的请求在队列里累积，下一批一次取走，因此负载越高批次越大，低负载时不增加延迟。
- 一批处理完后若队列仍有数据，任务重新排到线程池队尾，热点租户不会饿死其他租户。
- 队列满时直接返回 `503 USAGE_BACKLOG_FULL`，不阻塞请求线程；停机时拒绝新请求，已入队的批次处理完再退出。

**正确性不依赖批处理器。** 不变量仍由租户行锁、唯一约束和 `CHECK` 约束守住。即使同一租户短暂出现两个批次（例如多实例部署，或队列被回收的瞬间），它们也只是在数据库行锁上排队。`meterflow.usage.batch-size=1` 时每条上报单独一个事务，可作对照。

## 预留与结算

`POST /v1/usage` 是调用完成后才上报：上游资源已经消耗，`QUOTA_EXCEEDED` 只能拒绝入账，挡不住调用本身，并发时多个调用可能一起超出额度。需要严格预算时，上游改走预留与结算：

```text
调用前  POST /v1/reservations              {requestId, model, units: 本次最多消耗, ttlSeconds}
        -> 可用额度 = quota - used - reserved，不够返回 429，上游不发起调用
        -> 够则冻结 units，状态 RESERVED，返回 expiresAt
调用后  POST /v1/reservations/{requestId}/commit   {units: 实际消耗}
        -> 解冻预留、按实际值写入用量事件，状态 COMMITTED，多冻结的部分回到可用额度
调用失败 POST /v1/reservations/{requestId}/release
        -> 解冻全部预留，状态 RELEASED
```

状态只能从 `RESERVED` 单向变为 `COMMITTED`、`RELEASED` 或 `EXPIRED` 之一，每次变化都在租户行锁内判定：

| 当前状态 | 再次预留（同内容） | 结算 | 释放 |
|---|---|---|---|
| `RESERVED`，未到期 | 回放 | 实际值 ≤ 预留：`COMMITTED`；超出：`422`，预留保持打开 | `RELEASED` |
| `RESERVED`，已到期 | 回放 | 先转为 `EXPIRED` 再拒绝：`409 RESERVATION_EXPIRED` | 转为 `EXPIRED`，返回成功 |
| `COMMITTED` | 回放 | 同值回放，不同值 `409` | `409 RESERVATION_COMMITTED` |
| `RELEASED` | 回放 | `409 RESERVATION_RELEASED` | 回放 |
| `EXPIRED` | 回放 | `409 RESERVATION_EXPIRED` | 回放 |

设计取舍：

- **预留是上限。** 结算值超过预留时拒绝而不是照单入账，否则 `used + reserved <= quota` 守不住。上游应把调用自身的上限（例如大模型的 `max_tokens` 加上已知的输入 token）作为预留值，从调用层面保证实际消耗不超过预留。
- **到期按时间判定。** 是否过期只看 `expires_at`，与后台任务有没有运行无关，所以同一次迟到的结算无论何时到达结果都一样。TTL 应大于上游调用的超时时间；超过 TTL 仍未结算，视为调用方已放弃。
- **后台回收。** 调用方崩溃、既不结算也不释放时，`ReservationSweeper` 每 5 秒（可配置）找出到期的预留，按租户分别加锁、标记 `EXPIRED` 并归还额度。在它运行之前，到期的预留仍占着额度，额度判断因此偏保守，不会多发。
- **同一条合并提交通道。** 预留、结算、释放与直接上报一样进入租户队列，同一批次内按到达顺序判定，可以看到彼此的效果（例如同批内先预留后结算）。热点租户的吞吐优势因此对预留流程同样成立。
- **与直接上报共用请求编号。** 结算产生的用量事件沿用预留的 `requestId`，账本只有一种事件；同一编号已直接上报则不能再预留，反之亦然，均返回 `409`。

## 运行指标

`/actuator/prometheus`（需管理员 Basic Auth）导出以下业务指标。指标不带租户标签，避免时间序列数随租户数增长。

| 指标 | 类型 | 含义 |
|---|---|---|
| `meterflow_usage_operations_total{type,outcome}` | 计数器 | 每个操作的结果。`type` 为 `charge`、`reserve`、`commit`、`release`；`outcome` 为 `accepted`、`replayed`、`idempotency_conflict`、`quota_exceeded`、`invalid_api_key`、`reservation_not_found`、`reservation_expired`、`reservation_released`、`reservation_committed`、`commit_exceeds_reservation`、`backlog_full`、`shutting_down`、`failed` |
| `meterflow_usage_reservations_expired_total` | 计数器 | 后台任务回收的到期预留数；持续增长说明调用方经常不结算 |
| `meterflow_usage_batch_size` | 分布 | 每个账本事务承载的操作数；桶为 1、2、5、10、20、50、100 |
| `meterflow_usage_batch_duration_seconds` | 计时器 | 一个批次的事务耗时：加租户锁、SQL、持久化提交 |
| `meterflow_usage_queue_wait_seconds` | 计时器 | 操作在租户队列中等到所在批次开始的时间 |
| `meterflow_usage_pending` | 仪表 | 已入队、尚未被批次取走的操作数 |
| `meterflow_usage_response_timeouts_total` | 计数器 | 请求线程等待超时的次数；这类操作可能稍后才提交，因此不计入 `outcome` |

常用查询：

```promql
# 平均每个事务承载的操作数：合并提交是否在起作用
rate(meterflow_usage_batch_size_sum[1m]) / rate(meterflow_usage_batch_size_count[1m])
# 事务耗时 P99：落盘是否变慢
histogram_quantile(0.99, rate(meterflow_usage_batch_duration_seconds_bucket[1m]))
# 额度拒绝占比
sum(rate(meterflow_usage_operations_total{outcome="quota_exceeded"}[5m])) / sum(rate(meterflow_usage_operations_total[5m]))
```

排查时，平均批次大小接近 1 而排队时间上升，说明瓶颈不在合并而在单次提交；`pending` 持续增长或出现 `backlog_full`，说明写入超过了落盘能力。

## 已验证的边界

- 两个并发上报使用同一请求编号：一笔扣减，一次回放。
- 两个并发上报争用最后一单位：仅一笔成功，余额不超配额。
- 同一批次内的重复编号、冲突、撤销的密钥、额度不足：按到达顺序逐条判定，互不影响。
- 单租户 200 个编号各并发上报两次、配额 150（H2 32 线程、MySQL 64 线程）：恰好 150 次入账、150 次回放、100 次额度不足，对账一致。
- 批处理器：提交期间到达的请求合并到下一批且不超过批次上限；队列满时拒绝；事务失败时同批全部失败。
- 预留：冻结额度对其他预留和直接上报都不可用；结算按实际值入账并归还差额；重复操作回放，内容不同返回冲突；结算超出预留返回 422 且预留保持打开；到期后结算被拒绝，后台回收只回收到期且未结的预留；同一批次内先预留后结算、再释放的顺序判定。
- 40 个并发调用各预留 10、结算 4（H2）与 120 个并发调用混合结算、释放和放弃（MySQL）：已用加冻结始终不超过额度，回收后冻结为 0，对账一致。
- 另一事务持有租户行锁时，对账仍在 5 秒内返回（改为加锁读后该测试超时失败）；并发写入期间反复对账，每次结果都一致。
- 更新账户时由 MySQL 临时约束注入失败：整批事件插入随事务回滚，对账仍一致。
- 密钥撤销后再次调用：认证失败。

## 当前取舍

合并提交提高了单租户的吞吐上限，但没有消除串行化：同一租户的批次仍一个接一个提交。代价是请求线程要等待批处理线程交回结果，以及一个额外的线程池和每租户队列需要调优（批次上限、线程数、队列容量）。若还需进一步扩展，可评估额度分片或预留，并给出超发与对账代价；当前实现没有声称支持此类能力。

平台接收可信上游给出的 `units`，不自行核实模型 token 或支付金额。管理员身份目前采用单账户 Basic Auth，默认只监听本地回环地址；对外部署需要 TLS 和正式身份系统。
