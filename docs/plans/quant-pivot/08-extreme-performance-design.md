# Quant Pivot 极致性能重构设计

> 状态：已冻结，执行状态见
> [`09-extreme-performance-ledger.md`](09-extreme-performance-ledger.md)。

## 目标

在不保留旧 L2 历史、兼容 reader、双写或转发 re-export 的前提下，重构
quant-pivot 的数据所有权、行情摄取、持久化、订单簿发布、WebSocket fanout
和运行时资源模型。

## 不可变决策

- 最终 ClickHouse 表名为 `quant_book_l2_ledger`，schema version 从 `1` 开始。
- 删除 `quant_book_l2_event`、`quant_book_l2_checkpoint` 和旧 JCS L2 hash。
- 不回填旧 L2 历史；依赖旧 L2 的 replay、source slice、训练和验证证据失效。
- 系统尚未正式投产；切换直接删除未封存环境的完整项目数据库并执行唯一 bootstrap，
  不实现 L2 或其他业务数据迁移。production baseline 存在时永久 fail closed。
- 81 个内部 UUID ID 直接包装 `Uuid`，保持 16 字节 Copy 值语义。
- `ContentHash` 直接包装 32 字节 BLAKE3 digest，人类文本仅在边界格式化。
- 数据面固定为 8 个 token-affine partition actor，不按 token 数创建 worker。
- BookStore 只发布稳定 TokenSlot；mutable OrderBook 归 partition actor 单写所有。
- WebSocket 使用单写者 SessionHub、topic 倒排索引和一次编码的 `ByteString`。
- 禁止兼容 shim、旧 feature alias、旧表 reader 和 forwarding re-export。

## 性能契约

阻断基准平台为 Linux x86_64、8 vCPU、16 GiB；macOS 运行功能测试与
benchmark smoke，不与 Linux 数值比较。

| 指标 | 阈值 |
|---|---:|
| 活跃 token | 2,000 |
| 持续摄取 | 10K events/s |
| 突发摄取 | 50K events/s，持续 10 秒 |
| normalize 到 partition enqueue p99 | 250 us |
| ingress 到 durable publish p99 | 250 ms |
| BookStore 同步读 p99 | 10 us，零分配 |
| 在线稳态 RSS | 3.5 GiB |
| 离线计算期间进程 RSS | 10 GiB |
| 10K session、1K subscriber fanout p99 | 2 ms |
| 2K 市场报告纯计算 p99 | 2 s |
| 1M x 128 训练矩阵 | 60 s / 8 GiB |
| 1M 行 10 折 CPCV | 5 min |

## 数据面

Gamma/订阅冷路径构造一个不可变 `DataPlaneIndex` 并通过 ArcSwap 原子发布。
索引将外部 `TokenId`/`U256` 映射到内部 `TokenKey(u32)`，并持有稳定
`TokenSlot`。热路径只传递 `TokenKey`，未注册 token 必须 fail closed。

一个 WS message 规范化为一个批次，经 8 路 router 分组后进入 bounded
partition mailboxes。全部 mailbox 共享 256 MiB 字节预算；单批最多 1,024
events 或 1 MiB；250 ms 无法取得预算即 invalidates 当前 stream session。

每个 partition 同时只有一个 in-flight `PreparedCanonicalBatch`。唯一的
ledger persistence coordinator 聚合最多 8,192 rows 或 20 ms，并通过 8 个
长期存在、单调递增的 commit cursor 通知 partition；禁止逐行/逐批创建 ACK
channel。只有 ClickHouse durable ack 后才按顺序应用订单簿并发布 snapshot。

## 统一 L2 账本

`quant_book_l2_ledger` 同时承载 `Snapshot`、`Delta`、`TickSizeChange`、`Gap`
和 `LastTrade`。盘口使用 typed Decimal arrays，hash 使用 `FixedString(32)`。
MarketWs `LastTrade` 只保留在 canonical L2 ledger；参与者和成交结构特征来自
独立的 finalized exchange-history 投影。两条事实链各自只有一个 durable owner，
不得用 materialized view、dual write 或兼容 reader 合并语义。

Hash 使用域分隔固定宽度编码：schema、UUID、shard、token、sequence、事件
类型、时间和 variant 字段按固定顺序写入 BLAKE3。Price、Shares、Fee 分别
量化到 scale 8、18、4 的 signed i128 big-endian；禁止 JSON、JCS、Decimal
String、hex String 或临时序列化 Vec 进入 hash 热路径。

## 订单簿与读取

Partition actor 独占 mutable sorted Vec sides。批内 delta 先按 `(side, price)`
折叠最终值，再与旧 side 做 O(n+m) merge；每个变化 side 最多发布一个新
`Arc<[BookLevel]>`。Snapshot 预计算 best bid/ask、spread、mid、top1/5/20
depth、imbalance 和 crossed，microstructure 不得再次遍历同一盘口。

BookStore 提供同步 guard read 和显式 owned load。普通同步读取不得调用
`ArcSwap::load_full()`；guard 绝不能跨 `await`。

## WebSocket fanout

SessionHub actor 独占 sessions、topic subscribers、subject/family indices、
system readers 和 watched-market refcounts。Subscribe/Unsubscribe 修改倒排索引；
fanout 复杂度为 O(subscribers)，不得扫描全部 session 或获取 per-session 订阅锁。

`WsEnvelope` 每事件只编码一次并转成 `ByteString`；各 session outbound 只克隆
共享 Bytes。Book/status 为 best effort，reliable alert/lifecycle 在慢客户端队列
满时取消 session，禁止静默丢失。

## 运行时资源

8-vCPU 部署固定 Tokio worker 3、Tokio blocking 上限 4、Actix worker 1、Actix
blocking worker 1、global/offline Rayon 2。Serving、research jobs 和 Chainlink
adapter 使用独立 feature 边界；serving-only 依赖图不得包含 Polars、AWS SDK、
Chainlink SDK、SmartCore 或 Argmin。

全部运行时 crate 通过唯一 `quant-pivot-allocator` crate 固定声明
`tikv_jemallocator::Jemalloc`。禁止 system fallback、allocator feature 开关、第二
`#[global_allocator]` 或其他 allocator crate。作为 rustc host plugin 的 proc-macro crate
不进入目标进程 allocator 图。

## 清空边界

切换前停止全部 quant-pivot 进程，使 ingest/report/research、session 和 writer
完成 shutdown/drain。`preproduction-reset` 删除并重建完整项目 PostgreSQL 与
ClickHouse 数据库、清空 Redis `qp:*` namespace，然后只执行当前唯一 bootstrap；
不提供 `v2` schema、ALTER 数据迁移、旧 reader、双写或证据转换。

`apply` 必须同时匹配短期一次性 nonce 与完整确认字符串
`DELETE_ALL_PREPRODUCTION_DATA_AND_REBOOTSTRAP`。完成后 ledger 必须为空，旧对象、
row type、reader、hash、checkpoint parser 和相关研究证据不得残留。只有新账本重新
积累数据并完成训练、CPCV、parity 和治理审批闭环后，才允许启用
SemiAuto/AutoExecution。
