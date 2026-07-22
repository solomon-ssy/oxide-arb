# Quant Pivot 极致性能执行台账

> 本文件是中断恢复的唯一事实来源。任何时刻只能有一个任务处于
> `in_progress`。状态只允许 `pending`、`in_progress`、`blocked`、`done`、
> `superseded`。

## 当前检查点

- 基线分支：`quant-pivot`
- 工作树基线：开始执行时干净
- 当前任务：`PERF-21`（`in_progress`：可复核性能 harness 与 CI evidence）
- 最后完成步骤：真实 2K-token Gamma/CLOB→ClickHouse→Fresh BookStore smoke 首次贯通并生成 JSON/HDR/SHA-256；发现并修复 warm-up backlog 污染测量窗口
- 恢复命令：`cargo run -p quant-pivot-system-tests --bin performance_load -- --profile smoke --output target/performance-evidence-smoke`

## 任务表

| ID | 任务 | 状态 | 依赖 | 完成出口 | 证据 |
|---|---|---|---|---|---|
| PERF-00 | 冻结设计、台账和决策记录 | done | 无 | 文档可独立恢复上下文 | 设计、台账、操作手册和索引 |
| PERF-01 | 修复 release benchmark，冻结基线 | done | PERF-00 | release workspace/bench 可编译 | release check 通过；snapshot apply 135.12–137.51ns |
| PERF-02 | 81 个 UUID ID 改为 Copy | done | PERF-01 | 16 字节、无 Drop、wire/DB golden 不变 | 11 个 ID contract 测试、workspace Clippy、architecture 通过 |
| PERF-03 | ContentHash 改为 32 字节值 | done | PERF-02 | clone 零分配、派生 UUID 不漂移 | 32-byte/Copy、wire/DB/hash/UUID golden、workspace Clippy/architecture 通过 |
| PERF-04 | TokenKey、DataPlaneIndex、TokenSlot | done | PERF-01 | 热路径不 hash/clone TokenId | 不可变索引/稳定 slot/seqlock/borrowed read；normalize→apply 使用 TokenKey；Clippy/architecture 通过 |
| PERF-05 | 批量 ingress 与 8 partition actor | done | PERF-04 | 删除 512 worker 和逐事件 dispatch | 固定 8 actor；256 MiB budget；batch/cap/backpressure/barrier 测试；Clippy/architecture 通过 |
| PERF-06 | 统一 ledger 与固定宽度 hash | done | PERF-03/05 | 无 checkpoint JSON/JCS | 唯一 ledger/固定 hash/typed replay/Source Slice format 2；Clippy/architecture 通过 |
| PERF-07 | persistence coordinator/commit cursor | done | PERF-06 | 无逐行/逐批 ACK channel | 唯一 coordinator；8 watch cursors；8192 rows/20ms；Loom 与 workspace 全门禁通过 |
| PERF-08 | 单写者 book/freshness/summary | done | PERF-05/07 | 热状态无 Mutex/DashMap | actor-owned book/telemetry；O(n+m) merge；summary/property/benchmark；全门禁通过 |
| PERF-09 | LastTrade ledger materialized view | done | PERF-06/07 | canonical batch 只等待 ledger | 删除第二 durable ACK；26.1+ MV；async retry/source+MV exact-once 集成测试；Clippy/architecture 通过 |
| PERF-10 | SessionHub 与 ByteString fanout | done | PERF-01 | 无全 session 扫描/String fanout | actor/index/refcount snapshot；ByteString 单分配；慢客户端语义；10K/1K smoke 45.596–49.836µs；production-stack PASS |
| PERF-11 | destructive L2 reset/cutover | done | PERF-06/09 | 旧表/reader/hash 全删除 | 无 v2/迁移；双确认 clean bootstrap；PG/CH/Redis 隔离全流程 260.90s PASS |
| PERF-12 | 全仓 clone/borrow/move 治理 | done | PERF-02/03 | clone lints 全仓 deny | 三项 clone lint deny；Arc slice 大批共享；ArcSwap/blocking/UUID/WS 架构门禁；workspace Clippy PASS |
| PERF-13 | 报告/训练/CPCV 性能治理 | superseded | PERF-12 | 计算 SLO 通过 | 数据布局优化有效；CPCV 仅覆盖 orchestration、report 仅覆盖 early-terminal，不能签收完整计算 SLO |
| PERF-14 | feature/线程池/固定 jemalloc | superseded | PERF-13 | serving 裁剪与预算通过 | 依赖裁剪与 allocator 已完成；线程常量不等于全局 CPU/内存执行预算 |
| PERF-15 | 全负载验收与规范收口 | superseded | PERF-08/10/11/14 | 所有门禁通过 | 深度复核发现 P0 correctness 与真实 load harness 缺口，不能直接进入 fixed-runner 签收 |
| PERF-16 | session continuity 与跨分区 fail-closed | done | PERF-15 | poison 全局持久、重启代次有效、跨分区投递无部分提交 | 完整 UUID + 单调 epoch；ShardAssignment restart generation；进程级 SessionDirectory；按 session 拆批并原子预留全部 mailbox；durable publish 前后三重 ticket fence；API/Core/architecture 门禁通过 |
| PERF-17 | Fresh/LastKnown book 语义隔离 | done | PERF-16 | invalid book 无法进入 report/execution | coherent ArcSwap/seqlock/session fence；fresh-only port；诊断 LastKnown；资金路径与 WS tombstone；Loom/并发/全 core 门禁通过 |
| PERF-18 | token retirement 与 SessionHub 背压 | done | PERF-17 | mutable book 可回收、control 不被 fanout 饥饿 | transport ownership retirement barrier；三 lane biased hub；共享 64MiB frame permit；10K churn/fanout 门禁通过 |
| PERF-19 | 集中 ComputeExecutor 资源治理 | done | PERF-18 | CPU/内存预算在 composition root 强制执行 | 唯一 serving/offline executor；CPU/memory/job lease；取消边界；语义 architecture gate；workspace Clippy 与定向测试通过 |
| PERF-20 | ClickHouse durability/idempotency 收口 | superseded | PERF-19 | 26.5 allowlist、bounded retry、duplicate readiness | 用户明确取消，不实施、不签收 |
| PERF-21 | 可复核性能 harness 与 CI evidence | in_progress | PERF-19 | full-network load、JSON/HDR artifact、独立 workflow | 统一 release smoke 已通过且 artifact/hash 可复核；待 fixed Linux runner 生成 full/soak CI artifact |
| PERF-22 | fixed-runner/预生产硬验收与封存 | superseded | PERF-21 | 全部 SLO 与 24h ReportOnly soak 真实通过 | 用户明确取消，不实施、不签收 |

## 决策记录

| 时间 | 决策 | 原因 |
|---|---|---|
| 2026-07-22 | 最终表名为 `quant_book_l2_ledger` | 统一语义名，不使用版本后缀 |
| 2026-07-22 | 不保留旧 L2 历史 | 用户明确选择 clean reset |
| 2026-07-22 | 固定 8 partition actor | 8 vCPU 生产门禁；不按 token 数建 worker |
| 2026-07-22 | ClickHouse 最低版本锁定为 26.1 | 26.1 才修复 acknowledged async-insert retry 在 dependent materialized view 上的端到端去重；旧版本 fail closed |
| 2026-07-22 | PERF-13～15 superseded，新增 PERF-16～22 收尾链 | 深度复核证明已有局部门禁未覆盖 session poison、invalid book 资金边界、完整业务负载和全局资源预算；禁止用缺 runner 掩盖实现缺口 |
| 2026-07-22 | PERF-20 与 PERF-22 superseded，PERF-21 直接依赖 PERF-19 | 用户明确取消 ClickHouse durability/idempotency 收口与 fixed-runner/24h rollout；不得把未实施内容记为 done 或纳入最终闭环声明 |
| 2026-07-22 | SessionHub 固定预算使用 private compile-time constants，不进入 DeployConfig | 这些值只有一个已验证组合；伪可配置会制造不存在的受支持状态空间。architecture check 锁定精确常量并拒绝 `WebSocketHubConfig` |
| 2026-07-22 | 不在本地执行真实数据删除 | 只实现并测试强确认 reset 工具 |
| 2026-07-22 | PERF-11 采用整库 clean bootstrap，不实现 `v2`、ALTER 数据迁移或兼容读写 | 系统从未正式投产；用户明确允许删除未封存的 PostgreSQL/ClickHouse 项目库并重跑唯一 bootstrap。已有 production baseline 时仍永久拒绝 reset |
| 2026-07-22 | `ContentHash` 内存态只保留 raw BLAKE3-256 | canonical text 仅在 wire/DB/日志边界生成；内部比较、排序和 hash 组合使用固定字节 |
| 2026-07-22 | WS adapter 通过注入的 `TokenKeyResolver` 读取核心不可变索引 | 保持 API→core 依赖方向；未知 U256 使整个 normalized message/session fail closed，不再维护全局 DashMap intern pool |
| 2026-07-22 | session close 使用冷路径 partition drain barrier | 删除全局 invalid-session `DashSet`；close 读取 TokenSlot continuity 前等待相关 partition 已处理先前事件，250ms 超时整 session fail closed |
| 2026-07-22 | Source Slice 只保留一个 `L2Ledger` object，manifest format 直接切到 2 | 旧 source slice/training/replay 证据按决策全部失效；禁止旧 event/checkpoint 双对象 decoder |
| 2026-07-22 | CPCV fold/trial 统一在 service-owned 2-thread Rayon pool 执行 | 防止 fold 并行×trainer 并行乘法扩张，将每 worker scratch 和 CPU 预算固定在 8 vCPU 方案内 |
| 2026-07-22 | 所有目标进程无条件固定 `tikv-jemallocator`，删除 system fallback、mimalloc 和 allocator features | 用户在 A/B 后明确拍板；用唯一 `quant-pivot-allocator` 确保服务、工具、benchmark 和测试 harness 使用同一 allocator；proc-macro 是加载进 rustc 的宿主插件，不进入目标进程 allocator 图 |

## 执行记录

### PERF-00

- [x] 确认工作树和当前分支。
- [x] 写入冻结设计。
- [x] 创建可恢复台账。
- [x] 更新设计索引和操作手册。
- [x] 将任务标记为 `done` 并推进 PERF-01。

### PERF-01

- [x] 复现 release benchmark 中 debug-only hook 的无条件导入错误。
- [x] 为 `portfolio/lp.rs` 的 debug-only hook 增加 `cfg(debug_assertions)`。
- [x] `cargo check --release -p quant-pivot-research -p quant-pivot-bench --benches`
  通过（2m56s，首次 release 依赖编译）。
- [x] `cargo bench -p quant-pivot-bench --bench hot_paths -- --warm-up-time 1
  --measurement-time 3 --sample-size 20` 通过；macOS 当前基线
  `book_store_apply_snapshot = 135.12–137.51ns`。
- [x] 记录构建面问题：单一 BookStore benchmark 当前会链接 research/Polars 栈，作为
  PERF-14 feature 裁剪输入。

### PERF-02

- [x] 确认 `ids.rs` 恰有 81 个 `Arc<Uuid>` 内部 ID。
- [x] 重写 `UuidId` derive：`Uuid` 值语义、`const` 访问器、零中间
  `String` Serde visitor 和 native UUID SeaORM 绑定。
- [x] 将 81 个声明改为 `Uuid` 并显式派生 `Copy`；删除全仓所有因此产生的
  `clone_on_copy` / `cloned_instead_of_copied`。
- [x] 增加 16-byte、无 Drop、Copy、JSON、bitcode、bincode、SeaORM 回归测试。
- [x] `cargo test -p quant-pivot-models types::ids::tests`：11 passed。
- [x] `cargo clippy --workspace --all-targets -- -D warnings`：通过。
- [x] `cargo xtask architecture check`：通过。

### PERF-03

- [x] 盘点 `ContentHash` 构造、Serde、DB/ClickHouse 与 UUID v5 派生 contract。
- [x] 实现 `[u8; 32]`、`ContentHashText`、严格 parse 和 raw digest API；
  `ContentHash` 为 32 字节、`Copy`、无 `Drop`。
- [x] 新增 `ChDigest([u8; 32])` 作为 `FixedString(32)` RowBinary 边界。
- [x] 删除 `ContentHash::as_str()`；raw byte hashing 不再执行
  `Decimal/String/hex String -> parse` 式往返。
- [x] 保持 JSON、bitcode、bincode、SeaORM canonical
  `blake3:<64 lowercase hex>` contract，并增加 BLAKE3 `abc` 跨平台 golden。
- [x] 为 10 个既有 content-addressed UUID v5 派生增加精确 golden，验证无漂移。
- [x] 清除 `ContentHash`/`Option<ContentHash>` 全仓 `clone_on_copy`，并顺带缩小两个
  大 future 的栈状态和 `DomainSliceData` 大枚举搬移成本。
- [x] `cargo test -p quant-pivot-models types::content::tests`：14 passed。
- [x] `cargo test -p quant-pivot-models types::ids::tests`：12 passed。
- [x] `cargo test -p quant-pivot-models
  clickhouse::types::tests::digest_maps_content_hash_to_fixed_32_bytes`：1 passed。
- [x] `cargo clippy --workspace --all-targets -- -D warnings`：零诊断通过。
- [x] `cargo xtask architecture check`：通过。

### PERF-04

- [x] 盘点 `MarketRegistry`、`BookStore`、freshness board 和 WS token resolve 路径。
- [x] 新增 4-byte `TokenKey`、1-byte `PartitionId`、8-byte `PartitionBatchId`。
- [x] 将 `MarketRegistry` 的三个 `DashMap` 和 active list 合并为一个经
  `ArcSwap` 原子发布的不可变 `DataPlaneIndex`；Gamma 冷路径完整重建快照。
- [x] TokenKey 只追加不重排，`TokenSlot` 跨 catalog rebuild 保持同一地址；同时维护
  `U256 -> TokenKey` 与 `TokenId -> TokenKey` 边界索引。
- [x] `BookStore` 改为 stable slot facade；`read(TokenKey, closure)` 使用
  `ArcSwap::load()` guard，`load_owned` 仅用于跨任务/await 所有权边界。
- [x] 未注册 token 拒绝 apply，不再从摄取热路径动态扩张 map。
- [x] 删除 WS `TokenFreshnessBoard/RwLock<HashMap>` 和全局
  `Mutex<Option<Instant>>`；全局 message tick 使用 `AtomicU64`，per-token freshness
  使用 odd/even version seqlock，session invalidation 回调传完整 token scope。
- [x] MarketRegistry/BookStore/DataBundle/系统测试/benchmark 全部显式共享同一个
  `Arc<DataPlane>`，禁止测试夹具产生分裂真相。
- [x] `cargo test -p quant-pivot-core ingest::data_plane_index`：4 passed，包含并发
  torn-read 检查、slot/key rebuild 稳定性和 U256 lookup。
- [x] `cargo test -p quant-pivot-core ingest::book_store`：3 passed；
  `ingest::market_registry`：6 passed；`cargo test -p quant-pivot-api ws::shard`：3 passed。
- [x] `cargo clippy --workspace --all-targets -- -D warnings` 与
  `cargo xtask architecture check` 通过。
- [x] macOS release `book_store_apply_snapshot`：126.15–127.53ns，相对冻结基线改善
  2.37%–7.82%；borrowed `BookStore::read`：5.0887–5.1052ns。
- [x] 删除 WS `TokenInternPool/DashMap`；注入 `TokenKeyResolver`，normalize 遇到未知
  U256 时整条消息返回 `UnregisteredToken` 并触发 session reconnect。
- [x] `PipelineEvent` 的 book/delta/tick/trade/gap/resolution 载荷改用 Copy
  `TokenKey`；worker affinity、sequence map、delta grouping、freshness/apply 均不再 hash/clone
  `TokenId`，只在 ClickHouse/transport 边界恢复 `TokenId`。
- [x] `cargo test -p quant-pivot-api ws::normalize`：8 passed（含 unknown-token
  fail-closed）；`ws::shard`：3 passed。
- [x] TokenKey 化后再次执行 workspace Clippy 和 architecture check：通过。

### PERF-05

- [x] 删除动态 `book_apply_topology`、`MAX_BOOK_SHARD_COUNT=512`、动态 channel
  容量和逐事件 shared-ingress dispatch。
- [x] WS normalizer 将一条 WS message 的全部事件分配 sequence 后只发送一个
  `NormalizedIngressBatch`；session close 与全部 gap 也合并为一个 ingress batch。
- [x] 建立 8 个固定 token-affine Tokio partition actor，路由恒为
  `TokenKey % 8`；每 partition mailbox 为 256 batches。
- [x] 全部 WS shards 共享 256 MiB semaphore byte budget（1 permit = 1 KiB）；
  source batch 拆到多个 partition 后共享 permit，最后一个 partition 完成才释放。
- [x] 实现 1,024 events / 1 MiB partition batch cap；单事件超过 1 MiB 或预算、
  shared ingress、partition mailbox 在 250ms 内不可用时 fail closed。
- [x] actor 处理完成后回收 `Vec<PipelineEvent>` allocation；queue depth 仅在错误边界采样。
- [x] 删除全局 invalid-session `DashSet`；session close 通过仅在冷路径执行的 partition
  drain barrier 建立 happens-before，再以 TokenSlot session/sequence/state 判断 continuity。
- [x] shutdown 先 drain shared ingress quiet period，再 drop partition senders；actor 将已入队
  batches 处理完毕后自然退出。
- [x] 定向测试：ingress accounting/permit lifecycle 2 passed；WS batch/backpressure/session
  close 5 passed；partition affinity/cap/sequence/barrier 5 passed。
- [x] `cargo clippy --workspace --all-targets -- -D warnings`：通过。
- [x] `cargo xtask architecture check`：通过。
- [x] 命令修正记录：最初把两个 Cargo test filter 放在同一命令导致参数错误；已拆为
  两条独立命令并全部通过，不涉及代码或环境失败。

### PERF-06

- [x] 盘点旧 `BookL2EventRow`、`BookL2CheckpointRow`、JCS hash、reader/repository 和迁移面。
- [x] 实现唯一 `BookL2LedgerRow` typed arrays 与 `FixedString(32)` digest。
- [x] 实现 domain-separated fixed-width BLAKE3 encoder；Snapshot/Delta/Tick/Gap/LastTrade 精确 golden 已固化。
- [x] writer 的 snapshot/delta/tick/gap/last-trade 全部改写到 `quant_book_l2_ledger`；删除盘口 JSON/JCS hash。
- [x] repository API、PIT replay、historical prefetch 和测试夹具改用 typed arrays；不再解析盘口 JSON。
- [x] Source Slice 改为唯一 `L2Ledger` object；manifest format=2、schema hash domain=v2，不读旧双对象证据。
- [x] 删除旧 row 源文件、旧表名、旧 source object kind 和旧 repository query-limit 命名。
- [x] `cargo check --workspace --all-targets`：通过。
- [x] 定向测试：ledger hash 2、typed replay 4、ReplayPage 1、writer 8，全部通过。
- [x] `cargo clippy --workspace --all-targets -- -D warnings`：通过。
- [x] `cargo xtask architecture check`：通过。

### PERF-07

- [x] 盘点 DurableWriter 的逐 row ACK、现有批处理边界和 shutdown 语义。
- [x] 实现唯一 `LedgerPersistenceCoordinator`、8 个常驻 commit cursor 和 batch request。
- [x] 删除 canonical path 的 `join_all` 与 row/batch 临时 ACK channel；ledger 不再创建 flume ACK。
- [x] ClickHouse ledger 使用 borrowed batch insert，启用 `async_insert=1`、`wait_for_async_insert=1` 和 100ms server flush timeout；聚合 buffer 保留 capacity。
- [x] 失败状态携带 generation；client 用 `borrow_and_update()` 处理 jump/close，并在 timeout 后先 reconcile 唯一 in-flight batch。
- [x] coordinator cancellation 时 drain 已入队 request；partition 在 durable commit 后才执行 projection/apply，失败使相关 session 整批失效。
- [x] `cargo test -p quant-pivot-core observability::ledger_persistence`：4 passed（含 Loom notification/read race model）。
- [x] `cargo test -p quant-pivot-core ingest::data_pipeline`：5 passed。
- [x] `cargo check --workspace --all-targets`、`cargo clippy --workspace --all-targets -- -D warnings` 和 `cargo xtask architecture check`：通过。

### PERF-08

- [x] 将 mutable book、publish version 与 microstructure accumulator 从 `TokenSlot`/`BookStore` 移入唯一 partition actor。
- [x] 批内按 `TokenKey` 排序分组，delta 按 side/price 排序、最后值去重，再以 O(n+m) merge；partition 复用 command/change/merge scratch。
- [x] 每个变化 side 最多创建一个新 `Arc<[BookLevel]>`；未变化 side 保持 `Arc::ptr_eq`。
- [x] `BookSnapshot` 一次 side scan 预计算 best bid/ask、spread、mid、top1/5/20 USD depth、top5 share imbalance、crossed 与 total depth。
- [x] microstructure 直接消费 summary；移除 apply 后 `load_owned()`、重复盘口 scan 和全局 `DashMap<TokenId, ...>`。
- [x] partition accumulator 同秒合并，1s timer 写出 quiet completed bucket，shutdown flush 当前 bucket。
- [x] 删除 `TokenSlot::live: Mutex<OrderBook>` 和 `BookStore::apply_snapshot/apply_delta`；snapshot 与 freshness tuple 在同一 odd/even write section 内发布。
- [x] `cargo test -p quant-pivot-core ingest::order_book`：6 passed，含 proptest/reference map、重复价位、删除与 Arc side reuse。
- [x] `cargo test -p quant-pivot-models domain::market::book`：6 passed；`observability::book_fact_writer`：10 passed；`ingest::data_pipeline`：5 passed。
- [x] macOS release `book_store_read_borrowed`：5.0950–5.1185ns；固定 summary 优化后的 `book_store_publish_snapshot`：208.69–209.11ns，较本阶段首轮 220.40–221.84ns 改善 5.36%–6.20%。
- [x] `cargo clippy --workspace --all-targets -- -D warnings` 与 `cargo xtask architecture check`：通过。
- [x] 失败修复记录：按 Clippy 建议将纯访问器/构造器改为 `const fn`，用 let-else 提前释放带 permit 的 message；system-test seed 全部迁移到新 publish API，未增加兼容入口。

### PERF-09

- [x] 盘点 MarketWs LastTrade 当前 ledger commit 后的第二次 DurableWriter ACK 与 trade tape schema。
- [x] 创建 `quant_book_l2_ledger` → `quant_trade_tape` materialized view，只投影 `LastTrade`。
- [x] 删除 canonical MarketWs `write_last_trade_projection`、逐 row ACK 与 source-event 字符串格式化。
- [x] 保留 OnChain trade tape 直接写路径并验证 source 语义隔离。
- [x] 验证每个 LastTrade 逻辑投影一次、重试幂等/去重契约、workspace Clippy 与 architecture check。
- [x] manifest 从隔离的 ClickHouse 26.5 clean database 实际 DDL 再生成；临时数据库随后删除，未触碰现有 `quant_pivot` 数据。
- [x] `quant_book_l2_ledger` 与 `quant_trade_tape` 启用 10,000-block non-replicated dedup window；async writer 显式设置 `async_insert=1`、`wait_for_async_insert=1`、`async_insert_deduplicate=1` 与 100ms flush timeout。
- [x] runtime、schema plan/apply/verify/manifest 对 ClickHouse `<26.1` fail closed；parser/版本门槛单测通过。
- [x] 真实 `clickhouse-server:26.5` infrastructure suite：重复提交相同 LastTrade 两次后 ledger=1、trade tape=1，且 hash、side、price、size、fee、coverage、session/sequence 全字段投影正确；全套 1/1 scenario suite 通过。
- [x] `cargo test -p quant-pivot-core book_fact_writer` 10 passed；`data_pipeline` 5 passed；`cargo clippy --workspace --all-targets -- -D warnings` 与 `cargo xtask architecture check` 通过。

### PERF-10

- [x] 盘点 session 生命周期、订阅状态、fanout、subject/family close 与 watched-market polling 的所有调用链。
- [x] 设计并实现单写者 `SessionHub`、topic/subject/family/system-reader 倒排索引和 watched-market refcount snapshot。
- [x] outbound 改为 bounded `mpsc::Sender<ByteString>`；每事件只序列化/分配一次。
- [x] 实现 best-effort 与 reliable 慢客户端语义、取消与 metrics。
- [x] 删除 session 全扫描、订阅锁和 `Sender<String>`，迁移 coalescer/readers。
- [x] 补齐 10K session/topic/churn/close/slow-client 行为与 benchmark，运行 workspace 门禁。
- [x] 注册/订阅/取消/subject/family/close-all 通过 bounded actor command 与 control-only oneshot completion；fanout 不创建 ACK，`MarketBookUpdate/SystemStatus` 队列满时计数并丢帧，其余 lifecycle/alert 队列满时取消并移除慢 session。
- [x] `ByteString::from(String)` 与 recipient clone 指针一致测试通过，证明编码 String 原 allocation 被接管且接收者 clone 共享底层 bytes。
- [x] `cargo test -p quant-pivot-web ws::tests` 6 passed；book coalescer 5 passed；全 workspace Clippy 与 architecture 在行为实现后通过。
- [x] 真实 production binary `web_system_contracts` 1/1 passed，覆盖 ticket、upgrade、hub registration、pong、disconnect 与 ticket replay fail-closed。
- [x] macOS release `session_hub_10k_sessions_1k_topic_fanout`：45.596–49.836µs/事件；包含 1K shared-frame enqueue/receive，远低于 2ms smoke 目标，但 Linux fixed-runner p99 仍留 PERF-15 硬验收。
- [x] 环境故障记录：production-stack 成功退出后其后台 `cargo clean` 与随后一次 architecture rebuild 竞态，出现 `polars-core dep-info No such file or directory`；确认磁盘充足并等待 clean 完成。该错误不来自源码，此前独立 architecture PASS 与 production-stack PASS 均有效，最终门禁将在新 target 上重跑。

### PERF-11

- [x] 盘点现有 preproduction reset、lifecycle lease、schema/readiness 和研究证据生命周期；确认完整 clean bootstrap 会物理删除全部未投产 PG/CH 项目事实与 Redis `qp:*` 状态，不需要数据迁移或证据逐表失效。
- [x] 将现有唯一 `preproduction-reset plan/apply/verify` 收紧为显式整库确认；确认文本准确表达删除全部未投产数据并重跑 bootstrap，且与一次性 nonce 同时匹配。
- [x] plan/apply 两次验证 PostgreSQL 无项目连接、ClickHouse 无项目查询或 server mutation；production baseline、非精确 DB/user/schema、inventory 漂移均 fail closed。
- [x] reset 后只应用唯一 ledger/MV bootstrap；验证 ledger 为空、旧对象/marker 消失、研究证据不存在且 readiness fail closed，不保留 reader、双写、hash 或 schema 兼容。
- [x] 在隔离 ClickHouse/PostgreSQL/Redis 验证错误确认、活动 writer/query 拒绝、整库删除、唯一 bootstrap 启动与 production baseline 拒绝；未触碰当前配置数据库。
- [x] 失败记录（2026-07-22）：首次隔离系统测试中，模拟 ClickHouse active query 仅持续约 10 秒，在 xtask 子进程完成启动前自然结束，导致 plan 合法通过并使测试断言失败；数据库实现未发生错误。恢复命令：延长受控查询窗口后运行 `cargo test -p quant-pivot-system-tests --test preproduction_reset_recovery -- --nocapture`。
- [x] 失败记录（2026-07-22）：延长查询后 plan 已按预期拒绝，但客户端 task abort 与 ClickHouse 从 `system.processes` 移除查询之间存在短暂异步窗口，下一轮 plan 因仍见 1 个 active query 而正确 fail closed。恢复命令：等待服务端 active count 归零后复跑同一隔离测试。
- [x] 失败记录（2026-07-22）：单纯 abort ClickHouse HTTP future 在 5 秒内仍未保证服务端查询取消；测试清理超时，但 reset 始终正确拒绝。恢复命令：fixture 绑定唯一 query id，使用 `KILL QUERY ... SYNC` 后确认 `system.processes` 清零，再复跑隔离测试。
- [x] `cargo test -p quant-pivot-system-tests --test preproduction_reset_recovery -- --nocapture`：1 passed，260.90s；覆盖双确认、live-owner 拒绝、PG/CH/Redis 分阶段故障恢复、空 ledger/空研究证据/readiness fail closed、备份恢复和 production seal 后 reset 拒绝。
- [x] `cargo clippy -p quant-pivot-xtask -p quant-pivot-system-tests --all-targets -- -D warnings` 与 `cargo xtask architecture check`：通过。

### PERF-12

- [x] 初始盘点 3,521 个显式 `.clone()`、1,086 个 `Arc::clone`，并逐类审计 Copy ID、
  string-backed boundary ID、SeaORM owned model、共享 handle、queue/task payload 和测试夹具；
  最终静态计数为 3,519 / 1,105，编译器证明的冗余 clone 为零。
- [x] workspace 启用 `clone_on_copy`、`clone_on_ref_ptr`、`redundant_clone = deny`；修复全部
  裸 Arc clone，UUID/ContentHash 的 Copy clone 由编译期拒绝。
- [x] 将 factor/model report 热路径的 aligned/routed vectors 改为 `Arc<[FeatureVector]>`；
  删除进入 `spawn_blocking` 前的整批 `to_vec()`，route 只为实际子集建立一次 owned slice。
- [x] 审计 9 个 `ArcSwap::load_full()`：同步 bootstrap/kill-switch view 改为 `load()` guard；
  仅在返回 owned Arc、缓存或跨 await/task 的边界保留 `load_full()`。
- [x] 盘点所有生产 `spawn_blocking` 入口；archive decoder 已由 request-local bounded mpsc(2)
  约束，research/training/backtest/CPCV 已由 research job global/per-kind cap 约束。新增 AST
  allowlist，未声明 CPU/内存预算的新增入口使 architecture check 失败。
- [x] architecture check 新增全仓 `Arc<Uuid>` 和 WebSocket `Sender<String>` 禁令；20 个
  architecture 单测和实际 `cargo xtask architecture check` 通过。
- [x] 项目 `AGENTS.md`、Cursor Rust rule 和全局 `~/.codex/AGENTS.md` 固化值语义、borrow/move、
  ArcSwap guard、bounded actor/channel、blocking/inline/allocator 证据规则。
- [x] `cargo check -p quant-pivot-core -p quant-pivot-system-tests --all-targets` 通过；
  `cargo test -p quant-pivot-core service::model_runner::tests --lib` 6 passed；workspace Clippy
  在三项 clone lint deny 下零诊断通过；architecture check 通过。
- [x] 命令修正记录：首次向 `cargo test` 同时传两个位置 filter 被 Cargo 拒绝；改用统一
  `architecture::tests` filter 后 20/20 通过，不涉及源码或测试行为失败。

### PERF-13

- [x] 盘点报告、训练矩阵、经典模型和 CPCV 的数据布局、复制次数、峰值 RSS 与并行策略。
- [x] 将 `TrainingMatrix`/`DenseInputMatrix` 改为单一 row-major 连续存储；`ModelInputCell`
  为 Copy 且限定 24 bytes；transform 一次分配，必填数值使用单遍 Welford，
  smartcore 直接消费 flat `Vec<f64>`，不再 `Vec<Vec<_>>` 二次拷贝。
- [x] 保持 `training_input_hash` 既有 nested-row wire contract，自定义 Serialize 直接
  遍历连续 slice；回归测试与旧 `Vec<Vec<_>>` hash 字节一致。
- [x] ablation importance 改为就地清零/恢复单列，每列仅保留 `rows×8`
  scratch，删除每列整矩阵 clone。
- [x] training request 使用 `Arc<[TrainingExample]>`；Parquet decode 后 Vec 零拷贝
  转 Arc，进入 blocking 边界只做 `Arc::clone`。
- [x] CPCV 将时间组映射预计算为 `Arc<[Range<usize>]>`，fold 合并已排序
  group index 后直接遍历连续区间；删除每 fold `HashSet` + 全量百万行扫描和
  `parquet_examples.clone()`。经典模型 fold 从 borrowed range 直接建矩阵。
- [x] `TrialPerformanceMatrix` 改为带 shape 校验的连续 row-major Decimal 存储。
- [x] 定向测试：training matrix 8 passed；classical 5 passed；PBO 6 passed；
  CPCV 8 passed；`cargo check -p quant-pivot-core --features ml-classical --all-targets` 通过。
- [x] macOS release 单次门禁：`target/release/training_matrix_gate 1000000`
  输出 transform 13.310s，process 13.55s，maximum RSS 3,624,222,720 bytes
  (3.375 GiB)；低于 60s / 8GiB 硬阈值。
- [x] Criterion 10K×128 smoke：60.701–61.307ms，20.879–21.087M cells/s。
  1M 的重复 Criterion 预估单 sample 约 16s，不适合 RSS 门禁，在 warm-up
  后主动中止；弃用该测量方法，改用上述 single-shot binary。
- [x] CPCV purge 从每 row 扫描全部 test groups 的近似 O(n×t) 改为合并
  purge/embargo 时间区间后 O(n+t log t)；与旧二次参考语义的回归测试保持一致。
- [x] CPCV partition 从 `Vec<Vec<usize>>` 改为 `Vec<Range<usize>>`；
  evaluation 一次排序/精确 coverage 校验，path reconstruction 从每 group 线性
  `find` 改为连续 slice，且 `Option<&GroupEvaluation>` 禁止 rank vector 深拷贝。
- [x] `GroupRowFilter` 冻结为严格递增/唯一 contract，fold 不再 clone/sort 百万行
  index；trial replay 从 `BTreeMap` 改为直接 index vector，PBO 从 column 一次
  transpose 到 flat matrix，删除每 period 一个 Vec 的百万次分配。
- [x] `CpcvBacktestService` 长期持有 2-thread 专用 offline Rayon pool，fold 和 trial
  grid 均在该预算内执行，不污染 global Rayon pool。
- [x] 1M-row/10-partition release 门禁：45 combinations、9 paths，核心
  0.564s，process 1.15s，maximum RSS 808,091,648 bytes (0.753GiB)，低于
  300s / 10GiB 门禁。
- [x] 失败/恢复记录：CPCV 定向测试首次编译发现测试将临时
  `GroupRowFilter` 借用越过语句（E0716）；改为命名 binding 后 9/9 通过，
  生产代码未出现该问题。两次向 Cargo 同时传入多个 test filter 的命令错误
  已改为单 filter/分开运行，不涉及源码失败。
- [x] Report funnel 索引从 `BTreeMap` 改为预分配 `AHashMap`，最终 row 一次
  `sort_unstable_by(market_id)` 保持完全确定的持久化顺序。
- [x] 2K market report pure-compute release 门禁：5 warm-up + 100 samples，
  median 19.773ms，p99 20.609ms，maximum RSS 11,927,552 bytes，低于 2s 门禁。
- [x] release gate 命令、阈值、macOS 原始数据和 Linux `/usr/bin/time -v`
  恢复命令已写入 `docs/operations/performance.md`。
- [x] PERF-13 功能与性能证据齐全，状态更新为 `done`。

### PERF-14

- [x] 将 serving 与 research-jobs/ML 依赖面精确切分，不保留旧 `dataframe` feature alias；
  serving-only dependency tree 共 2,423 行，Polars/AWS SDK/Chainlink SDK/SmartCore/Argmin
  命中为 0，serving-only Clippy 通过。
- [x] 固定 Tokio 3、Actix 1+1 blocking、serving compute 2、offline Rayon 2、
  Tokio max blocking 4 的 8-vCPU 预算，并增加启动/架构门禁。
- [x] 同一 macOS release 语料 A/B：system 为 training 13.466s/3,590,029,312B、
  CPCV 0.537s/720,158,720B、report p99 20.496ms；mimalloc 为
  13.583s/4,319,854,592B、0.516s/734,052,352B、9.546ms；jemalloc 为
  13.493s/3,002,499,072B、0.543s/713,654,272B、13.583ms。
- [x] 按用户最终决策删除 mimalloc 与 allocator feature；新增唯一
  `quant-pivot-allocator`，所有目标进程 crate 强制链接固定 jemalloc。Cargo.lock 无
  mimalloc，架构门禁拒绝 system fallback、第二 allocator 和 allocator feature；
  `quant-pivot-macros` 作为 rustc host plugin 明确排除，避免把目标 allocator 注入编译器进程。
- [x] 删除 ClickHouse schema 中冗余 legacy forbidden-object 清单；clean bootstrap 的
  exact-object inventory 已拒绝任何额外对象，不再在生产源码保留旧 L2 或版本后缀名称。
- [x] `cargo clippy --workspace --all-targets -- -D warnings` 通过；architecture 单测
  20/20、CPCV 定向测试 9/9、实际 architecture check 通过。固定 jemalloc release gate
  数据即上述 A/B 的 jemalloc 组。

### PERF-15

- [x] `cargo check --release --workspace` 通过（2m06s）。
- [x] 首轮 `cargo test --workspace` 已通过 API 142、Core 381、Error 20 等测试，随后
  `quant-pivot-macros` test harness 以 `SIGABRT` 退出。根因是此前将目标 allocator
  policy crate 链入 proc-macro 宿主插件；已删除该依赖与 force-link，并将架构规则改为
  只要求全部目标进程 crate。恢复命令：`cargo test -p quant-pivot-macros --lib &&
  cargo test --workspace`。
- [x] 修复后 `cargo test -p quant-pivot-macros --lib`、`cargo fmt --all -- --check`、
  `cargo xtask architecture check` 和 `cargo clippy --workspace --all-targets -- -D warnings`
  均通过；Clippy 重建 allocator 下游依赖图耗时 5m27s。
- [x] 第二轮 `cargo test --workspace` 已越过 allocator/macro，API 142、Core 381、
  Research 503 等通过，随后 storage 发现两处收口失配：clean-bootstrap SQL 的冻结
  checksum 未同步、LastTrade MV 测试对 SQL 换行敏感。按未投产且唯一 bootstrap 决策
  更新 checksum，并将断言改为 whitespace-normalized SQL；`cargo test -p
  quant-pivot-storage --lib` 38/38 通过。
- [x] 后续 system test 暴露旧 fixture 依赖 Decimal/f64 舍入残差产生候选；保留生产
  `net == 0` 不发信号语义，将要求候选的 model-runtime 场景改成 6-market 离散
  cross-section。report harness 原本声明 `min_size=2`，但 artifact 错冻 default 5；已统一
  为 governed factor contract。drawdown 测试同时改为直接断言 provenance `0.8`，并对两次
  已量化报告的派生比较允许一个 scale-12 quantum。`core_business` 全场景通过。一次
  PostgreSQL testcontainer 首连超时在重试后通过，期间未进入业务场景、无代码调整。
- [x] 完整 workspace 第三轮已通过 library/core_business/fresh-stack/infrastructure，随后
  reset/recovery 在业务执行前发生一次 disposable ClickHouse 120s startup timeout；不调整
  业务代码，定向重跑 `preproduction_reset_recovery` 完整通过（255.69s）。
- [x] 跳过已定向通过的 reset 继续全仓时，`repository_contracts` 巨型 async harness 在首个
  scenario 前 stack overflow。各 scenario 已单独 boxing，但包含 150+ 顺序调用的 outer
  `with_postgres_suite` future 仍在测试线程栈上；增加与 core harness 相同的 outer
  `Box::pin` 边界后，全部 repository persistence scenarios 24.94s 通过。
- [x] 固定中央 jemalloc 后重新执行 macOS 计算 smoke：1M×128 transform 13.093s、
  process 14.14s、RSS 3,570,515,968B；1M-row/10-partition CPCV 0.536s、process
  0.97s、RSS 718,405,632B；2K-market report median 12.585ms、p99 13.157ms、
  RSS 11,878,400B。均通过对应 macOS smoke 阈值，不能替代 Linux 硬验收。
- [x] SessionHub 10K sessions/单 topic 1K subscribers 的 fixed-jemalloc macOS
  Criterion smoke 为 45.596–49.836µs/事件，payload 只创建一次并共享入 1K outbox；
  远低于 2ms smoke 目标，Linux fixed-runner p99 仍需单独留证。
- [x] fixed-jemalloc macOS Criterion hot-path smoke：ArcSwap guard 借用读取
  `book_store_read_borrowed` 为 5.1444–5.1610ns，snapshot publish 为
  198.19–199.00ns；前者无 refcount 增减且远低于 10µs smoke 目标。
- [x] 最终不跳项 `cargo test --workspace` 通过：reset/rebootstrap 231.72s、
  repository persistence 21.87s、production Web boundary 348.22s；其余 workspace
  unit/integration/doc tests 全部通过。macOS 链接器对巨型 system-test/bin artifact
  报 `__eh_frame` compact-unwind 大小提示，不影响链接、Rust panic unwind 或测试结果。
- [x] 最终重跑 `cargo fmt --all -- --check`、workspace Clippy `-D warnings`、
  architecture check、release workspace check 和全部 macOS 计算/在线 hot-path smoke。
- [x] 静态审计确认业务源码/schema 无旧 L2 表名/row/JSON checkpoint、内部
  `Arc<Uuid>`、自有 `_v2` schema、mimalloc、allocator feature 或第二
  `#[global_allocator]`；命中只存在于 architecture 的禁止模式与测试 fixture。
- [ ] `blocked`：当前主机为 macOS arm64，无法伪造 8 vCPU/16 GiB Linux x86_64
  fixed-runner 的 10K sustained/50K burst、enqueue/durable-ack p99、在线 RSS、
  warm-cache E2E 和 Linux 性能证据；须在指定 runner 执行后才可将 PERF-15 标为 `done`。

### PERF-16

- [x] 将 shard watch payload 从裸 token set 改为 `ShardAssignment { tokens,
  restart_generation }`；相同 token set 的强制重启也会进入 `Resubscribe`，同一 shard 的
  批量 invalidation 只增加一次 generation；generation 溢出取消数据面，fail closed。
- [x] `IngressTrace` 与所有 session lifecycle event 改为完整
  `StreamSessionTicket { stream_session_id: Uuid, epoch: u64 }`；删除 UUID XOR 压缩，
  全局 epoch 使用 checked 单调分配，溢出取消数据面。
- [x] 新增进程级 ArcSwap `SessionDirectory`，注册完整订阅 scope；poison 是 epoch-scoped
  且不可被旧 UUID/新 epoch 重新打开，所有 partition 与 `BookStore` 共享同一 fence。
- [x] normalized batch 先按 physical session 拆分；每个 session batch 在发送前一次性
  reserve 全部目标 partition mailbox permits，任何 timeout 都不发送任何 partition batch，
  并 poison/restart 完整订阅 scope。
- [x] canonical 路径在 sequence acceptance 前、durable ACK 后及 slot publish 前后复核
  session；open ledger failure、gap、sequence discontinuity、commit timeout 均 poison 整个
  session。late commit 只能恢复 commit cursor，不能恢复已 poison 的旧 ticket。
- [x] 回归测试覆盖等 token 强制重启、restart/epoch overflow、按 session 拆批、全 mailbox
  reservation 失败零部分投递、poison 后旧 snapshot 拒绝、新 session snapshot 恢复、
  durable ACK timeout 后 late commit cursor recovery。
- [x] 验证：`cargo check -p quant-pivot-models -p quant-pivot-api -p quant-pivot-core`；
  `cargo test -p quant-pivot-api ws::` 29 passed；`cargo test -p quant-pivot-core ingest::`
  60 passed；ledger persistence 5 passed；`cargo xtask architecture check` passed。
- [x] 命令修正记录：两次尝试向单次 `cargo test` 传两个位置 filter 被 Cargo 参数解析拒绝；
  改为共同父 filter 或独立命令后全部通过，不属于源码/测试失败。

### PERF-17

- [x] 删除业务可直接取得裸 snapshot 的 `read/load_owned/load_by_id/load_pair`、
  top-of-book 与 published snapshot API；不保留 alias、forwarding re-export 或兼容 wrapper。
- [x] 新增 `FreshBook`、诊断专用且不实现 `Deref` 的 `LastKnownBook`，以及
  `BookUnavailable::{UnknownToken, Unseen, Invalid, Retired, PoisonedSession}`；跨 await/task
  只能显式调用 `load_fresh_owned`，同步热读使用 ArcSwap guard 的 `read_fresh`。
- [x] `TokenSlot` 在同一 odd/even 版本内采样 ArcSwap snapshot、sequence、session epoch、
  freshness/state/latency，并在回调/owned clone 后再次复核 slot version 与进程级
  `SessionDirectory` active epoch。发布、poison、invalidate 任一竞态都不能返回语义 Fresh。
- [x] admission、entry condition、report/model market-data port、structural monitor、exit
  monitor/dispatcher 全部迁移 fresh-only。exit monitor 缺 fresh book 时先持久化
  `ManualRequired`，再同步发送 Critical `TradingSafety` operator alert；不使用 last-known depth
  创建、签名、提交或猜测退出订单。
- [x] reconciliation 与 data-quality 只能显式消费 `LastKnownBook` 诊断信息；unavailable token
  计入 insufficient 分母。WebSocket coalescer 在 invalid/poison 时发送 unavailable tombstone，
  客户端不会保留旧价。
- [x] system-test/benchmark fixture 不再绕过 session contract：先打开完整 ticket，再经
  `publish_snapshot_session` 写入；architecture gate 拒绝重新引入裸 public API，并要求 fresh read
  同时包含 coherent slot 与 active-session fence。
- [x] 并发门禁首次捕获真实 torn read：第二次 `freshness_version.load(Acquire)` 不能约束其前
  面的 protected loads，曾观察到 sequence=5288/session=5289。按 Linux
  `read_seqcount_retry` 与成熟 Rust seqlock 顺序，在 retry-counter load 前增加 Acquire fence，
  随后同一高并发回归连续 50 次通过。
- [x] Loom 初版断言错误地要求并发 poison 后物理 slot 绝不能短暂保持 Fresh；按冻结语义改为
  验证 `(slot Fresh && session Active)` 复合状态永不成立。该测试以及实际 publisher/poison
  线程竞态均通过。
- [x] 验证：`cargo check --workspace --all-targets`；受影响三 crate Clippy `-D warnings`；
  fresh BookStore 6、coalescer 6、entry condition 12、core ingest 63；最终完整 core lib
  391/391；`cargo fmt --all -- --check` 与 `cargo xtask architecture check` 全部通过。

### PERF-18

- [x] `ClobWsManager` 在 token transport ownership 从 1 降为 0 时发送带当前 epoch 的
  retirement 通知；pipeline 以 bounded queue 接收，按 partition 预留全部 mailbox permit，
  等待 barrier 后才完成。较新 session/epoch 已接管时旧 retirement 不得删除新状态。
- [x] partition retirement 删除 actor-owned `OrderBook`、stream sequence 和 microstructure
  scratch，发布 `Retired` tombstone 并释放大 `Arc` sides；`TokenKey` 与 slot 进程内保持稳定且
  永不复用。新 session 先进入 `Unseen`，只有完整 snapshot 可以重新变为 Fresh。
- [x] 修复 retirement 与 stale shard open 的竞态：创建 session epoch 前必须验证完整物理
  session scope 仍属于 transport union；任何 token 已失去 ownership 时整 session fail closed，
  防止旧 assignment 用更高 epoch 绕过 retirement cutoff。
- [x] SessionHub 拆为 1,024 control、2,048 reliable、8,192 topic latest-value best-effort
  三条 lane，并使用 biased select 优先 shutdown/fail-closed/control。control enqueue 或 ACK
  超过 100ms 触发 hub-wide cancellation；session loop 直接监听该 token，不依赖阻塞 hub 再发 close。
- [x] fanout 使用 `SharedFrame { ByteString, OwnedSemaphorePermit }`：唯一 frame allocation
  只收费一次，clone 共享同一 permit，最后一个 outbox 释放时归还。64MiB retained-frame
  budget 与 1MiB 单 frame 上限同时约束 count 和 bytes；reliable 溢出断开受影响 topic 的
  sessions，best-effort 只保留每 topic 最新值。
- [x] 用户指出固定唯一合法值不应伪装成部署配置；已删除新加的 config model、TOML 和
  `validate_web` 精确值检查。五个预算保留为 private constants，并由 architecture check
  验证精确声明、三 lane、biased select、byte permit 与 fail-closed contract。
- [x] 新增 control latency/timeout、三 lane depth/oldest age、retained frame bytes、coalesced、
  dropped、reliable disconnect，以及 partition mutable-book count 指标。
- [x] 回归覆盖 session ID 溢出、超大 frame、best-effort coalescing、reliable overflow、control
  ACK timeout、10K sessions/1K subscribers/10K fanout 下 subject revoke 在 100ms 内完成；
  10K catalog churn 的 mutable books 始终不超过 2K active window 且最终归零。
- [x] 验证：`cargo check --workspace --all-targets`；`cargo test -p quant-pivot-api ws::`
  30/30；`cargo test -p quant-pivot-core ingest::` 65/65；`cargo test -p quant-pivot-web
  ws::tests` 12/12；受影响四 crate Clippy `-D warnings` 与 `cargo xtask architecture check`
  全部通过。

### PERF-19

- [x] 新增唯一 `quant-pivot-compute::ComputeExecutor`，由 composition root 构建后注入；
  serving/offline Rayon 各固定 2 threads，统一持有 CPU permits、exclusive offline job lease
  与 10GiB offline memory permits，service 不再自建 pool。
- [x] CPCV、training dataset、model training、matrix/backtest/trade-policy 以及 archive/weather
  decode 均经统一 executor；caller future 被取消或丢弃时，worker 仍持有 lease 直到真实计算
  完成，禁止把 `spawn_blocking::abort` 当作终止语义。
- [x] weighted/sell/classical trainer 在 matrix、fold、trial、chunk/coordinate-search 边界检查
  `CancellationProbe`；取消等待者不会启动计算，panic 映射 typed `InfraError`。
- [x] architecture 从整文件 allowlist 改为语义扫描：生产直接 `spawn_blocking`、
  `ThreadPoolBuilder`、global Rayon 与非批准函数内 `par_iter` 均拒绝；新增同文件未批准并行
  调用的反例测试。
- [x] 验证：compute 5/5、research trainer 13/13、classical 5/5、core CPCV 9/9、API
  Binance 21/21、Tornado 2/2；`cargo check --workspace --all-targets`、workspace Clippy
  `-D warnings`、architecture tests 21/21 与实际 architecture check 通过。
- [x] 最终全仓测试发现 historical PIT 闭包进入 offline Rayon 后调用
  `Handle::current()` 会失去 Tokio reactor；现改为在 async 边界捕获显式 runtime handle，
  worker 仅用该 handle 驱动已预取的内存 async facade。`core_business` 全场景回归通过，
  未退回无预算 `spawn_blocking`。

### PERF-21

- [x] report gate 改为 2K full-compute funnel：完整 feature/model lineage、planner rejection、
  optimizer/TopN recommendation；禁止 early-terminal。CPCV gate 明确重命名为
  `cpcv_orchestration_gate`。
- [x] matrix fixture 覆盖 observed/substituted/missing/not-applicable 的真实 Decimal cell；新增
  feature-gated `model_train_replay_gate`，执行 1M-row Ridge train、冻结 transform replay 与
  全量 prediction checksum。
- [x] `quant-pivot-system-tests::performance` 使用 deterministic Gamma HTTP 与按真实
  subscription scope 路由的 CLOB WebSocket，贯通 SDK parser→normalize→bounded ingress→8
  partition→ClickHouse durable cursor→Fresh BookStore，不调用测试 publish shortcut。
- [x] 新增 enqueue 与 durable-publication observer；HDR 使用 coordinated-omission correction，
  原始 bucket JSON 与 SHA-256 随 `PerformanceEvidenceV1` 保存。证据包含 fixture/seed hash、
  环境、ClickHouse/RTT、事件与 correctness counters、CPU、encoded bytes、jemalloc net
  allocated、RSS。warm-up 必须经过 durable barrier 后才能打开测量窗口。
- [x] `cargo xtask performance run --profile full` 统一执行短 kernel 十次、真实 model gate
  一次与完整在线负载三次；跨 run variation 超过 3% 非零退出。`smoke` 不签收 SLO。
- [x] 新增独立 `performance.yml`：固定 self-hosted runner；main relevant-path 为 full，nightly
  为 2h soak，manual 可选 profile；artifact v4 保存 evidence 和 SHA-256 manifest。
- [x] kernel binary 在 Linux 直接采集 `VmHWM`；matrix 8GiB、CPCV/model 10GiB ceiling
  由 binary 自身硬拒绝，缺值同样失败。kernel evidence 结构化保存 `peak_rss_bytes`；CI
  manifest 经临时文件生成并排除既有 manifest，消除自引用哈希。
- [x] 首次本机 debug smoke 真实启动 PG/Redis/ClickHouse 26.5、解析 1K Gamma markets/2K
  tokens、完成初始 snapshots 与 8K 测量事件，所有 correctness counter 为零并生成完整
  artifact。该次发现 warm-up backlog 在开表后继续 publish（8,630/8,000）且污染 HDR；已加
  `all_durable_publications` barrier，原证据保留为失败诊断，不作为性能基线。
- [x] mixed-state fixture 首轮统一 smoke 拒绝 required `f0` 的 `Substituted` cell；修复为
  required 列恒为 `Observed`、optional 列覆盖四种状态，并新增直接执行
  `FittedInputTransform::fit` 的回归测试。失败证据保留为诊断，不删除 gate 或放宽契约。
- [x] 修复后 `cargo xtask performance run --profile smoke` 统一 release 验证：matrix
  1M×128 16.470s；CPCV orchestration 0.555s；2K full report median 13.819ms / p99
  14.408ms；100K Ridge train/replay 80.464s。真实 2K-token system load 的 8,000 source
  events 与 8,000 durable publications 精确一致，dropped/gap/duplicate/out-of-order/
  invalid-fresh-read/writer failures 全为零；两个 HDR artifact 的 SHA-256 已逐一复算一致。
  macOS `VmHWM`/CPU/RSS 缺值符合 smoke 语义，durable p99 不作为 Linux SLO 签收。
- [x] 最终全仓验证发现并修复 Config activity 的跨时钟因果排序：全局 feed 改用数据库
  `created_at`，领域 `decided_at/activated_at` 仍完整保留；actor clock 领先数据库 1 秒的
  确定性 repository contract 通过。
- [x] 最终本机质量链：`cargo fmt --all -- --check`、workspace Clippy `-D warnings`、
  `cargo xtask architecture check`、`cargo check --release --workspace` 全部通过。完整
  workspace test 两次分别暴露并推动修复上述 reactor/因果排序问题；修复后的测试集合已
  全覆盖通过：`core_business`、`repository_contracts`、reset/recovery 229.41s、production
  web boundary 295.27s，以及其余 workspace unit/integration/doc tests。一次完整命令中的
  reset plan 因容器尚残留 1 条 PostgreSQL session 正确 fail-closed；同一目标隔离复跑通过，
  未放宽生产连接检查。
- [x] 最终再次执行不跳项 `cargo test --workspace` 单次 exit 0：reset/recovery
  236.27s、repository contracts 22.95s、production web boundary 159.76s；Web 43/43、
  xtask 24/24、全部 workspace unit/integration/doc tests 均通过。
- [ ] fixed Linux runner 生成 full/soak CI artifact。未取得 artifact 前 PERF-21 保持
  `in_progress`，不得伪称 SLO 已通过。

## 更新规则

1. 开始任务前记录时间、工作树状态、目标文件和恢复命令。
2. 每完成一个可独立验证的步骤立即更新 `最后完成步骤` 与证据。
3. Benchmark 决策必须保留命令、原始数据、阈值和被淘汰方案。
4. 失败必须记录错误摘要、环境状态和下一条恢复命令。
5. 无法关联 profiler、正确性不变量或硬 SLO 的改动不得混入。
6. 未经用户明确要求不 commit、不 push。
