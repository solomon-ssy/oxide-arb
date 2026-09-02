# Quant Pivot 性能验证与运行手册

## 平台

- 阻断平台：固定 Linux x86_64、8 vCPU、16 GiB runner。
- 开发平台：macOS，运行功能测试和 benchmark smoke，不与 Linux 数值比较。
- ClickHouse integration 版本：仓库固定的 26.5。

## 标准质量门禁

```bash
cargo fmt --all --
cargo clippy --workspace --all-targets -- -D warnings
cargo xtask architecture check
cargo check --release --workspace
cargo test --workspace
```

## 性能门禁原则

- 使用固定语料、固定随机种子和相同 runner。
- 短 kernel 连续运行十次；完整在线负载连续运行三次。记录 HDR p50/p95/p99/max、
  CPU/event、jemalloc net allocated/event、encoded bytes/event 和 RSS。
- runner 自身波动超过 3% 时本轮无效，不得据此接受优化。
- channel/crate 候选只有在目标指标改善至少 10%，且其他核心指标退化不超过
  3% 时才能替换现有实现。
- 全部运行时 crate 通过唯一 `quant-pivot-allocator` 固定使用
  `tikv-jemallocator`；禁止 system fallback、allocator feature 开关和第二 allocator
  声明。`quant-pivot-macros` 是加载进 rustc 的编译器插件，不属于目标进程 allocator 图。
- `#[inline]` 需要跨 crate 极小函数或至少 3% 的可重复 benchmark 证据；禁止
  `#[inline(always)]`。

## 计算负载门禁

Research orchestration 的 offline job/CPU lease 只允许覆盖 page-bounded owned kernel，
repository、ClickHouse、S3 与其他网络等待不得持有 job/CPU lease。普通 kernel 的 memory
lease 由 Rayon worker 持有到实际退出；当 resident payload/encoded bytes 必须跨 async I/O
或 PostgreSQL await 时，必须先取得 executor-owned `OfflineMemoryLease`，随后所有 kernel 复用
该 reservation，直到最后一次 readback/commit 才释放。reservation 使用 RAII，且 worker 持有
内部 `Arc`，caller future 被 abort 也不得在 Rayon 内核退出前提前归还 memory permits；禁止在
owned lease 内再次 acquire memory 形成双重计费或锁序反转。

Model training 在 Parquet GET 前取得唯一 8 GiB owned lease，并覆盖 decode、fit、classical
estimator PUT/readback、outer model artifact PUT/readback 与 model-version/run PostgreSQL 原子
commit；同一对象的 URI/key readback 必须顺序验证，禁止同时保留无界重复副本。Model-score
calibration 在 replay prepare 前取得唯一 10 GiB owned lease，并覆盖 Dataset/Source Slice
decode、allocation-independent replay、isotonic/Platt fit、artifact/run transaction 与最终
readback。

FeatureParity 的固定顺序是 evidence I/O → canonical plan kernel → PIT/history I/O →
materialized PIT/selection/cross-section/model/comparison kernel。禁止在 offline worker 内用
`Handle::block_on` 驱动可达 repository 或 artifact store 的 async graph；允许它只驱动已预取、
无外部 I/O owner 的内存 async facade。所有 serving/offline CPU kernel 必须在提交前形成
owned `'static` input（大对象使用 move/`Arc`，不得用深拷贝换生命周期）；调用方 future 始终
保持可 poll，Rayon worker 自己持有 job/CPU/memory lease 直至实际退出。生产代码禁止
`run_offline_scoped`、`run_serving_scoped` 或在应用 Tokio worker 上调用 `block_in_place`。

1M×128 训练矩阵使用 single-shot binary 测量，避免 Criterion 重复 sample
扭曲峰值 RSS：

```bash
cargo build --release -p quant-pivot-bench --no-default-features --bin training_matrix_gate
/usr/bin/time -l target/release/training_matrix_gate 1000000   # macOS
/usr/bin/time -v target/release/training_matrix_gate 1000000   # Linux
```

门禁同时要求 transform 不超过 60s、maximum RSS 不超过 8GiB，并验证
row count 不发生截断。binary 在 Linux 直接读取 `/proc/self/status` 的 `VmHWM`；缺值或
超过 8GiB 都会非零退出，不依赖操作者另外运行 `/usr/bin/time`。mixed-state fixture 的
macOS 2026-07-22 统一 release smoke 为 16.470s；macOS 不提供 `VmHWM`，因此该值只证明
功能与时间回归，不签收 RSS。

1M-row/10-partition CPCV orchestration 门禁隔离模型本身，专门覆盖
45 个 purge/train/evaluate combination、9 条完整路径、精确 row coverage 和
2-thread offline Rayon 预算：

```bash
cargo build --release -p quant-pivot-bench --no-default-features --bin cpcv_orchestration_gate
/usr/bin/time -l target/release/cpcv_orchestration_gate 1000000   # macOS
/usr/bin/time -v target/release/cpcv_orchestration_gate 1000000   # Linux
```

该 gate 只证明 purge/train/evaluate orchestration kernel，不签收真实模型训练。门禁为
300s，同时受离线进程 RSS 10GiB 上限约束；Linux binary 同样直接读取 `VmHWM` 并硬拒绝
超限。macOS 2026-07-22 统一 release smoke 为 0.555s，RSS 不参与本机签收。

真实 classical model gate 使用 mixed observed/substituted/missing/not-applicable Decimal
矩阵，完成 1M-row Ridge train、冻结 transform replay 和全量 prediction 校验：

```bash
cargo run --release -p quant-pivot-bench \
  --features model-train-gate --bin model_train_replay_gate -- 1000000
```

该 gate 的 1M-row 时间上限为 300s、RSS 上限为 10GiB；Linux 缺少 `VmHWM` 也视为失败。
macOS 2026-07-22 统一 smoke 使用 100K rows：train 79.965s、replay 0.443s、total
80.464s，prediction checksum 50,000。它不外推或签收 1M-row Linux SLO。

完整 global-portfolio finance gate 从 promoted joint-scenario contract 开始，依次执行 concrete
scenario materialization、可执行 L2 tier ladder、逐场景 Decimal economics、直接 HiGHS MILP、exact
post-check 与全部 leave-one-out marginal re-optimization。发布路径只上传一个矩阵：objective lock
通过固定 relaxation column 生效，进入 marginal 阶段后解除；历史 replay 不执行 marginal explanations。

```bash
cargo build --release -p quant-pivot-bench --no-default-features --bin portfolio_compute_gate
/usr/bin/time -l target/release/portfolio_compute_gate 10000 400 20 180   # macOS
/usr/bin/time -v target/release/portfolio_compute_gate 10000 400 20 180   # Linux
```

阻断 workload tuple 固定为 10,000 executable tiers、400 promoted joint scenarios、Top20、180 秒
full-path bootstrap ceiling；Linux 进程 RSS ceiling 为 8GiB。四个维度是一个不可拆分的资格合同，
不能把独立字段上限的笛卡尔积伪称为已验证。400 scenarios 来自当前 promoted template（320 PIT
bootstrap + 40 calibration uncertainty + 40 structural stress），不是 CVaR/SAA 的普适黄金样本数；
场景数只能由 tail assumptions、误差目标、实际覆盖和重新取得的容量证据改变。

macOS 2026-08-10 两次已构建 binary 的同输入结果为 77.910s / 78.350s，plan hash 均为
`blake3:efa064ebbae3faae5ba7033787a23034d45b14dbb7c64ae61af206bea421d298`，外部 RSS
约 2.17GiB。该证据只证明功能、确定性和本机容量；最终 production p95/p99 仍须由固定 Linux
runner 的十次 release artifact 签收。默认 180 秒是依据当前 300 秒 report cadence 和这组本机
workload 建立的有限 bootstrap liveness ceiling，不是金融/统计常数或已签收 latency SLO。

2K market report-funnel gate 反复执行 5 次 warm-up + 100 次测量。它只物化已冻结的
feature/model/tier/recommendation funnel rows，不执行 feature inference、scenario、optimizer 或 TopN
选择；early-terminal/缺 lineage 输入会直接使 gate 失败：

```bash
cargo build --release -p quant-pivot-bench --bin report_funnel_gate
/usr/bin/time -l target/release/report_funnel_gate   # macOS
/usr/bin/time -v target/release/report_funnel_gate   # Linux
```

单 sample p99 门禁为 2s。macOS 2026-07-22 当前 funnel 统一 release smoke：median
13.819ms、p99 14.408ms；该结果只用于回归预警。旧 early-terminal 数值已 superseded，
不得用于签收，也不得再将该 gate 描述为 full report compute。warm-cache 端到端 5s 仍由 Linux
fixed runner 的真实 PostgreSQL/ClickHouse 套件验收，不以 funnel 数值替代。

## 在线热路径门禁

Canonical L2 由唯一 coordinator 在应用内按最多 20ms / 8,192 rows 聚合，并通过
独占 critical lane 执行同步 ClickHouse insert（显式 `async_insert=0`、
`insert_deduplicate=1`）。不得再叠加 server async-insert queue：durable publication 的
2 秒 quarantine 是 submit→ack 的 fail-closed 边界，未知结果只允许在 12 秒 final
reconciliation 内收敛。`quant_book_l2_ledger_persistence_stage_seconds` 必须同时观察
`admission_to_sink` 与 `sink_ack`，用来区分 Tokio/coordinator 调度停顿和存储 ACK 延迟；
不得靠放宽 deadline 掩盖任一阶段的超时。

所有 bulk facts 同样由应用侧有界 writer 聚合后显式同步插入；禁止再启用 ClickHouse
server async-insert queue。`book_microstructure_1s` 的应用聚合窗口为 1 秒，其他 analytics
沿用部署配置的 5 秒 / 5,000-row 默认批次；双层 5ms/100ms flush 会制造 tiny parts，属于
架构回归。

Self-managed ClickHouse 必须加载
`docker/clickhouse/config.d/quant-pivot-governance.xml`：后台 merge pool 固定有界，保留低量
`query_log`，通过 9363 `/metrics` 暴露当前 metrics/events/asynchronous metrics/errors/
histograms，并禁用会自增 MergeTree 压力的 metric/asynchronous/text/trace/processors/
query-thread/query-view/part 等持久 system logs。分析读取由
`db.clickhouse.max_concurrent_reads` 做进程级 admission，并由
`max_threads_per_query` 限制单查询线程；canonical/bulk/read 的 foreground priority 分别为
1/4/8，但该 priority **不能**调度 background merges，不能把它描述为 merge 隔离。
`ch_read_admission_wait_seconds{operation}` 的 p99 与告警预算必须由 fixed mixed-workload
runner 的已签收 baseline 给出；本次 production rehearsal 要求 read wait 不持续饱和且不触发
canonical quarantine。`ch_read_permits_used` 不应长期贴满上限，任何
`ch_read_admission_rejections_total` 增长都阻断 performance 签收。Provider-managed
ClickHouse 保留相同应用侧 admission/thread caps；provider capacity、system-log retention
与 mixed-workload benchmark 证据属于独立 promotion gate，进程不会伪装拥有 server 配置权。

所有 ClickHouse I/O 都由 `db.clickhouse.io` 的 typed deadline 包络。runtime read 的默认
30 秒预算覆盖 admission、connect、response 和 decode，同时下推向上取整的
`max_execution_time`；maintenance/bootstrap 的每条请求默认 120 秒，只使用 client-side
总包络以兼容 DDL/SYSTEM。critical insert 默认 send/end/attempt 为 300/1,200/1,800ms，bulk
为 750/1,750/3,000ms；send + end 必须小于等于 attempt，为 metadata/调度保留显式余量，
attempt 必须覆盖 lane permit、metadata、全部 chunks 与最终 ACK，
三次尝试复用固定 100/200ms backoff。`flush_interval_secs` 限于 1..=5，`batch_size`
限于 1..=5,000，variable bulk queue 还受 `max_inflight_write_bytes`（默认 64MiB）约束。
默认 bulk retry window 为 9.3 秒；一秒 derived-fact flush + retry + 500ms margin 为
10.8 秒，保持在共享 12 秒 receipt deadline 内。通用 bulk receipt 的最坏预算按
`flush + retry window + 500ms scheduling margin` 同源计算；配置上界16秒、默认 crypto 上界14.8秒，20 秒 shutdown
stage 覆盖 stop-production→flush→receipt drain。上述默认值是首次 mixed-load calibration
前的 bootstrap capacity budget，不是延迟 SLO；调整后必须重新运行 fixed mixed-workload
runner，且不能破坏 canonical 2 秒 publication/final reconciliation 边界。超时返回 typed
transient `ClickHouseTimeout`，并必须通过 RAII 归还 read/write permits。

Crypto source facts 通过全局 acknowledged writer 按 5 秒 / 5,000 rows 上限聚合；禁止逐事件
同步 INSERT。`quant_pivot_system_async_writer_inflight_items{writer="quant_crypto_price_report"}`
和 `quant_pivot_system_async_writer_inflight_bytes{writer="quant_crypto_price_report"}` 分别覆盖
worker 已取走但 receipt 尚未完成 cursor commit 的总 row 数与 resident bytes，不能用 channel
`queue_depth` 代替。source shutdown 先停止生产，再触发同一 FIFO 的 flush barrier 并按 source
顺序提交 ACK/cursor；WsIngress 与 Analytics stage 均使用 20 秒同源 deadline，覆盖受治理的
bulk retry ceiling，超时 fail closed 且不越过 cursor。

SessionHub 的资源上限属于二进制架构契约，不是部署可调参数：control lane 1,024、
reliable lane 2,048、best-effort topic 8,192、共享 frame budget 64 MiB、单 frame
上限 1 MiB。修改这些值必须同时修改实现、架构门禁并重新取得负载证据；不得通过
部署配置绕过验证。control enqueue/ACK 的 100ms deadline 超限会触发 hub-wide
fail-closed cancellation。

固定 jemalloc 后的 macOS 2026-07-22 Criterion smoke（1s warm-up、3s
measurement、20 samples）：

- `book_store_read_borrowed`：5.1444–5.1610ns；读取通过 ArcSwap guard，不增加
  `Arc` refcount，远低于 10µs smoke 目标。
- `book_store_publish_snapshot`：198.19–199.00ns。
- `session_hub_10k_sessions_1k_topic_fanout`：45.596–49.836µs/事件；一个
  `ByteString` frame 共享投递至 1,000 个 outbox，远低于 2ms smoke 目标。

这些数值只用于 macOS 回归预警。完整在线门禁统一运行：

```bash
cargo xtask performance run --profile full --output target/performance-evidence
```

`full` 固定为 2K active tokens、5 分钟 warm-up、30 分钟 10K events/s open-loop、
10 秒 50K burst、5 分钟 recovery，并连续运行三次；`soak` 固定为两小时
catalog/session churn。两者只接受 Linux 上
`QUANT_PIVOT_PERF_RUNNER=quant-pivot-perf-8c16g`，该值是 runner 身份证明，不是可调性能
配置。`smoke` 仍使用 2K tokens 和真实 parser→normalize→partition→ClickHouse durable
ack→Fresh BookStore 链，只缩短时间与速率，且只签收功能/证据完整性，不签收硬 SLO。

系统负载由本地 deterministic Gamma HTTP 和按 subscription 路由的 CLOB WebSocket
upstream 驱动真实 production adapter。每次生成 schema-versioned
`PerformanceEvidenceV1`、原始 HDR bucket JSON 及 SHA-256，包含 git/rustc/kernel、CPU
model/governor、allocator、ClickHouse version/settings、RTT、fixture/seed hash、事件与
正确性计数、吞吐、CPU/event、encoded bytes/event、jemalloc net allocated/event 和 RSS。
warm-up 在打开 histogram 前必须跨越 durable publication barrier，禁止 backlog 污染样本。
kernel evidence 另保存每次 stdout/stderr、输出 SHA-256 与 `peak_rss_bytes`；Linux matrix、
CPCV、portfolio、model gate 在 binary 内执行 RSS ceiling，不能通过省略外部采集绕过。

`.github/workflows/performance.yml` 只使用 `[self-hosted,
quant-pivot-perf-8c16g]`；main relevant-path 运行 `full`，nightly 运行 `soak`，manual 可显式
选择 profile。GitHub Actions v4 artifact 保存 evidence 与最终 SHA-256 manifest。
manifest 先写入工作目录外的临时文件，并排除既有 manifest 后原子移动，禁止自引用哈希。

## 破坏式 L2 reset

项目尚未正式投产，因此不提供 L2 数据迁移、双写或兼容 reader。切换通过唯一的
`preproduction-reset plan/apply/verify` 完整删除并重建项目 PostgreSQL/ClickHouse
数据库，同时清空 Redis `qp:*` namespace，再执行仓库当前唯一 bootstrap。

执行前必须停止 quant-pivot 全部进程；这同时保证 WS ingest、session、writer、报告、
训练与回测均已停止和 drain。`plan` 与 `apply` 都会验证 PostgreSQL 无项目连接、
ClickHouse 无项目查询或 server-wide mutation，并拒绝 production baseline、非精确
`quant_pivot` target 或 inventory 漂移。

`apply` 必须同时提供 `plan` 生成的 15 分钟一次性 nonce 和完整确认字符串：

```text
DELETE_ALL_PREPRODUCTION_DATA_AND_REBOOTSTRAP
```

该操作会删除所有未投产项目数据，不只是 L2。不得对当前共享环境自动执行；只允许
操作者显式运行，自动化验收使用隔离的 PostgreSQL/ClickHouse/Redis 容器。
