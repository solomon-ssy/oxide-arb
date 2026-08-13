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
