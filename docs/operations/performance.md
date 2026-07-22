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
- 连续运行十次，记录中位数、p95、p99、CPU/event、alloc/event 和 RSS。
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
row count 不发生截断。固定 jemalloc 后的 macOS 2026-07-22 smoke：
1,000,000×128 transform 13.093s，process 14.14s，maximum RSS
3,570,515,968 bytes (3.325GiB)。

1M-row/10-partition CPCV 门禁隔离模型本身的已独立矩阵门禁，专门覆盖
45 个 purge/train/evaluate combination、9 条完整路径、精确 row coverage 和
2-thread offline Rayon 预算：

```bash
cargo build --release -p quant-pivot-bench --no-default-features --bin cpcv_gate
/usr/bin/time -l target/release/cpcv_gate 1000000   # macOS
/usr/bin/time -v target/release/cpcv_gate 1000000   # Linux
```

门禁为 300s，同时受离线进程 RSS 10GiB 上限约束。macOS 2026-07-22
固定 jemalloc smoke：核心 CPCV 0.536s，process 0.97s，maximum RSS
718,405,632 bytes (0.669GiB)。

2K market report pure-compute 门禁反复执行 5 次 warm-up + 100 次测量，
验证 funnel row count 和 `market_id` 确定性排序：

```bash
cargo build --release -p quant-pivot-bench --bin report_compute_gate
/usr/bin/time -l target/release/report_compute_gate   # macOS
/usr/bin/time -v target/release/report_compute_gate   # Linux
```

单 sample p99 门禁为 2s。固定 jemalloc 后的 macOS 2026-07-22 smoke：
median 12.585ms，p99 13.157ms，maximum RSS 11,878,400 bytes。warm-cache 端到端 5s 仍由
Linux fixed runner 的真实 PostgreSQL/ClickHouse 套件验收，不以纯计算数值替代。

## 在线热路径门禁

固定 jemalloc 后的 macOS 2026-07-22 Criterion smoke（1s warm-up、3s
measurement、20 samples）：

- `book_store_read_borrowed`：5.1444–5.1610ns；读取通过 ArcSwap guard，不增加
  `Arc` refcount，远低于 10µs smoke 目标。
- `book_store_publish_snapshot`：198.19–199.00ns。
- `session_hub_10k_sessions_1k_topic_fanout`：45.596–49.836µs/事件；一个
  `ByteString` frame 共享投递至 1,000 个 outbox，远低于 2ms smoke 目标。

这些数值只用于 macOS 回归预警；10K events/s sustained、50K events/s burst、
enqueue p99、durable-ack p99、在线 RSS 以及 Linux p99 仍必须在固定 Linux runner
用完整网络与 ClickHouse 负载验收。

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
