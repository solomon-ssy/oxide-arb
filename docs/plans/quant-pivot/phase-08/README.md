# Phase 08+ — 部署架构与水平扩展

> 状态：设计文档（**Phase 5–7 生产闭环完成后**再评估实现）
>
> 父文档：[`../08-third-party-crates-and-ml-stack.md`](../08-third-party-crates-and-ml-stack.md) §8、
> [`../06-config-deploy-and-ops.md`](../06-config-deploy-and-ops.md)
>
> 本目录承接 Phase 5 **明确延后**的部署/运维增强项，保证单实例方案在 Phase 5 正确的前提下，
> 多副本演进有完整契约。

## 1. 延后项索引（来自 Phase 5）

| 能力 | 登记于 | 本子phase章节 |
|---|---|---|
| 多副本 leader-elected execution / exit / recon worker | [`phase-05/README.md`](../phase-05/README.md) §6.2、`05.4`/`05.6` §11 | §2 |
| 多副本 leader-elected report worker | [`phase-04/04.3-report-scheduler.md`](../phase-04/04.3-report-scheduler.md) §10 | §2 |
| Trailing stop 高频评估（sub-second mark 跟踪） | [`05.6-exit-lifecycle-and-monitor.md`](../phase-05/05.6-exit-lifecycle-and-monitor.md) §11 | §3 |

## 2. 多副本 Leader-Elected Workers

### 2.1 问题

Phase 5 采用**单实例 + Postgres advisory lock**（`ExecutionDispatcher`、`ExitMonitorWorker`、
`ReconciliationWorker` 各持 lock 名）。多副本同写会导致重复下单 / 重复 exit / 对账竞态。

### 2.2 目标架构

```text
                    ┌─────────────────┐
                    │  Postgres / Redis │
                    │  (job lease)      │
                    └────────┬────────┘
           ┌─────────────────┼─────────────────┐
           ▼                 ▼                 ▼
    ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
    │  replica A  │   │  replica B  │   │  replica C  │
    │  (leader)   │   │  (standby)  │   │  (standby)  │
    └─────────────┘   └─────────────┘   └─────────────┘
```

**候选技术栈（08 §8 对比结论）：**

| 组件 | Phase 5 | Phase 8+ |
|---|---|---|
| Report 调度 | `tokio-cron-scheduler` 进程内 | `apalis` + Postgres storage **或** 保留 cron 仅 leader 跑 |
| Exit/Recon/Dispatch | advisory lock | `apalis` worker + lease TTL **或** 增强 advisory lock + heartbeat |
| 队列 | 无 | `apalis` 持久化队列（Redis/Postgres backend） |

### 2.3 必建契约

```rust
pub trait LeaderElectedTask: Send + Sync {
    fn lock_key(&self) -> &'static str;
    fn lease_ttl(&self) -> Duration;
    async fn try_acquire(&self) -> QuantResult<Option<LeaderLease>>;
    async fn run_as_leader(&self, lease: LeaderLease, shutdown: CancellationToken) -> QuantResult<()>;
}
```

**TaskId 映射：**

- `TaskId::ExitMonitor` → lock `quant:leader:exit_monitor`
- `TaskId::Reconciliation` → `quant:leader:reconciliation`
- `TaskId::ExecutionDispatcher` → `quant:leader:execution_dispatch`
- Report scheduler → `quant:leader:report_schedule`（04.3 登记）

### 2.4 迁移原则

- 单实例部署**无需改代码路径**；多副本通过 deploy-config `leader_election.enabled=true` 启用。
- Failover：lease 过期 → standby 接管；**at-least-once** 扫描 acceptable，靠 PG 幂等守卫防双写。

### 2.5 验收

- 双副本仅一 leader 提交 exit order。
- Leader 崩溃后 ≤ `2 × lease_ttl` 内 standby 接管。
- 04.3 report cron 不 double-fire。

## 3. Trailing Stop 高频评估

### 3.1 问题

05.6 按 `execution.exit_monitor.monitor_secs`（默认 10s）周期评估 trailing stop
（`effective_stop = max(stop_loss, peak×(1−trail_bps))`）。对 fast market，10s 可能错过 peak 更新。

### 3.2 设计（Phase 8+）

**选项 A — Book-driven（推荐）：**

- `BookStore` 订阅 token L2 更新 → 内存 `peak_mark_price` 实时更新（intent 列已有）。
- `ExitMonitorWorker` 仍 10s 决策，但读**已实时更新**的 peak；SL 触发延迟降至 book 延迟。

**选项 B — Sub-second worker：**

- 独立 `TrailingStopEvaluator` 100ms tick（仅 open lots with `trailing_stop`）。
- 与 exit monitor 共享 `decide_exit` 但拆分 cadence config：

```toml
[execution.exit_monitor]
monitor_secs = 10
trailing_eval_ms = 100   # Phase 8+；0 = 禁用，回退 05.6 行为
```

### 3.3 不变量

- 仍走 05.6 优先级阶梯；trailing 仅影响第 4 档 SL 判定。
- 不引入新 exit 路径；不 bypass kill-switch。

### 3.4 验收

- 模拟 book tick 更新 peak → 10s 内 SL 用新 effective_stop。
- `trailing_eval_ms=0` 行为与 05.6 基线一致。

## 4. 质量门禁

同 Phase 5/6；另加 multi-replica integration test job（CI optional job）。

## 5. 延后 / 缺口

- **Kubernetes lease**（coordination.k8s.io）→ 若不用 Postgres advisory lock。
- **Regional failover** → 超出 Phase 8 范围。
