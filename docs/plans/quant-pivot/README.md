# Quant Pivot 重构设计索引

<!-- quant-pivot-deployment-contract:v1 -->
> **Deployment contract**
> - `fresh_boot_assumption`: 项目尚未正式生产上线，将从全新 `boot` / schema version `1` 部署；仓库和数据库不保存 lifecycle seal 状态。
> - `schema_data_version_impact`: 本文中的历史版本号与递增路径不再具有实施效力；当前实现不迁移测试数据、旧结构或旧版本。
> - `pre_deployment_behavior`: 允许 clean-break、migration squash 与全新基础设施 bootstrap，但任何数据销毁仍需操作者单独授权。
> - `post_deployment_behavior`: 本次重构只交付并验证唯一终态 schema；不设计旧版本共存、升级或降级路径。
> - `rollback_and_data_verification`: 仅在 disposable 空数据库执行 fresh-install 验证；任何真实数据库重置需要操作者另行授权。

> 状态：生产级破坏式重构设计
>
> 范围：仅限 Polymarket 的 quant-pivot，彻底替换当前 Endgame arbitrage 系统
>
> 兼容策略：零兼容。删除旧 Endgame、DryRun/Paper/Live、旧 runtime-config shape、旧命名和 re-export。
>
> **Phase 12 clean break**：`report_only / semi_auto / auto_execution` 及整个
> `QuantRuntimeMode` 轴正在由 [`phase-12/README.md`](phase-12/README.md) 破坏式取代。
> Phase 12 是执行授权、账户单写者、break-glass 恢复和快速经济反馈的唯一 current owner；
> 下列早期 Phase 文档中的 runtime-mode 文字在完成迁移前仅用于 deletion inventory。

## 0. 决策摘要

`quant-pivot` 必须从事件驱动的 Endgame arbitrage bot 重构为 Polymarket-only 的量化系统 `quant-pivot`。新系统的核心产物是周期性 TopN 量化建议报告；报告始终可生成，入场权限由 recommendation ceiling、逐 intent 授权、kill switch 与账户恢复状态共同决定。

旧产品闭环是：

```text
book update -> endgame detection -> risk gate -> FOK order -> post-trade -> settlement
```

新产品闭环是：

```text
Polymarket facts -> represented routes -> route-specific model/calibration/trade policy
 -> executable USD scenario tiers -> global robust portfolio plan
 -> TopN recommendation report -> analysis | operator authorization | policy automatic ceiling
```

一份报告允许跨 category/Route。Category 只作为 filter、risk bucket 与解释维度；任一 represented Route
缺少完整 artifact binding 或唯一 MILP 未得到 exact-verified optimal result 时，整份报告 fail closed。

## 1. 阅读顺序

按以下顺序阅读和实现：

1. [`00-quant-pivot-architecture.md`](00-quant-pivot-architecture.md)：目标产品、生产不变量、运行模式、完整业务闭环。
2. [`01-domain-model-and-schema.md`](01-domain-model-and-schema.md)：新领域词汇、Postgres 表、ClickHouse facts、旧 schema 替换映射。
3. [`02-crate-refactor-and-deletion-plan.md`](02-crate-refactor-and-deletion-plan.md)：crate、模块、配置、文档、脚本、测试的删除、合并、保留、重命名清单。
4. [`03-data-factor-model-pipeline.md`](03-data-factor-model-pipeline.md)：数据、特征、因子、模型、训练、point-in-time 验证平面（概念规格）。可执行的子phase实施契约（3.0–3.7）见 [`phase-03/README.md`](phase-03/README.md)。
5. [`04-topn-report-and-recommendation.md`](04-topn-report-and-recommendation.md)：TopN 报告 payload，明确买什么、什么时候买、买多少、什么时候卖、卖多少、入场触发、止盈、止损、出场节点。
6. [`05-execution-risk-and-governance.md`](05-execution-risk-and-governance.md)：`report_only`、`semi_auto`、`auto_execution` 的语义、审批、OrderIntent、组合风险、审计规则。
7. [`06-config-deploy-and-ops.md`](06-config-deploy-and-ops.md)：Deploy Config、六类 boot typed policy、CI、migration、Docker、observability、runbook 调整。
8. [`phase-05/05.8-portfolio-optimization-highs.md`](phase-05/05.8-portfolio-optimization-highs.md)：跨 Route 统一经济层级、联合场景、唯一 MILP 与 exact verification。
9. [`08-third-party-crates-and-ml-stack.md`](08-third-party-crates-and-ml-stack.md)：第三方 crate、模型训练、推理、优化、依赖引入顺序和 MSRV/native 风险。
10. [`09-account-capital-position-reconciliation.md`](09-account-capital-position-reconciliation.md)：账户/资本/持仓/对账平面——`AccountSnapshot`、planner 资金感知签名、资金状态机、对账证据链、Polymarket 余额/持仓数据源（设计先行，实现分相位到 Phase 4/5/6）。
11. [`08-cold-start-production-closeout.md`](08-cold-start-production-closeout.md)：冷启动、schema、catalog、bootstrap、parity、认证和 UI 的生产收尾契约与验收矩阵。
12. [`08-extreme-performance-design.md`](08-extreme-performance-design.md)：数据面、统一 L2 ledger、订单簿和 WebSocket fanout 的极致性能设计。
13. [`09-extreme-performance-ledger.md`](09-extreme-performance-ledger.md)：性能重构执行状态、决策和中断恢复台账。
14. [`phase-12/README.md`](phase-12/README.md)：删除 runtime-mode、重建执行授权、账户单写者/break-glass 和快速经济反馈。

**子phase实施目录（按 Phase 推进时读）：**

- [`phase-03/README.md`](phase-03/README.md) — 研究平面 3.0–3.8
- [`phase-04/README.md`](phase-04/README.md) — TopN 报告 4.0–4.4
- [`phase-05/README.md`](phase-05/README.md) — 执行/风险/治理 05.0–05.10
- [`phase-06/README.md`](phase-06/README.md) — ML 扩展（退出信号、跨账户对账、ONNX/classical publish、attribution feedback、反事实归因；**闭合 Phase 5 延后 seam**）
- [`phase-10/README.md`](phase-10/README.md) — 前端破坏式重构 10.0–10.6（概念规格：[`10-frontend-refactor.md`](10-frontend-refactor.md)）
- [`phase-12/README.md`](phase-12/README.md) — 执行授权、账户恢复与快速经济反馈 clean break

## 2. 硬边界

- 平台仍然只做 Polymarket。禁止引入通用 exchange、venue routing、多平台抽象。
- 策略不再是 Endgame-only，也不再用“无风险套利”定义产品。
- 核心产物是 `RecommendationReport`，不是 `ScoredOpportunity`。
- 不再存在统一运行模式；默认入场授权为逐 intent operator approval，报告生成与执行授权正交。
- 旧 `ExecutionMode::DryRun`、`ExecutionMode::Paper`、`ExecutionMode::Live` 必须删除，不做 alias。
- Runtime Config 只有六类 system-owned clean-install 资源；固定 schema discriminator 为 `1`，旧 JSON 路径不提供 parser、alias、converter 或 shim。
- 旧 Endgame 文档只能作为删除盘点资料，不能作为活跃实现依据。
- 禁止 compatibility re-export。公开 API 必须显式、收敛、语义精准。
- 不追求最小变更、最小侵入、最小工作量；优先追求正确领域模型、清晰边界和生产可维护性。

## 3. 被取代的旧文档

以下文档描述的是旧 Endgame arbitrage 系统，后续只能用于删除盘点，不能继续指导实现：

- `docs/plans/phase3-algorithm.md`
- `docs/plans/phase4.1-risk.md`
- `docs/plans/phase4.2-core.md`
- `docs/plans/phase5-replay-analytics.md`
- `docs/plans/phase5.0-control-plane-foundation.md`
- `docs/plans/phase5.1-fact-data-plane.md`
- `docs/plans/phase5.1a-l2-book-facts-retention.md`
- `docs/plans/phase5.2-pit-materialization-runner.md`
- `docs/plans/phase5.3-evidence-engine.md`
- `docs/plans/phase5.4-factor-builders-quality-gates.md`
- `docs/plans/phase5.5-registry-governance-api-scheduler.md`
- `docs/plans/phase5.6-live-consumption.md`
- `docs/plans/phase5.8-verification-operations.md`
- `docs/plans/phase7.3-business-markets-opportunities-trades.md`
- `docs/plans/phase7.4-risk.md`
- `docs/plans/phase7.5-analytics.md`
- `docs/operations/runbook.md`
- `docs/operations/live-production-guide.md`
- `docs/operations/bankroll-and-risk-metrics.md`

## 4. 可复用基础

以下基础能力可以保留，但必须改名、改语义、改边界：

- `quant-pivot-models` 中的 typed IDs 和 Decimal money newtypes。
- deploy-only、只追加且带 artifact checksum 的 `SeaORM` PostgreSQL migrations，以及规范化 schema manifest。
- DTO 三层契约：request/query、persistence DTO、view/response。
- Postgres、Redis、ClickHouse storage 基础设施。
- RBAC、Casbin、operation log、单 active HS256 JWT signing key、原子 refresh family 与受治理 runtime config 机制。
- Polymarket Gamma、CLOB market data、L2 book ingest。
- `BookStore` published snapshot 模式。
- ClickHouse fact writer 与 async writer 模式。
- control-factor plane 中的 materialization、governance、audit 模式。

## 5. 文档契约

每个实施 Phase 必须写清楚：

- 添加替代代码前必须删除哪些 crate、模块、类型和配置；
- 新领域类型名、新表名、新 ClickHouse fact 名；
- Deploy Config key、typed policy resource 或 immutable artifact owner，以及精确生效边界；
- 生产不变量和失败语义；
- Phase 完成前必须更新的测试、benchmark、architecture lint。
- 第三方 crate 选型、feature gate、MSRV/native 依赖风险和替代方案。

如果某个 Phase 不能说明它删除了什么旧概念，就不允许进入实现。
