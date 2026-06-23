# Quant Pivot 重构设计索引

> 状态：生产级破坏式重构设计
>
> 范围：仅限 Polymarket 的 quant-pivot，彻底替换当前 Endgame arbitrage 系统
>
> 兼容策略：零兼容。删除旧 Endgame、DryRun/Paper/Live、runtime-config v2、旧命名和 re-export。

## 0. 决策摘要

`quant-pivot` 必须从事件驱动的 Endgame arbitrage bot 重构为 Polymarket-only 的量化系统 `quant-pivot`。新系统的核心产物是周期性 TopN 量化建议报告；在受治理的运行模式下，系统可以停留在报告层，也可以等待人工确认后执行，或在严格风控下自动执行。

旧产品闭环是：

```text
book update -> endgame detection -> risk gate -> FOK order -> post-trade -> settlement
```

新产品闭环是：

```text
Polymarket facts -> features -> factors/models -> portfolio plan
 -> TopN recommendation report -> report_only / semi_auto / auto_execution
```

## 1. 阅读顺序

按以下顺序阅读和实现：

1. [`00-quant-pivot-architecture.md`](00-quant-pivot-architecture.md)：目标产品、生产不变量、运行模式、完整业务闭环。
2. [`01-domain-model-and-schema.md`](01-domain-model-and-schema.md)：新领域词汇、Postgres 表、ClickHouse facts、旧 schema 替换映射。
3. [`02-crate-refactor-and-deletion-plan.md`](02-crate-refactor-and-deletion-plan.md)：crate、模块、配置、文档、脚本、测试的删除、合并、保留、重命名清单。
4. [`03-data-factor-model-pipeline.md`](03-data-factor-model-pipeline.md)：数据、特征、因子、模型、训练、point-in-time 验证平面（概念规格）。可执行的子phase实施契约（3.0–3.7）见 [`phase-03/README.md`](phase-03/README.md)。
5. [`04-topn-report-and-recommendation.md`](04-topn-report-and-recommendation.md)：TopN 报告 payload，明确买什么、什么时候买、买多少、什么时候卖、卖多少、入场触发、止盈、止损、出场节点。
6. [`05-execution-risk-and-governance.md`](05-execution-risk-and-governance.md)：`report_only`、`semi_auto`、`auto_execution` 的语义、审批、OrderIntent、组合风险、审计规则。
7. [`06-config-deploy-and-ops.md`](06-config-deploy-and-ops.md)：deploy-config、runtime-config v3、CI、benchmark、Docker、observability、runbook 调整。
8. [`07-implementation-phases.md`](07-implementation-phases.md)：实施 Phase、退出标准、测试矩阵、迁移顺序。
9. [`08-third-party-crates-and-ml-stack.md`](08-third-party-crates-and-ml-stack.md)：第三方 crate、模型训练、推理、优化、依赖引入顺序和 MSRV/native 风险。
10. [`09-account-capital-position-reconciliation.md`](09-account-capital-position-reconciliation.md)：账户/资本/持仓/对账平面——`AccountSnapshot`、planner 资金感知签名、资金状态机、对账证据链、Polymarket 余额/持仓数据源（设计先行，实现分相位到 Phase 4/5/6）。

## 2. 硬边界

- 平台仍然只做 Polymarket。禁止引入通用 exchange、venue routing、多平台抽象。
- 策略不再是 Endgame-only，也不再用“无风险套利”定义产品。
- 核心产物是 `RecommendationReport`，不是 `ScoredOpportunity`。
- 运行模式重建为 `report_only`、`semi_auto`、`auto_execution`。
- `report_only` 是默认、最安全、最先上线的模式。
- 旧 `ExecutionMode::DryRun`、`ExecutionMode::Paper`、`ExecutionMode::Live` 必须删除，不做 alias。
- 旧 runtime-config schema v2 被 schema v3 替换，不做迁就旧 JSON 的 shim。
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
- declarative schema catalog 与 migration graph。
- DTO 三层契约：request/query、persistence DTO、view/response。
- Postgres、Redis、ClickHouse storage 基础设施。
- RBAC、JWT、Casbin、operation log、受治理 runtime config 机制。
- Polymarket Gamma、CLOB market data、L2 book ingest。
- `BookStore` published snapshot 模式。
- ClickHouse fact writer 与 async writer 模式。
- control-factor plane 中的 materialization、governance、audit 模式。

## 5. 文档契约

每个实施 Phase 必须写清楚：

- 添加替代代码前必须删除哪些 crate、模块、类型和配置；
- 新领域类型名、新表名、新 ClickHouse fact 名；
- deploy-config key 与 runtime-config v3 path；
- 生产不变量和失败语义；
- Phase 完成前必须更新的测试、benchmark、architecture lint。
- 第三方 crate 选型、feature gate、MSRV/native 依赖风险和替代方案。

如果某个 Phase 不能说明它删除了什么旧概念，就不允许进入实现。
