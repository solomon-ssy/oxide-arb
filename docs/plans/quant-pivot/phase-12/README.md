# Phase 12 — 执行授权、账户恢复与快速经济反馈

<!-- quant-pivot-deployment-contract:v1 -->
> **Deployment contract**
> - `fresh_boot_assumption`: 项目尚未正式生产上线；Phase 12 只交付唯一 fresh-boot schema。
> - `schema_data_version_impact`: 不迁移开发/测试旧数据，不保留旧 enum、表、wire、parser、双读或双写。
> - `pre_deployment_behavior`: 允许 clean break 和 bootstrap schema squash；任何真实数据销毁仍需单独授权。
> - `post_deployment_behavior`: 首次生产部署后恢复正常前向 migration、备份、验证和显式回滚纪律。
> - `rollback_and_data_verification`: 实施阶段只在 disposable PostgreSQL/ClickHouse 环境验证 fresh install。

> 状态：**IMPLEMENTATION IN PROGRESS**
>
> `operational_activation_claimed=false`

## 1. 唯一目标

Phase 12 破坏式删除 `QuantRuntimeMode`，把报告生成、入场授权、kill switch、结算授权和模型执行资格拆回各自的语义 owner；同时建立账户单写者、严格 break-glass 恢复和 recommendation 级可成交经济反馈。

本 Phase 的业务终态：

```text
facts -> report/recommendation
      -> analysis-only | operator approval | policy automatic ceiling
      -> governed system order -> CLOB observation -> finalized account chain execution
      -> strategy position lot -> actual execution outcome

recommendation -> frozen policy replay at profile horizon
               -> recommendation economic outcome
               -> route economic health

unknown external execution -> account recovery incident -> latched ExitOnly
break-glass UI exit -> user pause -> finalized replay -> exact reconciliation -> governed unpause
```

## 2. 阅读和恢复顺序

1. [`12.0-execution-authority-account-recovery-fast-feedback.md`](12.0-execution-authority-account-recovery-fast-feedback.md) — 唯一设计合同、删除/合并/迁移 inventory、接口和验收标准。
2. [`12.1-implementation-ledger.md`](12.1-implementation-ledger.md) — 唯一执行状态、Todo、证据、决策、阻断和中断恢复入口。
3. 父索引 [`../README.md`](../README.md) — 全系统阅读顺序与被 Phase 12 取代的旧语义。

任何中断恢复都从 12.1 ledger 开始；不得从聊天记录、日期化 audit、Phase 5 runtime-mode 文档或旧 Phase 11 history 推断 current task。

## 3. 硬边界

- 平台仍然只做 Polymarket。
- 默认入场授权是逐 intent operator approval；不创建/不批准 intent 就不会下单。
- 正常策略账户只有本系统一个写者；Polymarket UI 只允许严格 break-glass。
- `token_payout_ratio` 仍是 Buy 模型唯一终局监督目标；MTM 不得成为 fallback label。
- 不新增第二套 L2、fee、entry/exit 或 MTM replay engine。
- Sports 垂直延期到批准具有实时 SLA 和历史回放的数据源后；禁止公共源伪生产 skeleton。
- 零兼容、零 re-export、零死语义、零无 owner 的重复事实表。

## 4. 完成定义

Phase 12 只有在 12.1 ledger 零 `TODO/BLOCKED/IN_PROGRESS`、设计 hash fixed point、旧 runtime-mode 语义在活跃代码/schema/UI/文档中完全消失、fresh stacks 与全部工程/故障注入门禁通过后，才允许声明 Implementation Closure。

Implementation Closure 不等于 `PolicyAutomatic` Operational Activation；后者仍需真实账户、真实数据、terminal 模型/策略门禁、MTM coverage、健康 Route 和独立受治理激活。
