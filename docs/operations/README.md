# 运维文档

面向**运维 / 量化 / 研发**的操作指南：如何启动 quant-pivot、准备 Polymarket 账户和凭证、生成报告、治理下单、退出仓位、处理 reconciliation，并理解整个业务闭环。

| 文档 | 读者 | 内容 |
|------|------|------|
| [runbook.md](./runbook.md) | 运维、量化、审批人 | **主手册**：运行前准备、API key/私钥/funder、充值、提现、报告、`semi_auto` / `auto_execution` 下单、卖出、赎回、事故处理 |
| [architecture-and-design.md](./architecture-and-design.md) | 研发、运维、量化 | **架构与详细设计**：从 Gamma/CLOB/Data API 到报告、组合、执行、reconciliation、exit、settlement、attribution 的完整闭环 |
| [docker-integration.md](./docker-integration.md) | CI / 开发 | testcontainers 集成测试 |
| [network-integration.md](./network-integration.md) | CI / 开发 | 需外网的 Polymarket / RPC 集成测试 |

相关设计文档（非逐步操作手册）：

- [quant-pivot architecture index](../plans/quant-pivot/README.md) — 当前 quant-pivot 规划入口
- [00-quant-pivot-architecture.md](../plans/quant-pivot/00-quant-pivot-architecture.md) — 目标架构背景
- [04-topn-report-and-recommendation.md](../plans/quant-pivot/04-topn-report-and-recommendation.md) — Top-N report 设计背景
- [05-execution-risk-and-governance.md](../plans/quant-pivot/05-execution-risk-and-governance.md) — 执行、风控和治理背景
- [09-account-capital-position-reconciliation.md](../plans/quant-pivot/09-account-capital-position-reconciliation.md) — 账户、资金、仓位、reconciliation 背景
- [schema-catalog.md](../persistence/schema-catalog.md) — Postgres 表生命周期
