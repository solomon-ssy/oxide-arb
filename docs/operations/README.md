# 运维文档

面向**运维 / 量化 / 研发**的操作指南：如何启动 oxide-arb、验证机会与 PnL、沉淀 control factors 并接入 Live。

| 文档 | 读者 | 内容 |
|------|------|------|
| [runbook.md](./runbook.md) | 运维、量化 | **主手册**（含 Bot 钱包、凭证、Polymarket、DryRun/Paper/Live、因子发布） |
| [live-production-guide.md](./live-production-guide.md) | 运维、量化、决策 | **实盘生产指南**（Readiness 审计 + Live SOP + 充提 + 监控 + 上线门槛） |
| [bankroll-and-risk-metrics.md](./bankroll-and-risk-metrics.md) | 研发、运维 | 为什么 `risk.bankroll_usd` 是配置而非权威钱包余额 |
| [docker-integration.md](./docker-integration.md) | CI / 开发 | testcontainers 集成测试 |
| [network-integration.md](./network-integration.md) | CI / 开发 | 需外网的 Polymarket / RPC 集成测试 |

相关设计文档（非逐步操作手册）：

- [schema-catalog.md](../persistence/schema-catalog.md) — Postgres 表生命周期
- [replay-analytics-endgame-audit.md](../replay-analytics-endgame-audit.md) — control-factor 控制面动机
- [phase5.6-live-consumption.md](../plans/phase5.6-live-consumption.md) — 热路径因子快照接入
- [ADR-002](../plans/ADR-002-quant-signal-plane.md) — Quant Signal Plane 架构决策
- [phase9-quant-signal-plane.md](../plans/phase9-quant-signal-plane.md) — Top-N 量化报告母计划
