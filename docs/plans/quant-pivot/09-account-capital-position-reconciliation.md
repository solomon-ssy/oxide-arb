# 09 — Account, Capital, Position & Reconciliation Plane

<!-- quant-pivot-deployment-contract:v1 -->
> **Deployment contract**
> - `fresh_boot_assumption`: 项目尚未正式生产上线，将从全新 `boot` / schema version `1` 部署；仓库和数据库不保存 lifecycle seal 状态。
> - `schema_data_version_impact`: 本文中的历史版本号与递增路径不再具有实施效力；当前实现不迁移测试数据、旧结构或旧版本。
> - `pre_deployment_behavior`: 允许 clean-break、migration squash 与全新基础设施 bootstrap，但任何数据销毁仍需操作者单独授权。
> - `post_deployment_behavior`: 本次实现只交付唯一终态 clean-install contract；不设计升级、降级、旧版本共存或历史数据转换。
> - `rollback_and_data_verification`: 仅在 disposable 空基础设施执行 fresh-install 验证；任何真实数据重置需要操作者另行授权。

> Status: design index. Implementation is split across Phase 04/05/06 docs.

## Current Authority

- `AccountSnapshot` is decision-time venue evidence. It separates `venue_net_liquidation_usd`
  from `capital_base_usd`; the latter is the strategy sizing and drawdown basis.
- `quant_equity_snapshot` is the PG source of truth for strategy-capital equity history,
  high-water mark, and drawdown. ClickHouse mirrors, if added later, are analytical only.
- `quant_position` is the strategy lot ledger. Realized PnL comes from repository aggregates,
  not from scanning report snapshots.
- `quant_capital_allocation` is the reserved/spent/released capital ledger for intents.
- `quant_reconciliation` records venue-vs-ledger repair evidence and outcomes.

## Implemented Phase Mapping

- Phase 04: report-time account snapshots and capital-aware portfolio planning.
- Phase 05.2/05.4: governed `OrderIntent`, capital reservation, execution submission.
- Phase 05.5: reconciliation worker and evidence chain.
- Phase 05.6: per-lot position ledger, exits, realized PnL.
- Phase 05.9: equity history and drawdown-aware sizing.

## Deferred

- Phase 06: operator-facing cross-account reconciliation report and observability rollups.
- Phase 06+: optional ClickHouse equity mirror for analytics; PG remains authoritative.
- Phase 08: multi-replica leader election / cross-instance worker locks.
