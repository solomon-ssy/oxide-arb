# 09 — Account, Capital, Position & Reconciliation Plane

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
