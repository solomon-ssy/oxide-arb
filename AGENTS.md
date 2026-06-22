# AGENTS.md — Coding Agent Guide for quant-pivot

Primary onboarding guide for AI agents and contributors. Active architecture lives in [`docs/plans/quant-pivot/`](docs/plans/quant-pivot/README.md). Legacy Endgame phase docs are **superseded** — deletion inventory only.

## 1. What This System Is

**quant-pivot** is a Polymarket-only quantitative system. It:

1. Ingests Gamma metadata and CLOB L2 books into a lock-free **BookStore**.
2. Writes ClickHouse facts and data-quality signals (Phase 2+).
3. Builds features, factors, and models (Phase 3+).
4. Produces periodic **RecommendationReport** (TopN) as the primary artifact (Phase 4+).
5. Optionally executes via **OrderIntent** under `semi_auto` or `auto_execution` (Phase 5+).

Default mode: **`QuantRuntimeMode::ReportOnly`** — reports only, no orders, no private keys required.

## 2. Hard Boundaries

| Rule | Detail |
|------|--------|
| Platform | Polymarket only — no `VenueId`, no multi-exchange |
| Primary artifact | `RecommendationReport` / `Recommendation` — not `ScoredOpportunity` |
| Runtime modes | `ReportOnly`, `SemiAuto`, `AutoExecution` — **no** DryRun/Paper/Live |
| Compatibility | Zero re-export shim, zero runtime-config v2 parser |
| Money | `rust_decimal` newtypes — never `f64` for prices/USD/shares |

## 3. Workspace Crates

```
quant-pivot/
├── config/quant-pivot.toml
├── crates/
│   ├── quant-pivot-bin
│   ├── quant-pivot-core      # AppContext, data ingest, governance
│   ├── quant-pivot-api       # Polymarket clients
│   ├── quant-pivot-models
│   ├── quant-pivot-error
│   ├── quant-pivot-storage
│   ├── quant-pivot-repository
│   ├── quant-pivot-web
│   ├── quant-pivot-bench
│   ├── quant-pivot-test-support
│   ├── quant-pivot-macros
│   └── quant-pivot-xtask
└── docs/plans/quant-pivot/
```

**Phase 0 deleted:** `quant-pivot-algorithm`, old `quant-pivot-risk`, `quant-pivot-control`.

## 4. Phase 0 Runtime (Current)

`AppContext` bundles:

- **InfraBundle** — DB pools, Redis, ClickHouse, metrics, alerts
- **DataBundle** — BookStore, MarketRegistry, DataPipeline (ingest only)
- **GovernanceBundle** — RuntimeConfigStore, RuntimeModeHandle

No detection funnel, no FOK execution hot path, no post-trade relay.

## 5. Domain Vocabulary

See [`.cursor/rules/quant-pivot-domain.mdc`](.cursor/rules/quant-pivot-domain.mdc).

## 6. DTO & Persistence

Three DTO families unchanged in pattern:

- `*Request` / `*Query` — wire, validated
- `*Info` / `New*` / `*Patch` — persistence
- `*View` — API responses

Authoritative: [`docs/models/dto-paradigm.md`](docs/models/dto-paradigm.md).

## 7. Quality Gates

```bash
cargo fmt --all --
cargo clippy --workspace --all-targets -- -D warnings
bash scripts/lint-architecture.sh
bash scripts/lint-quant-pivot-boundary.sh
cargo test --workspace
```

## 8. Forbidden Patterns

| Forbidden | Instead |
|-----------|---------|
| `EndgameDetector`, `ScoredOpportunity`, `OpportunityPipeline` | quant report pipeline types |
| `ExecutionMode::DryRun/Paper/Live` | `QuantRuntimeMode` |
| `pub use` compatibility re-exports | explicit module paths |
| `unwrap()` in `src/` | `?` / structured errors |
| `f64` for money | `Usd` / `Price` / `Shares` |
| I/O inside hot-path risk checks | pre-fetch into context |

## 9. Implementation Phases

Follow [`docs/plans/quant-pivot/07-implementation-phases.md`](docs/plans/quant-pivot/07-implementation-phases.md). Do not skip Phase exit criteria.

## 10. Reference Index

| Document | Topic |
|----------|-------|
| [quant-pivot/README.md](docs/plans/quant-pivot/README.md) | Architecture index |
| [00-quant-pivot-architecture.md](docs/plans/quant-pivot/00-quant-pivot-architecture.md) | Target architecture |
| [02-crate-refactor-and-deletion-plan.md](docs/plans/quant-pivot/02-crate-refactor-and-deletion-plan.md) | Deletion inventory |
| [quant-pivot-domain.mdc](.cursor/rules/quant-pivot-domain.mdc) | Domain rules |
| [quant-pivot-rust-style.mdc](.cursor/rules/quant-pivot-rust-style.mdc) | Rust style (still applies) |
