# AGENTS.md — Coding Agent Guide for quant-pivot

Primary onboarding guide for AI agents and contributors. Active architecture lives in [`docs/plans/quant-pivot/`](docs/plans/quant-pivot/README.md). Legacy Endgame phase docs are **superseded** — deletion inventory only.

## 1. What This System Is

**quant-pivot** is a Polymarket-only quantitative system. It:

1. Ingests Gamma metadata and CLOB L2 books into a borrowed-read **BookStore** facade.
2. Writes ClickHouse facts and data-quality signals (Phase 2+).
3. Builds features, factors, and models (Phase 3+).
4. Produces periodic **RecommendationReport** (TopN) as the primary artifact (Phase 4+).
5. Optionally executes via **OrderIntent** under `semi_auto` or `auto_execution` (Phase 5+).

Default mode: **`QuantRuntimeMode::ReportOnly`** — the report is the final artifact (human places orders manually); the system never signs/submits orders. **`ReportOnly` is NOT dry-run**: report sizing is built on the **real venue account** (CLOB collateral + Data API positions), so a private key (for reads / L2 read credentials) and a `funder` address are required to generate reports. Private keys are used for **signing/submitting** orders only in `SemiAuto`/`AutoExecution`. Account truth is **credential-gated, not mode-gated**; missing credentials → report generation fails closed (no simulation/no configured-budget fallback).

## 2. Hard Boundaries

| Rule | Detail |
|------|--------|
| Platform | Polymarket only — no `VenueId`, no multi-exchange |
| Primary artifact | `RecommendationReport` / `Recommendation` — not `ScoredOpportunity` |
| Runtime modes | `ReportOnly`, `SemiAuto`, `AutoExecution` — **no** DryRun/Paper/Live |
| Compatibility | Zero compatibility shim, forwarding re-export, legacy parser, or dual write |
| Money | `rust_decimal` newtypes — never `f64` for prices/USD/shares |

## 3. Workspace Crates

```
quant-pivot/
├── config/quant-pivot.toml
├── ui/                       # Admin SPA (git submodule)
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

## 4.1 Performance architecture invariants

- Internal UUID-backed IDs and `ContentHash` are fixed-width `Copy` values. `Arc<Uuid>` is
  forbidden; string-backed venue IDs remain explicit boundary types.
- `DataPlaneIndex` is an immutable `ArcSwap` snapshot. Hot routing uses `TokenKey`; mutable books
  have exactly one owner among 8 token-affine partition actors.
- Ingress is batched and bounded by both mailbox slots and a shared byte semaphore. Timeout,
  unknown token, sequence discontinuity, or persistence failure invalidates the affected session
  and fails closed.
- `quant_book_l2_ledger` is the only canonical L2 table. It stores typed decimal arrays and a raw
  32-byte domain-separated BLAKE3 digest. Legacy event/checkpoint tables, JSON checkpoints,
  compatibility readers, dual writes, and version-suffixed table names are forbidden.
- A partition publishes its `TokenSlot` only after the persistent commit cursor reports durable
  success. `BookStore::read` uses an `ArcSwap` guard; owned loads are reserved for crossing an
  `await` or task boundary.
- WebSocket lifecycle and fanout have one `SessionHub` writer and topic/subject/family indexes.
  Each event is encoded once as `ByteString`; scanning every session, per-session subscription
  locks, and `Sender<String>` are forbidden.
- All queues are bounded and document backpressure, cancellation, and drain semantics. New
  `spawn_blocking` sites are rejected unless added to the architecture allowlist with explicit CPU
  and memory budgets.
- Hard SLOs, benchmark runner requirements, the fixed jemalloc policy, and inline evidence thresholds live in
  [`docs/operations/performance.md`](docs/operations/performance.md). Work status is recovered only
  from [`09-extreme-performance-ledger.md`](docs/plans/quant-pivot/09-extreme-performance-ledger.md),
  which may contain at most one `in_progress` task; while execution is active it must contain
  exactly one, and after completion/blocking it may contain none.

## 5. Domain Vocabulary

See [`.cursor/rules/quant-pivot-domain.mdc`](.cursor/rules/quant-pivot-domain.mdc).

## 6. DTO & Persistence

Three DTO families unchanged in pattern:

- `*Request` / `*Query` — wire, validated
- `*Info` / `New*` / `*Patch` — persistence
- `*View` — API responses

Authoritative: [`docs/models/dto-paradigm.md`](docs/models/dto-paradigm.md).

SeaORM schema, JSONB/newtype/enum, query loading and transaction rules are mandatory in
[`docs/persistence/seaorm-and-typed-persistence.md`](docs/persistence/seaorm-and-typed-persistence.md)
and [`.cursor/rules/quant-pivot-persistence.mdc`](.cursor/rules/quant-pivot-persistence.mdc).
In particular, domain-owned JSONB uses a canonical typed struct/tagged enum with
`FromJsonQueryResult`; values requiring SQL query/constraint/lifecycle semantics are
normalized into columns/tables instead of JSONB.

## 7. Quality Gates

```bash
cargo fmt --all --
cargo clippy --workspace --all-targets -- -D warnings
cargo xtask architecture audit-functions
cargo xtask architecture check
cargo test --workspace
```

## 7.1 Import style

Full rules: [`.cursor/rules/quant-pivot-rust-style.mdc`](.cursor/rules/quant-pivot-rust-style.mdc).
Enforced structurally by `cargo xtask architecture check`.

| Rule | Detail |
|------|--------|
| Module preamble only | `use` belongs at the file/module head, including the head of `mod tests`; never inside a function, method, closure, loop or block |
| One tree per root | With the same visibility and attributes, every root has one tree-shaped import: `use std::{cmp::Ordering, panic::{self, AssertUnwindSafe}};` |
| Imported body symbols | Structs, enums, traits, types and constants are imported into the module preamble; bodies do not spell deep fully qualified paths |
| Body qualifiers | Body paths normally have at most one `::`: `Side::Buy`, `task::spawn_blocking`; Tokio functions may retain `tokio::time::timeout` / `tokio::task::spawn_blocking` |
| Collision aliases | Import conflicting owners with a semantic alias, e.g. `use anyhow::Error as AnyhowError;` and call `AnyhowError::from` |
| Alias semantics | Aliases name the role/domain (`MarketEntity`, `SdkError`, `ChronoDuration`); aliases assembled from a full internal path are forbidden |
| Attribute exception | Imports with different leading attributes may remain separate because stable Rust cannot attach `#[cfg]` to members inside one `use` tree |
| Public paths | Canonical bounded-context facades are allowed; compatibility and forwarding re-exports are forbidden |
| SeaORM relation exception | `#[sea_orm::model]` relation descriptors import the relation module and retain `module::Entity`; the derive macro requires the final identifier to remain literally `Entity` |
| Macro/attribute exception | A `macro_rules!` definition may be followed by its restricted re-export; framework attribute/derive paths such as `#[sea_orm::model]` remain canonical qualified invocations |
| Associated-item exception | Primitive/generic owner paths such as `i64::MAX`, `D::Error::custom`, and `Self::Error` remain qualified |

```rust
use std::{cmp::Ordering, panic::{self, AssertUnwindSafe}};

use anyhow::Error as AnyhowError;
use quant_pivot_models::enums::Side;

let ordering = Ordering::Equal;
let side = Side::Buy;
let result = tokio::task::spawn_blocking(work).await;
let error = AnyhowError::from(source);
```

## 7.2 Function design

`cargo xtask architecture audit-functions` is the authoritative full-workspace AST audit and is
also embedded in `cargo xtask architecture check`.

| Rule | Required form |
|------|---------------|
| Zero-argument nominal factory | associated constructor on the returned type; `Default` when exactly equivalent |
| Unary nominal behavior | inherent method on the semantic owner; a shared extension trait only when crate direction or orphan rules prevent ownership and the trait has genuine reuse |
| Context-free conversion | `From`/`Into`, `TryFrom`/`TryInto`, `FromStr`, `Display`, or serde |
| Multiple operands | receiver/associated function on the invariant owner; constructor on the result; named input value when the inputs jointly form a stable concept |
| Repository I/O | method/associated operation on the concrete repository, store, or adapter that owns the transaction invariant; never choose a receiver merely because it is the first parameter |
| Thin forwarding | delete and inline unless the layer adds validation, typed error semantics, transactionality, authorization, observability, or a real public boundary |
| Function name | at most four non-empty `snake_case` words in production, tests, benchmarks, macros, and xtask |
| API replacement | migrate every caller and delete the old path; no deprecated alias, compatibility wrapper, or forwarding re-export |

Free functions remain valid only for ownerless pure algorithms, framework/trait/ABI-required
entries, callbacks, and macro/proc-macro entry points. Multi-domain algorithms with policy, time,
I/O, or strategy context are reviewed semantically rather than assigned to their first argument.
Structural exceptions are derived from syntax; path and function-name allowlists are forbidden.

## 7.3 Error layering

Platform failures live in **`quant-pivot-error`** as typed sub-errors composed into
[`QuantError`](crates/quant-pivot-error/src/lib.rs) via `#[from]`. Third-party errors
Third-party SDK and Polars errors convert through **facade newtypes** in the owning
crate, never inside `quant-pivot-error`.
HTTP status mapping stays in **`quant-pivot-web/src/error.rs`**.

| Sub-error | Domain |
|-----------|--------|
| `ReportError` | Report pipeline invariants / contract violations |
| `InfraError` | Bootstrap, metrics, channels, web server runtime |
| `ControlError` | Runtime mode / config apply / book subscriptions |
| `QueryError` | Inbound API time-window validation |

`QuantError` has no catch-all string variant. Every platform failure must enter through a
domain-specific typed sub-error and its `From` conversion. HTTP-only masking and status mapping
stay in `quant-pivot-web::error::WebError` and are covered by mapping tests.

`StorageError` persistence variants (no string bucket — `Conflict(String)` removed):

| Variant | Semantics | Typical HTTP |
|---------|-----------|--------------|
| `NotFound { entity, id }` | Row absent | 404 |
| `Duplicate { entity, key }` | Unique/PK violation | 409 |
| `IllegalTransition { entity, id, from, to }` | FSM rejected | 409 |
| `StateConflict { entity, id, detail }` | Wrong lifecycle state | 409 |
| `InvariantViolation { entity, detail }` | Caller payload invalid | 400 |

Idempotent writes (e.g. attribution final insert) return repository **outcome enums**
(`InsertFinalOutcome`) rather than treating duplicate as `Err`.

## 8. Forbidden Patterns

| Forbidden | Instead |
|-----------|---------|
| `EndgameDetector`, `ScoredOpportunity`, `OpportunityPipeline` | quant report pipeline types |
| `ExecutionMode::DryRun/Paper/Live` | `QuantRuntimeMode` |
| `pub use` compatibility re-exports | explicit module paths |
| bare `Json` / `serde_json::Value` persistence | relation/typed columns, or canonical `FromJsonQueryResult` document |
| Split same-root imports with the same visibility/attributes | one tree: `use std::{cmp::Ordering, panic::{self, AssertUnwindSafe}};` |
| `use` inside a function/method/block | import at the owning file or nested module preamble |
| Deep body path such as `quant_pivot_models::enums::Side::Buy` | import the owner and use `Side::Buy`; only Tokio function paths receive the explicit three-segment exception |
| `unwrap()` in `src/` | `?` / structured errors |
| `QuantError::Internal(` in production `src/` | typed `quant-pivot-error` sub-variant |
| `fn *_error(` manual mappers | `From` + `?` |
| Context-free `to_*` / `try_to_*` conversion | `From` / `TryFrom` on the destination |
| Private exact forwarding function | inline at its callers |
| More than four words in a function name | shorter owner-aware name; use nested test modules for context |
| Repository transaction free helper | concrete repository/store/adapter method |
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
