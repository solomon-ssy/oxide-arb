# AGENTS.md — Coding Agent Guide for oxide-arb

This document is the **primary onboarding and enforcement guide** for AI coding agents and human contributors working in the oxide-arb workspace. It consolidates and expands the rules in [`.cursor/rules/`](.cursor/rules/). When in doubt, this file's **quality gates** and **ADR-001 (single strategy, single platform)** take precedence over informal patterns in legacy code.

---

## Table of Contents

1. [What This System Is](#1-what-this-system-is)
2. [Architecture Decisions (ADR-001)](#2-architecture-decisions-adr-001)
3. [Technology Stack & Toolchain](#3-technology-stack--toolchain)
4. [Workspace Layout & Crate Boundaries](#4-workspace-layout--crate-boundaries)
5. [Runtime Architecture (`AppContext`)](#5-runtime-architecture-appcontext)
6. [End-to-End Data Flows](#6-end-to-end-data-flows)
7. [Domain Vocabulary](#7-domain-vocabulary)
8. [Money, Fees & Polymarket Rules](#8-money-fees--polymarket-rules)
9. [Execution Modes & Safety Model](#9-execution-modes--safety-model)
10. [Configuration System](#10-configuration-system)
11. [Persistence Layer](#11-persistence-layer)
12. [DTO & HTTP API Contract Layer](#12-dto--http-api-contract-layer)
13. [Web Layer (`oxide-arb-web`)](#13-web-layer-oxide-arb-web)
14. [Control Factor Plane](#14-control-factor-plane)
15. [ClickHouse Conventions](#15-clickhouse-conventions)
16. [Error Handling](#16-error-handling)
17. [Rust Coding Standards](#17-rust-coding-standards)
18. [Testing Conventions](#18-testing-conventions)
19. [Quality Gates (Mandatory)](#19-quality-gates-mandatory)
20. [Forbidden Patterns](#20-forbidden-patterns)
21. [How to Extend the Codebase](#21-how-to-extend-the-codebase)
22. [Frontend (`oxide-arb-ui`) — Only When Asked](#22-frontend-oxide-arb-ui--only-when-asked)
23. [Agent Working Principles](#23-agent-working-principles)
24. [Reference Index](#24-reference-index)

---

## 1. What This System Is

**oxide-arb** is a **Polymarket Endgame Convergence** arbitrage bot. It:

1. Ingests market metadata (Gamma) and real-time order books (CLOB WebSocket).
2. Maintains a lock-free **`BookStore`** snapshot for hot-path readers.
3. Runs detection through Scanner / Funnel → Algorithm (walker, endgame detector, calibration, scoring).
4. Produces **`ScoredOpportunity`** candidates.
5. Evaluates each candidate through a **static risk pipeline** (no I/O on the hot path).
6. Executes approved trades via **`ExecutionPipeline`** on the CLOB (**FOK-only**), with capital reservations and safety gates.
7. Persists durable trade state and drives **post-trade** processing (fill accounting, positions, reconciliation, settlement/redeem).
8. Exposes an **admin/control plane** (HTTP + WebSocket + RBAC) for operations, analytics, and governed mutations.

The bot is designed for **capital safety first**: fail closed, circuit breakers, exposure limits, reconciliation before auto-recovery, and mode-aware validation (`DryRun` / `Paper` / `Live`).

---

## 2. Architecture Decisions (ADR-001)

Approved decision: **single strategy (Endgame only), single platform (Polymarket on Polygon only)**.

Full ADR: [`docs/plans/ADR-001-single-strategy-single-platform.md`](docs/plans/ADR-001-single-strategy-single-platform.md)

### Keep

| Area | Decision |
|------|----------|
| Strategy | Endgame convergence detection only |
| Platform | Polymarket CLOB + Gamma + CTF on Polygon |
| Execution | **FOK-only** execution; directional positions (no hedging) |
| Scoring | Resolution calibration, Quarter-Kelly sizing, fill probability |
| Oracle | 2-of-3 voting oracle for resolution (kept, but not for multi-platform abstraction) |

### Do Not Introduce

- Multi-venue routing (`VenueId`, generic venue adapters).
- Multi-strategy dispatch (`Strategy` trait, strategy registry/factory).
- Cross-book / statistical arb opportunity types.
- Multi-leg hedging FSM states.
- Generic `FeeCalculator` trait with multiple implementations (Polymarket formula is the only one).
- Abstractions "just in case" another exchange is added later.

When you see remnants of these patterns in old docs or comments, treat them as obsolete unless the user explicitly asks to restore them.

---

## 3. Technology Stack & Toolchain

| Layer | Choice |
|-------|--------|
| Language | Rust **2024 edition**, MSRV **1.85** (`rust-version` in root `Cargo.toml`) |
| Async runtime | Tokio (full features) |
| ORM / Postgres | SeaORM + `sea-orm-migration` |
| Cache | Redis (`deadpool-redis`), in-process `moka` |
| Analytics store | ClickHouse (`clickhouse` crate) |
| HTTP admin API | actix-web 4 + Casbin RBAC + JWT |
| External trading API | `polymarket_client_sdk_v2` (CLOB, WS, Gamma, CTF) |
| Chain | Polygon via `alloy`; EIP-712 signing |
| Money | `rust_decimal` — **never `f64`** |
| Logging | `tracing` + structured fields |
| Metrics | Prometheus (`prometheus` crate) |
| Property/snapshot tests | `proptest`, `insta` |

### Workspace Lint Policy (root `Cargo.toml`)

- `unsafe_code = "deny"` — no `unsafe` anywhere.
- Clippy: `all = deny`, `pedantic = warn`, `nursery = warn`.
- Deliberate allow: `future_not_send` in actix-web handlers (architectural; actix worker model is `!Send` by design).

### Release Profile

Production builds use `lto = "fat"`, `codegen-units = 1`, `panic = "abort"`. Hot-path changes should be validated in `oxide-arb-bench`.

---

## 4. Workspace Layout & Crate Boundaries

```
oxide-arb/
├── config/oxide-arb.toml              # Default runtime config
├── crates/
│   ├── oxide-arb-bin                  # Process entry; TLS CryptoProvider install
│   ├── oxide-arb-core                 # App wiring, pipelines, execution, post-trade
│   ├── oxide-arb-algorithm           # Pure detection math (no I/O)
│   ├── oxide-arb-risk                 # Pre-trade checks, circuit breaker, exposure
│   ├── oxide-arb-api                  # Polymarket SDK wrappers
│   ├── oxide-arb-models               # Domain types, config, DTO layers, idens
│   ├── oxide-arb-error                # Unified OxideError tree
│   ├── oxide-arb-storage              # DB connections, migrations
│   ├── oxide-arb-repository         # Repository traits + Postgres/CH impls
│   ├── oxide-arb-control              # Control factor control plane
│   ├── oxide-arb-web                  # HTTP routes, auth, WS admin
│   ├── oxide-arb-bench                # Criterion benchmarks
│   ├── oxide-arb-test-support         # Mocks, harnesses, smoke fixtures
│   ├── oxide-arb-macros               # Proc macros (`#[oxide_schema]`, etc.)
│   └── oxide-arb-xtask                # Maintenance CLI
├── docs/                              # Design docs, runbooks, ADRs
├── scripts/                           # CI gates, architecture lint, PGO
└── oxide-arb-ui/                      # vben fork (only when user asks)
```

### Crate Responsibilities

| Crate | Owns | Must NOT own |
|-------|------|--------------|
| `oxide-arb-models` | Typed IDs, money newtypes, enums, runtime config, `domain::*` DTOs, `domain::api` wire types, schema idens | Business orchestration, I/O |
| `oxide-arb-error` | `OxideError`, domain sub-errors, `OxideResult<T>` | Business logic |
| `oxide-arb-api` | CLOB/WS/Gamma/CTF clients, fee calc, keystore, oracle; maps to domain/`PipelineEvent` | Risk decisions, persistence |
| `oxide-arb-algorithm` | Walker, endgame, calibration, scoring; pure computation via injected traits | Connections, runtime state |
| `oxide-arb-risk` | `StaticRiskPipeline`, `RiskEngine`, circuit breaker FSM, exposure snapshots | SeaORM entities, HTTP |
| `oxide-arb-core` | BookStore, detection funnel, execution pipeline, capital, settlement, post-trade relay | Raw HTTP handlers |
| `oxide-arb-storage` | Connection pools, migration runner | Repository business queries |
| `oxide-arb-repository` | Trait definitions + Postgres/ClickHouse implementations | HTTP, trading logic |
| `oxide-arb-web` | actix routes, JWT auth, Casbin middleware, WS admin | Wire type definitions (those live in `models`) |
| `oxide-arb-control` | Factor registry, materialization, governance API surface | Hot-path execution |
| `oxide-arb-bin` | `main`, graceful shutdown, CryptoProvider bootstrap | Feature logic |

### Dependency Direction (Enforced)

`scripts/lint-architecture.sh` checks:

1. **`oxide-arb-core` / `oxide-arb-risk` / `oxide-arb-algorithm` must NOT import** `sea_orm` or `oxide_arb_models::entities`.
2. **Write paths must NOT accept** `*Info` or `*RegistryInfo` as input types.
3. **No cross-crate re-exports** of `oxide_arb_models::` or `oxide_arb_error::` from `api` / `core` / `storage` `lib.rs` or `mod.rs`.
4. **`from_decimal_unchecked` forbidden** in `oxide-arb-api/src/ws/` and `oxide-arb-api/src/clob/` ingest paths.
5. **`data_pipeline.rs` success path must NOT clone** `PipelineEvent` on `try_send`.

Repository write verbs are exactly five: `create`, `create_batch`, `update`, `upsert`, `upsert_batch`. Writes accept `New*` / `Upsert*` only — never `*Info`.

---

## 5. Runtime Architecture (`AppContext`)

Application wiring lives in `oxide-arb-core/src/app/`. The runtime is composed of bundles:

| Bundle | Responsibility |
|--------|----------------|
| `InfraBundle` | DB pools, Redis, ClickHouse, metrics hub, alert dispatcher |
| `DataBundle` | BookStore, market catalog, WS/Gamma ingest, pipeline channels |
| `RiskBundle` | `RiskEngine`, exposure tracker, blacklist, metrics state |
| `ExecutionBundle` | `ExecutionPipeline`, `CapitalManager`, `ExecutionFSM`, CLOB dispatcher |
| `TradingBundle` | Mode handle, trading gate, venue guard |
| `ControlFactorBundle` | Factor snapshot store, scheduler, materialization workers |
| `SettlementBundle` | Redeem preflight, payout service, CTF client |
| `RuntimeChannels` | Internal async channels (coalescer, relay notify, etc.) |

`AppContext` (`app/mod.rs`) queues background tasks:

- Post-trade relay + consumer
- Reconciliation worker
- Execution heartbeat + runners
- Risk decision audit drain
- Market settlement tasks
- Control factor scheduler + materialization execute worker
- Periodic services (metrics refresh, etc.)

**Rule:** Hot-path code paths wired in `build.rs` must not perform blocking I/O. Background workers own retries, polling, and crash recovery.

---

## 6. End-to-End Data Flows

### 6.1 Detection Hot Path

```text
Gamma API (metadata) ──┐
                         ├──► Market catalog / token registry
CLOB WS (L2 books) ──────┘
         │
         ▼
BookStore
  • live map: writers update in-memory books
  • published: ArcSwap snapshot — ALL hot-path readers use this only
         │
         ▼
Scanner / Detection Funnel
  • coalesce / dedup / spill on channel pressure
  • backpressure must NOT halt trading globally
         │
         ▼
Algorithm Pipeline (oxide-arb-algorithm)
  • OrderbookWalker — simulates fills through book levels
  • EndgameDetector + InMemoryConvergenceTracker
  • ResolutionCalibrator (MoM priors, bucket fallback)
  • EndgameScorer — composite ranking
  • InMemoryEmissionCooldown — per-market duplicate suppression
         │
         ▼
ScoredOpportunity ──► (optional) ClickHouse audit / detection facts
```

`oxide-arb-algorithm` is **pure computation**. Fee estimation and calibration persistence are injected via traits — the crate holds no connections.

### 6.2 Risk Evaluation Hot Path

```text
ScoredOpportunity + book snapshot + exposure state
         │
         ▼
PreTradeContext (immutable per evaluation)
  • RiskSnapshot (ArcSwap-published state — no I/O)
  • live metrics (phase-2 checks only)
         │
         ▼
StaticRiskPipeline (oxide-arb-risk)
  • statically registered checks in deterministic order
  • ShortCircuit mode: stop on first hard gate failure
  • FullReport mode: evaluate all (diagnostics)
  • metrics_split index: checks before split use snapshot only
         │
         ▼
RiskDecision (Allow / Deny with reason + check id)
```

Registered checks include (non-exhaustive): manual halt, circuit breaker, blacklists, control factor snapshot expiry, reconciliation maintenance, redeem route resolvable, staleness, depth, exposure caps, loss caps, WS connectivity, API error rate, drawdown guard, duplicate market, etc. See `oxide-arb-risk/src/pipeline/mod.rs`.

**Invariant:** Individual `RiskCheck::evaluate` must not perform I/O. All external state must be pre-fetched into `PreTradeContext` / `RiskSnapshot`.

### 6.3 Execution Hot Path

```text
Approved ScoredOpportunity
         │
         ▼
PlanBuilder — order plan (size, price, FOK)
         │
         ▼
CapitalManager — reserve capital before dispatch
         │
         ▼
ExecutionPipeline
  • BlockingTradesCheck / VenueGuard / ExecutionFSM emergency check
  • CLOB order submission (Live) or simulation (DryRun/Paper)
  • durable trade row: create → submitted → observed
         │
         ▼
PostTradeRelay (notify-woken + periodic poll for crash recovery)
         │
         ▼
PostTradeConsumer (idempotent)
  • risk fill accounting
  • position create (idempotent)
  • terminal state advance
```

`ExecutionFSM` is a **global kill switch** (not the old Idle→Validate→Exec per-order FSM). Per-market concurrency is tracked by `MarketInFlightRegistry`.

Emergency classes (`EmergencyClass`):

| Class | Auto-recover allowed? |
|-------|----------------------|
| `VenueFault` | Yes, when safe |
| `ReservationFault` | No — requires operator ack |
| `PersistenceFault` | Never auto-recover |

### 6.4 Reconciliation & Settlement (Background)

```text
Trades in reconcile-pending / ambiguous states
         │
         ▼
ReconciliationWorker (batch poll + notify)
  • evidence ladder: CLOB trades, CTF balance, competing orders
  • economics: fill economics, reservation adjustment, resolution prob
  • defer-only policy — never force-close without evidence
         │
         ▼
TradeReconcileResolution → terminal trade state
         │
         ▼
SettlementService (when market resolves)
  • redeem preflight
  • CTF payout / redeem routing
```

Reconciliation can engage `ExecutionFSM` emergency halt when evidence is insufficient. `BlockingTradesCheck` and `TradeIntegrityStore` block admission/resume while blocking durable trade rows exist.

### 6.5 Admin / Control Plane (Off Hot Path)

```text
HTTP /api/* (actix-web, JWT + Casbin)
  • CRUD for markets, trades, users, roles, runtime config
  • governed mutations (mode change, factor publish, breaker reset) require X-Acting-Role + reason
         │
         ▼
WebSocket /api/ws
  • subscribe/unsubscribe/sync/ping
  • realtime dashboards (NOT removed events: trade.opened, opportunity.expired)
         │
         ▼
Control factor scheduler
  • shadow → publish workflow
  • hot-path reads published snapshot via ArcSwap (no DB on hot path)
```

---

## 7. Domain Vocabulary

Use these terms consistently across code, logs, metrics, and docs.

| Term | Definition | Common Mistake |
|------|------------|----------------|
| **MarketId** | Polymarket `condition_id`: `0x…`, 66 chars (`varchar(66)`) | Confusing with TokenId |
| **TokenId** | CLOB outcome token: decimal string | Storing as MarketId |
| **EventId** | Polymarket event identifier (text) | — |
| **Opportunity** | Scored arb candidate after algorithm pipeline | Raw detection event |
| **OpportunityId** | UUID v7 when assigned (time-sortable) | Using v4 on hot insert paths |
| **TradeId** | UUID v7 internal trade row id | — |
| **Endgame** | Price near $1 convergence zone; calibration buckets (`DurationBucket`, `PriceZone`) | Any high-price market |
| **BookStore** | `live` write map + `published` ArcSwap read snapshot | Reading `live` on hot path |
| **RiskSnapshot** | Immutable ArcSwap-published risk state for checks | Live DB reads in checks |
| **PreTradeContext** | Per-evaluation inputs: snapshot + opportunity + metrics | Mutating during check |
| **ExecutionMode** | `DryRun` / `Paper` / `Live` | Ignoring mode in validation |
| **ControlFactor** | Tunable runtime parameter with shadow/publish governance | Direct config file edit in Live |
| **Patch\<T\> / NullablePatch\<T\>** | Persistence partial-update semantics | `Option<Option<T>>` in domain layer |
| **`*Info` / `New*` / `*Patch`** | Persistence DTO family | Merging with `*Request` |
| **`*Request` / `*Query` / `*View`** | HTTP wire contract family | Putting `#[validate]` on `*Info` |

Typed IDs:

- Internal UUID ids (`TradeId`, `UserId`, `OpportunityId`): `XxxId::from_v7()` for time-ordered rows, `from_v4()` for random.
- External string ids (`MarketId`, `TokenId`): `StrId` newtype wrapping `Arc<str>`.

---

## 8. Money, Fees & Polymarket Rules

### Money Newtypes (`oxide-arb-models/src/types/money.rs`)

| Type | Postgres NUMERIC | Valid Operations |
|------|------------------|------------------|
| `Usd` | `(28, 8)` | Scalar ops; `Shares * Price → Usd` |
| `Price` | `(20, 18)` | `Usd / Price → Shares` |
| `Shares` | `(38, 18)` | `Shares * Price → Usd` |
| `Bps` | `(10, 4)` | Basis points |
| `Probability` | `(20, 18)` | Calibration / scoring |

Rules:

- **`f64` is never used for money.** `From<f64>` is intentionally not implemented.
- Cross-type math only where explicitly defined in `money.rs`.
- Persist via SeaORM `Value::Decimal` → native Postgres NUMERIC (lossless round-trip).
- Display/formatting for UI is string-out; keep computation in Decimal.

### Fees

- Polymarket fee formula in `oxide-arb-api/src/fees/`.
- Respect neg-risk flag and per-market fee config from Gamma metadata.
- Fee spend is tracked by risk checks (`FeeSpendCheck`).

### Chain Constants

- Contract addresses, chain IDs, tick size literals → `oxide-arb-models/src/constants.rs` only.
- Tunable thresholds → runtime config modules, validated per `ExecutionMode`.

---

## 9. Execution Modes & Safety Model

| Mode | CLOB Orders | On-chain | Validation |
|------|-------------|----------|------------|
| `DryRun` | Simulated | No | Relaxed; synthetic ids allowed in DB |
| `Paper` | Simulated / recorded | No | Stricter than DryRun |
| `Live` | Real orders | Real redeem/settle | Full credential + balance + route checks |

Validation entry points:

- `Settings::ensure_valid_for_mode` (static config)
- `validate_runtime_for_mode` (runtime config store)
- Live readiness checks before mode transition

**Fail closed:** missing credentials, open circuit breaker, failed validation, stale metrics, unreconciled blocking trades → **reject**, never degrade to silent trading.

Circuit breaker FSM (`oxide-arb-risk`): `Closed → Open → HalfOpen → Recovered / Halted`.

Capital: reservations before dispatch; exposure limits enforced in risk pipeline; `CapitalManager` coordinates release/adjustment on fill/reconcile.

Backpressure: detection coalescer/dedup/spill — channel pressure must not globally halt trading.

---

## 10. Configuration System

| Source | Purpose |
|--------|---------|
| `config/oxide-arb.toml` | Default file-based settings |
| `OXIDE_ARB__*` env vars | Override (double-underscore nesting) |
| Runtime config store | Versioned tunables with activate/rollback governance |
| Control factor snapshots | Hot-path reads of published factor values |

Load + validate before starting subsystems. Mode-aware validation must run on every Live transition.

Schema metadata for UI forms: `schemars` derives on runtime config types (`runtime_config/` modules). Doc comments become field descriptions — single source of truth for the admin UI renderer.

Do not add tunable trading thresholds to `constants.rs`. Do not hardcode business defaults in DTO `Default` impls — apply defaults in handlers or explicit builder functions.

---

## 11. Persistence Layer

Authoritative docs:

- [`docs/persistence/schema-catalog.md`](docs/persistence/schema-catalog.md) — Postgres tables, indexes, triggers, seed
- [`docs/models/dto-paradigm.md`](docs/models/dto-paradigm.md) — DTO layers above tables

### Vertical Chain (One Resource)

```text
idens/<table>.rs        Table DDL, indexes, seed specs     ← schema-catalog.md
entities/<table>.rs     SeaORM Entity / Model
domain/<ctx>/<x>.rs     Persistence DTOs: *Info / New* / *Patch
domain/api/<x>.rs       Wire DTOs: *Request / *Query / *View
routes/<x>.rs           Handler: validate → translate → persist → project
```

### Schema Catalog Rules

- Every iden enum: `#[oxide_schema]` (never bare `DeriveIden`).
- Non-core tables: explicit lifecycle (`control`, `audit`).
- Column types via `crate::schema::column` builders — no hand-rolled `.text()` for IDs or money.
- ID family rules:
  - Internal UUID → native `uuid` column + `UuidId` newtype.
  - External semantic string → `varchar`/`text` + `StrId` newtype.
  - Surrogate keys only for true insert proxies (e.g. casbin_rule).
- Junction tables: composite PK, no meaningless surrogate UUID.
- `UpdatedAt`: declare variant + `timestamp_with_write_default`; trigger auto-generated.
- Seed dependencies: declared in `SeedSpec`; loaders use `ctx.require<T>()?` — **no `unwrap()`**.

### Adding a New Table (Checklist)

1. `idens/<table>.rs` with `#[oxide_schema]`, `table()`, `indexes()`, `dependencies()`, `seed_units()`.
2. Register module in `idens/mod.rs`.
3. `entities/<table>.rs` SeaORM model.
4. `domain/<ctx>/` persistence DTOs.
5. Repository trait + Postgres impl (if accessed by business code).
6. Migration generated from schema graph (not hand-written DDL in migration files).
7. Run schema graph tests + Postgres migration tests.

---

## 12. DTO & HTTP API Contract Layer

### Three DTO Families — Never Merge

| Family | Module | Serde | Validation | Sensitive Fields |
|--------|--------|-------|------------|------------------|
| `*Request` / `*Query` | `domain::api` | Deserialize | `#[derive(Validate)]` | Plaintext credentials inbound |
| `*Info` / `New*` / `*Patch` | `domain::<ctx>` | ORM derives | None | Hashed secrets (e.g. `password_hash`) |
| `*View` / `*Response` | `domain::api` | Serialize only | None | **Stripped** — no hashes |

Merging breaks: credential boundary, validation ownership, null semantics (`Option<Option<T>>` wire vs `Patch<T>` persistence).

### Placement Decision (Accepted)

`domain::api` stays in **`oxide-arb-models`**, not `oxide-arb-web`. Do not move until a second consumer exists (then extract `oxide-arb-contract` crate).

### Persistence DTO Rules

- `*Info`: read projection; `DerivePartialModel` + `FromQueryResult`; `info_from_model!` for insert-return.
- `New*`: insert payload; `DeriveIntoActiveModel`; omit DB-managed timestamps.
- `*Patch`: `Patch<T>` / `NullablePatch<T>`; exclude credentials and `status` — use dedicated methods (`change_password`, `change_status`).
- **Never** expose `ActiveModel` / `ActiveValue` in public signatures.
- **Never** implement `to_active_model()` manually.

### API Contract Rules

- `*Request`: `Deserialize + Validate`; partial update fields use `#[serde(default, with = "double_option")]`.
- `*Query`: paginated queries flatten `PageRequest`; provide `normalized()`; window queries provide `resolve()` returning **domain errors** (not `WebError`).
- `*View`: built via `From<XInfo>`; strip sensitive columns.

### Conversion Table

| Conversion | Where |
|------------|-------|
| `*Request → New*` | Handler (hash credentials, mint ids, apply defaults) |
| `*Request → *Patch` | `impl From<UpdateXRequest> for XPatch` in `domain/api/` |
| `*Info → *View` | `impl From<XInfo> for XView` in `domain/api/` |
| `Model → *Info` | `info_from_model!` in `domain/<ctx>/` |

### Serde Adapters — Never Hand-Roll

| Need | Use |
|------|-----|
| Three-way null | `serde_with::rust::double_option` |
| Flattened numeric query fields | `#[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]` |
| Wire name ≠ variant name | `Display` + `FromStr` + `SerializeDisplay` / `DeserializeFromStr` |

Non-flattened query fields: parsed natively by `serde_urlencoded` — do not add `DisplayFromStr`.

---

## 13. Web Layer (`oxide-arb-web`)

### Handler Pattern

```text
ValidatedJson<CreateXRequest>     // extract + run #[validate]
  → sensitive transforms (hash, mint id, defaults)   // handler only
  → NewX { ... }
  → repo.create(new) → XInfo
  → XView::from(info)
  → WebResponse::ok(view)
```

Rules:

- Always extract bodies with `ValidatedJson<T>`.
- Paginate via `Paginated<XView>` projecting `Paginated<XInfo>`.
- After Casbin policy table mutations: `state.casbin.reload()`.
- Governed routes require `X-Acting-Role` header + `reason` in body (see `protected_route_specs()` in `routes/mod.rs`).

### Response Envelope

```json
{ "code": 200, "message": "...", "data": { ... } }
```

`successCode: 200` (not 0). API version header: `Accept-Api-Version: v1`.

Verify endpoints against `crates/oxide-arb-web/src/routes/mod.rs` — do not invent routes.

---

## 14. Control Factor Plane

Control factors let operators tune detection/risk/execution parameters without redeploying, via a governed shadow → publish workflow.

- Hot path reads **published snapshot** (ArcSwap) — no DB query per trade.
- Risk checks: `ControlFactorSnapshotExpiredCheck`, `ControlFactorManualAckRequiredCheck`.
- Materialization builds evidence for replay/analytics (ClickHouse fact plane).
- Scheduler + execute workers queued from `AppContext`.

When changing factor schema or snapshot build logic, update both `oxide-arb-control` and risk check expectations.

---

## 15. ClickHouse Conventions

Scope: `oxide-arb-repository/src/clickhouse/**`, `oxide-arb-models/src/clickhouse/**`.

- Bind typed IDs directly (`MarketId`, `TokenId`, `OpportunityId` → `String` / `Array(String)`).
- Do **not** convert to `Vec<String>` for `IN ?` filters — erases domain meaning.
- `Ch*` enums with `Serialize_repr` + matching `#[repr(i8)]` bind to `Enum8`.
- `ChUsd`, `ChPrice`, etc. bind through `#[serde(transparent)]` wrappers.
- Every evidence/replay query: **explicit stable ORDER BY** (event time + tie-breakers: `ingestion_time`, `sequence`, row id).

Convert to string only at textual boundaries: logs, error messages, query fingerprints, external protocols.

---

## 16. Error Handling

Unified tree in `oxide-arb-error` → `OxideError` with `#[from]` sub-errors:

`AlgoError`, `ApiError`, `WsError`, `StorageError`, `SigningError`, `ConfigError`, `TradingError`, `ReservationError`, `RbacError`, `AuthError`, `GovernanceError`, etc.

Conventions:

- Public functions return `OxideResult<T>` or a domain-specific `Result<T, SubError>` that converts via `?`.
- Use `thiserror` for error enums; add context with structured `tracing` fields at boundaries.
- Map domain errors to `WebError` in the web layer via `From` impls — domain layers must not depend on `oxide-arb-web`.
- Seed loaders and migration helpers: return structured errors; **no `unwrap()`**.

---

## 17. Rust Coding Standards

Reference: [`.cursor/rules/oxide-arb-rust-style.mdc`](.cursor/rules/oxide-arb-rust-style.mdc)

Canonical examples: `crates/oxide-arb-core/src/execution/execution_pipeline.rs`, `crates/oxide-arb-storage/tests/cache_unit.rs`.

### 17.1 Import Layout (File Preamble)

Order, with **one blank line between groups**, **no blank lines inside a group**:

1. `mod` / `pub mod` (and `#[path = "..."]` test modules)
2. `pub use` re-exports
3. `use` statements (stdlib → external crates → `oxide_arb_*` → `crate::`)

Rules:

- **One `use` root per scope** — merge siblings into a tree; never three separate `use oxide_arb_models::foo::Bar` lines.
- **No `use` inside functions** in production code. Test-only imports go in `mod tests { use ... }`.
- In `src/`: prefer `use crate::...`. In `tests/` and `#[path]` submodules: `use super::...` is fine.

```rust
use oxide_arb_models::{
    domain::{
        book::BookLevel,
        opportunity::Opportunity,
    },
    enums::common::Side,
    types::{MarketId, Usd},
};
```

### 17.2 Path Depth in Item Bodies

After imports:

- **Forbidden:** `std::...` or `oxide_arb_*::...` with **two or more** `::` segments in expressions/types.
- **Allowed:** at most one `::` in a call path (`Duration::from_millis`, `Usd::ZERO`).

```rust
// Good
use std::fmt::{self, Display, Formatter};

impl Display for Side {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result { ... }
}

// Bad — import instead
impl std::fmt::Display for Side { ... }
```

Special cases:

- **`Display` / `Debug`:** import `Formatter`; return `fmt::Result` (never bare `Result` — shadows `std::result::Result`).
- **Proc macros (`oxide-arb-macros`):** generated code may use fully qualified `::std::fmt::...`.
- **`money` module:** `use std::ops::{Add, Sub, ...}`; `impl Add for Usd`, not `impl std::ops::Add`.
- **Public API trait bounds:** `impl std::future::Future<...>` acceptable when importing `Future` adds no clarity.

### 17.3 `Arc` Cloning

When cloning **`Arc<T>`** or cloning **out of** an `Arc` container (`DashMap` values, `ArcSwap` loads, shared repo handles):

```rust
// Good — explicit shared ownership
let metrics = Arc::clone(&self.metrics);
books.get(token_id).map(|e| Arc::clone(e.value()));

// Bad on Arc-typed values
let metrics = self.metrics.clone();
entry.get().clone();
```

Still use `.clone()` for: typed IDs (`MarketId`, …), `String`, `Vec`, Decimal newtypes, `CancellationToken`, channel `Sender`, config structs, SeaORM models, `#[derive(Clone)]` domain structs that are not `Arc<…>`.

After refactors, grep suspicious patterns: `self.<arc_field>.clone()`, `.get().clone()` on Arc map entries, `load_full().clone()` when loaded type is already `Arc<_>`.

### 17.4 Comments & Documentation

- Complex business logic and non-obvious invariants: **English** Rustdoc or inline comments.
- Do not comment obvious code.
- New public items: standard Rust doc comments (`///`).
- Safety-critical paths (risk, execution, reconciliation): document invariants and failure modes.

### 17.5 Performance Hot Path

- Prefer `Arc`, `DashMap`, `ArcSwap`, atomics over mutexes on hot paths.
- Avoid allocations in detection/risk/execution per-tick paths.
- Measure regressions in `oxide-arb-bench` (`hot_paths`, `e2e_paths`).
- CI enforces SLO ceilings (`scripts/check-bench-slo.sh`) and PR regression gates (`scripts/check-bench-regression.sh`).

---

## 18. Testing Conventions

| Test Type | Location | Notes |
|-----------|----------|-------|
| Unit tests | `#[cfg(test)] mod tests` in source files | Prefer `?` over `unwrap()` |
| Crate integration | `crates/*/tests/*.rs` | Use `oxide-arb-test-support` mocks |
| Wiremock API | `cargo test-network` | Polymarket REST mocking |
| Docker DB | `cargo test-docker` | testcontainers Postgres + Redis |
| Snapshot | `insta` | Update deliberately; review diffs |
| Property | `proptest` | Algorithm/risk invariants |
| Ignored soak | `--ignored` | Large-scale ingest tests |

Mock repositories live in `oxide-arb-test-support/src/mocks/repos.rs` (`MockTradeRepository`, etc.).

Write tests that assert **behavior**, not constants. Do not add trivial tests that only verify literal values.

Integration tests requiring live Polymarket/RPC are `#[ignore]` and gated behind env vars — they do not block PR CI by default.

---

## 19. Quality Gates (Mandatory)

**Every code change must pass locally before requesting review:**

```bash
cargo fmt --all -- && cargo clippy --workspace --all-targets -- -D warnings
```

This matches CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) and the production promotion script ([`scripts/check-production-gates.sh`](scripts/check-production-gates.sh)).

CI also sets `RUSTFLAGS="-D warnings"` — treat all compiler warnings as errors.

### Recommended Additional Checks (Non-Trivial Changes)

```bash
bash scripts/lint-architecture.sh          # crate boundary enforcement
cargo test --workspace                     # unit + integration
cargo test-network                         # wiremock API tests
cargo test-docker                          # Postgres/Redis integration
```

Production promotion (before Live) additionally runs bench SLO, regression gates, and ignored soak tests — see `scripts/check-production-gates.sh`.

---

## 20. Forbidden Patterns

| Forbidden | Why | Instead |
|-----------|-----|---------|
| `#[allow(clippy::...)]` | Hides real issues | Fix root cause |
| `#[allow(unused_imports)]` | Hides dead code | Delete or move to test module |
| `unwrap()` / `expect()` in `src/` | Panics in production | `?`, `match`, explicit error |
| `unsafe` | Workspace denies it | Safe alternatives only |
| `f64` for money | Precision loss | `Usd` / `Price` / `Shares` |
| Hand-rolled three-way null serde | Inconsistent wire semantics | `serde_with::double_option` |
| Merging `*Request` with `New*` | Breaks security boundaries | Separate types + `From` bridges |
| `password_hash` in `*View` | Credential leak | `From<XInfo>` projection strips it |
| `#[validate]` on persistence DTOs | Wrong validation layer | Validate wire types only |
| Credentials/`status` in `*Patch` | Bypasses audit methods | Dedicated `change_*` repo methods |
| Business defaults in DTO `Default` | Hidden policy | Handler/builder explicit defaults |
| `sea_orm` / `entities` in core/risk/algorithm | Layer violation | Repository traits |
| `*Info` as write input | Bypasses insert/update contracts | `New*` / `*Patch` |
| `from_decimal_unchecked` in WS/CLOB ingest | Silent bad prices | Validated decimal parsing |
| Reading `BookStore.live` on hot path | Lock contention | `published` snapshot only |
| I/O inside `RiskCheck::evaluate` | Hot path latency | Pre-fetch into context |
| Modifying inactive `oxide-arb-ui` apps | Upstream vben noise | Only `apps/web-antdv-next` when asked |
| Moving `domain::api` to web crate | Breaks DTO paradigm | Keep in models until second consumer |

### `unwrap()` / `expect()` Policy

- **Production (`src/`):** forbidden on fallible operations (I/O, parsing, indexing, lock poisoning).
- **Tests:** prefer `?` or assert specific error variants; `expect` only when test setup failure should abort the test immediately and the message adds context.
- **Seed loaders / migrations:** forbidden — use `?` and structured errors.

---

## 21. How to Extend the Codebase

### 21.1 New Persisted Enum

1. Add enum in `oxide-arb-models/src/enums/`.
2. `DeriveActiveEnum` + `IntoActiveValue` if stored in Postgres.
3. Add to SeaORM entity + migration via schema graph.
4. Mirror wire enum in `domain::api` if exposed over HTTP.
5. Add repository mapping + tests.

### 21.2 New External API Integration

1. Implement in `oxide-arb-api/src/`.
2. Map responses into domain types or `PipelineEvent`.
3. Propagate errors via `oxide_arb_error::api::ApiError`.
4. Add wiremock integration test (`cargo test-network`).
5. Never leak raw SDK types into `core` or `risk`.

### 21.3 New Risk Check

1. Implement `RiskCheck` trait in `oxide-arb-risk/src/pipeline/checks/`.
2. Register in `StaticRiskPipeline` constructor in deterministic order.
3. Set `requires_metrics()` correctly (snapshot-only vs live metrics).
4. Add unit tests in `oxide-arb-risk/tests/`.
5. If check blocks Live trading, document in runbook.

### 21.4 Hot Path Change

1. Read existing pattern in target module first.
2. No blocking I/O; prefer lock-free reads.
3. Run `cargo bench -p oxide-arb-bench --bench hot_paths`.
4. Verify SLO script still passes.

### 21.5 New HTTP Resource

Follow [`docs/models/dto-paradigm.md`](docs/models/dto-paradigm.md) checklist:

1. Schema catalog table + entity.
2. `domain/<ctx>/`: `XInfo`, `NewX`, `XPatch`.
3. `domain/api/`: `CreateXRequest`, `UpdateXRequest`, `XPageQuery`, `XView`, `From` bridges.
4. Repository methods.
5. `routes/<x>.rs` handler.
6. Casbin permissions + menu seed if user-facing.
7. Web integration test in `oxide-arb-web/tests/`.

### 21.6 New Runtime Config Field

1. Add to appropriate `runtime_config/` module with `schemars` + validation.
2. Update `validation.rs` mode-aware rules.
3. Update UI catalog if exposed in admin UI.
4. Refresh insta snapshots if schema metadata snapshots exist.

### 21.7 General Change Discipline

- **Minimal diff:** only touch files required by the task.
- **Match existing style:** naming, error handling, import layout, Arc cloning.
- **Do not refactor unrelated code** unless explicitly requested.
- **Do not commit or push** unless the user explicitly asks.

---

## 22. Frontend (`oxide-arb-ui`) — Only When Asked

Do not modify `oxide-arb-ui` unless the user explicitly requests UI work.

Active app: `oxide-arb-ui/apps/web-antdv-next`. Shared libs: `oxide-arb-ui/packages/*`. Other apps are untouched vben upstream.

Full rules: [`.cursor/rules/oxide-arb-ui.mdc`](.cursor/rules/oxide-arb-ui.mdc) and `docs/plans/phase7-ui-layer.md`.

Key constraints when working on UI:

- Tables/forms via adapters only (`useVbenVxeGrid`, `useVbenForm`).
- Money/price/shares as **`string`** (rust_decimal wire format) — never JS `number`.
- Enum values mirror Rust serde / DB `string_value`.
- Governed mutations via `useGovernedAction` (never hand-roll `X-Acting-Role`).
- Verify endpoints against `oxide-arb-web` routes — do not invent API paths.

---

## 23. Agent Working Principles

1. **Read before write.** Locate the canonical module for the concern; follow existing patterns.
2. **Fail closed.** Security, credentials, validation, circuit breakers, reconciliation gates default to **deny**.
3. **Hot path zero I/O.** Risk checks, book reads, and scoring must not block on network or DB.
4. **Layer discipline.** Models → repository → core/risk/algorithm → web. No shortcuts across layers.
5. **English comments** for non-obvious logic; Rustdoc on public API.
6. **Meaningful tests** for behavior changes; skip trivial assertions.
7. **Quality gate first.** Run `cargo fmt --all -- && cargo clippy --workspace --all-targets -- -D warnings` before declaring done.
8. **No drive-by changes.** No unrelated formatting, refactors, or doc files unless requested.
9. **Ask when uncertain.** Stop and clarify ambiguous requirements rather than guessing.

---

## 24. Reference Index

| Document | Topic |
|----------|-------|
| [`.cursor/rules/oxide-arb-domain.mdc`](.cursor/rules/oxide-arb-domain.mdc) | Domain & crate boundaries |
| [`.cursor/rules/oxide-arb-rust-style.mdc`](.cursor/rules/oxide-arb-rust-style.mdc) | Import layout, path depth, Arc, clippy |
| [`.cursor/rules/oxide-arb-dto-api.mdc`](.cursor/rules/oxide-arb-dto-api.mdc) | DTO three-layer model |
| [`.cursor/rules/oxide-arb-clickhouse-rust.mdc`](.cursor/rules/oxide-arb-clickhouse-rust.mdc) | ClickHouse bind & ordering |
| [`.cursor/rules/oxide-arb-ui.mdc`](.cursor/rules/oxide-arb-ui.mdc) | Frontend architecture |
| [`docs/plans/ADR-001-single-strategy-single-platform.md`](docs/plans/ADR-001-single-strategy-single-platform.md) | Single strategy/platform decision |
| [`docs/models/dto-paradigm.md`](docs/models/dto-paradigm.md) | DTO & API contract (authoritative) |
| [`docs/persistence/schema-catalog.md`](docs/persistence/schema-catalog.md) | Postgres schema (authoritative) |
| [`docs/operations/runbook.md`](docs/operations/runbook.md) | DryRun / Paper / Live operations |
| [`docs/operations/bankroll-and-risk-metrics.md`](docs/operations/bankroll-and-risk-metrics.md) | Bankroll vs wallet balance |
| [`scripts/lint-architecture.sh`](scripts/lint-architecture.sh) | Automated boundary checks |
| [`scripts/check-production-gates.sh`](scripts/check-production-gates.sh) | Pre-Live promotion gate |
| [`.github/workflows/ci.yml`](.github/workflows/ci.yml) | CI pipeline |
