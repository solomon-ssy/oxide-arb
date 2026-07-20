# quant-pivot Cold-Start Production Closeout

<!-- quant-pivot-lifecycle-contract:v1 -->
> **Lifecycle contract**
> - `lifecycle_assumption`: 项目尚未正式生产上线，当前状态为 `pre_production_resettable`，系统自有基线统一为 `boot` / schema version `1`。
> - `schema_data_version_impact`: 本文中的历史版本号与递增路径不再具有实施效力；当前实现不迁移测试数据、旧结构或旧版本。
> - `pre_production_behavior`: 允许 clean-break、migration squash 与全新基础设施 bootstrap，但任何数据销毁仍需操作者单独授权。
> - `production_frozen_behavior`: 一旦完成不可逆 production seal，后续变更必须提供前向 migration、兼容性评估、回滚方案与数据验证。
> - `rollback_and_data_verification`: 封存前通过清空后的 fresh-install 验证；封存后不得回退到 boot reset。

This document is the execution contract for closing the remaining cold-start,
schema, governance, deterministic-evidence, authentication, and operator-UI
gaps discovered during the 2026-07-16 implementation audit.

## Required Outcomes

- PostgreSQL uses an audited SeaORM migration crate, an exact normalized schema
  manifest, a DDL-free runtime identity, and a separate migration identity.
- SeaORM 2 dense entities and PostgreSQL native enums own typed persistence;
  duplicate hand-maintained DDL models are removed.
- Bootstrap is restart-safe and fail-closed. Runtime configuration approval,
  activation, state transition, and audit use one durable transaction.
- Catalog commits are atomic, idempotent across ambiguous commit outcomes, and
  point-in-time reads require a committed baseline.
- Automatic parity freezes its complete subject and candidate set before a job
  becomes executable. An empty cold store remains not eligible, not failed.
- Research evidence is canonical, deployment-bound, historically keyed, and
  remains blocked until the real 200-day source contract is satisfied.
- Authentication uses memory-only access tokens, atomic refresh-family
  rotation, bounded absolute sessions, and session-bound single-use WS tickets.
- The operator UI fails closed, remains recoverable while the backend is
  offline, and exposes one authoritative next action without stale capability
  decisions.

## Delivery Order

1. Restore a compiling Rust 1.97.1 / SeaORM 2 / SQLx 0.9 baseline. The entire
   build contract is pinned to Rust 1.97.1, the current stable point release.
   Rust 1.95 was only the first compiler able to build Polars 0.54 after
   SeaORM's own Rust 1.94 MSRV; it is not retained as a second supported
   toolchain. Rust 1.97.1 also contains the upstream LLVM miscompilation fix
   published on 2026-07-16.
2. Establish immutable PostgreSQL and ClickHouse schema contracts.
3. Complete bootstrap, runtime-config approval, and capability watches.
4. Complete catalog reconciliation and frozen parity protocols.
5. Complete research evidence, authentication, WebSocket, and UI behavior.
6. Remove duplicate/dead code and update active architecture documentation.
7. Reset only quant-pivot-owned data and complete deterministic cold-start,
   explicit activation, restart, and automated quality-gate verification.

## Non-Negotiable Acceptance

- All documented Rust and UI quality gates pass.
- Clean databases deterministically reach `awaiting_activation`; activation is
  explicit and survives restart.
- Real 91-day evidence is reported as `blocked_insufficient_history` against
  the unchanged 200-day requirement.
- Automated UI lint, typecheck, unit tests, production build, dependency checks,
  and forbidden-pattern bundle scans pass.
- No compatibility re-export, legacy schema ledger, query JWT, mock production
  endpoint, or runtime DDL path remains.

## Deferred Operational Validation

The two-hour/twelve-reconciliation soak and production evidence-archive audit
require a separately controlled operator run. They remain required promotion
evidence; this implementation must not claim production completion until those
artifacts and the external-environment evidence listed below are archived.

Protected browser behavior, multi-viewport layout, reduced-motion behavior,
keyboard access, accessibility scanning, and light/dark visual snapshots are
part of the automated closeout gate rather than deferred evidence.

## PostgreSQL Migration Contract

`quant-pivot-migration` is the only PostgreSQL DDL owner and is linked only by
deploy/test tooling. The application runtime can verify schema and migration
state but cannot apply, roll back, or synchronize schema.

- Migrations implement SeaORM `MigrationTrait` and are registered in strict
  chronological order by one `MigratorTrait` implementation.
- The pre-production patch chain is represented by four immutable schema-first
  migrations instead of one generated SQL dump: a frozen SeaORM 2 dense-entity
  snapshot creates native enums/tables/columns/primary keys/defaults; typed
  relational specs add CHECK/UNIQUE/FK invariants; typed index specs add exact
  btree/GIN/unique/partial indexes; and typed trigger bindings add WORM and
  `updated_at` behavior.
- The dense entities under `snapshots/v1` are a migration time capsule, not the
  runtime persistence model and not a second evolving schema owner. Never edit
  an applied snapshot. A future table or column change gets a new, narrowly
  named migration and, only when needed, a new versioned support module.
- Every migration has a tested `down` implementation for disposable-database
  `fresh`/`refresh` verification. Production recovery remains a new forward
  migration or a database restore; operators do not roll back durable business
  data by invoking `down`.
- SeaQuery owns ordinary DDL. PostgreSQL syntax that SeaQuery 1.0 cannot model
  (table-level CHECK/UNIQUE additions and triggers/functions) is isolated in
  immutable `migrations/support/v1.rs`. Its API accepts typed specs, quotes all
  identifiers, validates static definitions, rejects statement separators, and
  is included in every dependent migration checksum. Raw SQL is forbidden in
  migration modules and repositories.
- SeaORM's `seaql_migrations(version, applied_at)` ledger is necessary but not
  sufficient because it has no source checksum. Every migration therefore
  writes the same-transaction `schema_migration_audit` row containing a
  domain-separated BLAKE3 checksum over length-prefixed migration source,
  frozen entity snapshot, and versioned support artifacts, their total byte
  length, and the pinned migration engine version.
- Deploy apply holds a process-wide PostgreSQL advisory lock and invokes the
  native `Migrator::up(..., None)` once. SeaORM rc.43's `exec_up_with` opens,
  records, and commits a separate PostgreSQL transaction for each pending
  migration, so enum-label additions and later uses remain separated without
  repeated ledger discovery or a custom migration loop.
- `plan` is read-only and must not create `seaql_migrations`. `apply` runs only
  through the dedicated migration identity. Runtime verification requires an
  exact native-ledger/checksum-ledger/manifest match and rejects the legacy
  `_sqlx_migrations` object.
- After adding or intentionally changing an unapplied migration artifact,
  regenerate only the compiled artifact ledger with
  `cargo run -p quant-pivot-xtask -- postgres-schema migration-manifest` and
  review `schema/postgres/migrations.json`. This command never connects to a
  database or applies DDL; CI rejects an unreviewed artifact checksum change.
- Runtime has `SELECT` only on both migration ledgers. Schema ownership, DDL,
  ledger mutation, `CREATE`, and database temporary-object privileges remain
  exclusive to deploy infrastructure.
- SeaORM `schema-sync` is disabled in production. The stable `SchemaBuilder`
  `apply` API is used only by the frozen initial snapshot. Its experimental
  discovery/sync API cannot own constraints, partial indexes, triggers,
  functions, grants, or destructive production evolution.
- Catalog seeds are not schema migrations. Each seed has its own immutable
  version/checksum and performs data write plus seed-ledger write in one
  transaction; startup only hydrates and verifies applied seed artifacts.

For a future PostgreSQL enum extension, the enum-label migration must be its
own committed migration. A following migration may then use the new label in a
constraint, index predicate, or data rewrite. Both migrations must include all
execution artifacts in their checksum specification and pass clean-database,
upgrade, checksum-tamper, and runtime-no-DDL tests before release.

This contract follows SeaORM's official guidance to use a separate migration
crate, schema-first immutable migrations, `up`/`down`, and frozen entity
snapshots when `SchemaBuilder` is embedded in a migration. References:
[setting up migration](https://www.sea-ql.org/SeaORM/docs/migration/setting-up-migration/),
[writing migration](https://www.sea-ql.org/SeaORM/docs/migration/writing-migration/),
[running migration](https://www.sea-ql.org/SeaORM/docs/migration/running-migration/),
and [entity-first schema](https://www.sea-ql.org/SeaORM/docs/generate-entity/entity-first/).

## Implementation Record (2026-07-19)

### Delivered contracts

- The workspace is pinned to Rust 1.97.1, SeaORM 2.0.0-rc.43, and SQLx 0.9.0.
  The dependency graph contains one SeaORM/SQLx generation.
- `quant-pivot-migration` owns exactly one PostgreSQL boot migration. A
  disposable PostgreSQL 16 clean boot reports 93 tables, 1,213 columns, 273
  indexes, and 340 constraints. ClickHouse likewise owns one boot manifest at
  system schema version 1. Runtime identities have no DDL path.
- Governed configuration consists of six independently revisioned schema-1
  resources. Activation is guarded by a database-authoritative bundle
  generation, full revision vector, candidate hash, approval, preflight token,
  and exact idempotency digest. Audit, outbox, activation, and snapshot commit
  atomically; the reconciler publishes only committed bundles.
- Policy and research profiles are immutable content-addressed artifacts.
  Model specifications bind a typed thesis, input contract, training contract,
  research profile artifact, schema version, and definition hash; training and
  inference verify this lineage rather than carrying an unused JSON object.
- Persistence uses entity-first SeaORM/SeaQuery, ActiveEnum/newtypes, native
  PostgreSQL types, and closed typed JSON documents where the system owns a
  stable key set. Only audited external payload boundaries remain open JSON.
  Raw SQL is restricted to typed dialect modules for catalog/admin behavior or
  expressions the ORM cannot represent.
- Production seal acquires the shared lifecycle lease and rechecks live
  PostgreSQL, ClickHouse, active policy bundle, clean compile-time build
  identity, and content-addressed WORM evidence. Migration and reset mutations
  fail closed after a frozen baseline.
- The Config API, generated TypeScript contract, and UI share the same resource
  registry. The UI covers validation, approval, activation, rollback, stale
  generation, frozen/read-only, recovery, authorization, responsive,
  accessibility, and reduced-motion states.

### Fresh-boot acceptance status

The repository schema has been booted from zero in a disposable PostgreSQL 16
container and its semantic manifest regenerated from the resulting catalog.
The guarded local preproduction reset remains intentionally pending until the
operator confirms all previously exposed wallet, relayer, JWT/RPC, PostgreSQL,
and ClickHouse credentials have been rotated and installed as permission-0600
credential files. No old secret value may be read, copied, or reused.

The eventual destructive scope is limited to PostgreSQL database
`quant_pivot`, ClickHouse database `quant_pivot`, and Redis keys matching the
validated non-empty `qp:` namespace. The local environment remains
`pre_production_resettable`; irreversible production freeze is validated only
in a disposable environment.
### Automated verification

CI and local closeout use the same command inventory through
`scripts/check-production-gates.sh`, split into `rust-static`, `ui`, `network`,
`docker`, and `protected-e2e`. The following gates are required after material
closeout changes before promotion; only the results recorded by the final run
may be treated as passing evidence:

- `cargo fmt --all --check`;
- `cargo clippy --workspace --all-targets -- -D warnings`;
- `cargo test --workspace`;
- two consecutive `cargo test-docker` passes (all registered PostgreSQL, Redis,
  ClickHouse, core, and web/auth/WS suites);
- `cargo test-network` (Gamma and Data API Wiremock suites);
- all architecture, import-style, boundary, error, dead-semantics,
  ClickHouse-correctness, training-serving, and UI semantic-color lints;
- `cargo machete --with-metadata` and `cargo +nightly udeps --workspace --all-targets`;
- UI lint, typecheck, 494 unit tests, production build, Knip, circular checks,
  production bundle forbidden-pattern scan, and dashboard gzip-budget check;
- protected login/dashboard, refresh restoration, deep-link, real 404,
  reduced-motion, keyboard/accessibility, and responsive light/dark visual
  behavior at desktop, tablet, and mobile viewports.

The final local run on 2026-07-17 passed this inventory:

- `rust-static` completed format, workspace clippy, every architecture and
  semantic lint, `cargo machete`, nightly `cargo udeps`, workspace tests,
  classical/optimizer/dataframe feature tests, benchmark release compilation,
  and the executable hot-path benchmark.
- `network` passed all 10 Gamma and all 5 Data API cases.
- `docker` passed the complete registered suite twice consecutively after the
  PostgreSQL container-concurrency and database-timestamp boundary fixes.
- `ui` passed lint, typecheck, all 494 unit tests, production build, Knip,
  circular-dependency checks, semantic-color and forbidden-pattern scans. The
  dashboard-specific gzip chunk measured 37,883 bytes against the 300 KiB
  budget.
- `protected-e2e` passed all 8 scenarios. Desktop/mobile light and dark visual
  snapshots and tablet layout checks passed; the Axe scan reported zero serious
  or critical violations.
- Targeted regression tests additionally proved decimal-string preservation in
  dashboard monetary sections and byte-level independence of the JWT and
  evidence signing keys.

The workflow definitions invoke the same five group commands. No remote CI run
or hosted CI artifact was produced in this local implementation session, so
this record does not claim one.

### Explicitly deferred or externally blocked

- The two-hour/twelve-reconciliation soak has not run. This record does not
  claim that gate passed.
- Production ClickHouse Cloud identity/DDL behavior, production secret-manager
  mounts, WORM restore proof, and Cloud retention/capacity evidence require the
  real deployment environment and remain promotion gates.
- The pre-reset store had 1,944,000 rows spanning about 91 days. The required
  history remains 200 days; after the authorized clean reset the local evidence
  store is empty and therefore remains fail-closed rather than being backfilled
  with fixtures or extrapolated data.
- The local host intermittently could not reach Binance and Alchemy. These
  failures are bounded and capability-visible; the RPC provider URL is now
  redacted. Live provider reliability is not inferred from Wiremock tests.
- `proc-macro-error2 2.0.1` is a transitive future-incompatibility warning in
  upstream dependency trees. No local lint waiver or vendored fork was added.
  The macOS debug linker can also warn that `__eh_frame` exceeds the compact
  unwind table limit; release artifacts are not affected by this debug-only
  evidence run.

The implementation is therefore code-complete against the local production
gate inventory but is **not production-complete** until the soak, real
ClickHouse Cloud, secret-mount, WORM restore, retention/capacity, and 200-day
readiness artifacts are archived.
