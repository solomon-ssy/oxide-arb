# quant-pivot Cold-Start Production Closeout

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

## Implementation Record (2026-07-17)

### Delivered contracts

- The workspace is pinned to Rust 1.97.1, SeaORM 2.0.0-rc.43, and SQLx 0.9.0.
  The dependency graph contains one SeaORM/SQLx generation.
- `quant-pivot-migration` owns four audited SeaORM migrations. Runtime schema
  verification reports 84 tables and 246 indexes; ClickHouse schema version 4
  verifies 27 required objects. Runtime identities have no DDL path.
- Runtime persistence uses SeaORM 2 dense entities, typed columns/relations,
  PostgreSQL native enums/enum arrays, and typed repository commands. The old
  `idens` and catalog-version dual models were deleted.
- Catalog object/change ledgers, deterministic content hashes, baseline-bound
  PIT reads, bounded keyset pagination, ID rechecks, typed rejection records,
  and ambiguous-commit recovery are active. Projection and ledger commit in one
  writer transaction; no post-commit visibility sleep or `committed -> failed`
  transition remains.
- Bootstrap uses the four-phase durable FSM with monotonic `state_revision`,
  WORM transition audit, explicit approval-bound activation, typed capability
  watches, and fail-closed singleton restore. Runtime config schema v17 is the
  only accepted document version.
- Automatic parity freezes run subjects/candidates transactionally. A cold
  store with no serving subject creates no run and opens no latch.
- Research source registration, typed JCS attestations, deployment-scoped
  deterministic key IDs, ClickHouse offline-safety classes, schema manifests,
  and ReplacingMergeTree correctness lint are implemented. Evidence uses one
  32-byte active BLAKE3 keyed-hash key plus optional historical verification
  keys. Duplicate, malformed, and active-in-history configurations fail
  closed. The 200-day gate was not reduced and fixtures cannot satisfy
  production readiness.
- Access JWTs are memory-only HS256 tokens signed by one Base64URL-no-pad
  encoded 32-byte random key. Encode and decode fix the algorithm and validate
  issuer, audience, subject, expiry, session family, typed `token_use`, and
  media-type `typ`. Refresh families rotate through one Redis CAS script with
  absolute session expiry; `expires_in` reflects absolute-session clipping.
  WebSocket authentication uses a 30-second single-use session-bound ticket
  rather than a query JWT. Replacing the active key immediately invalidates all
  previously issued JWTs. Deploy validation also decodes both key materials and
  rejects reuse of the JWT key as the evidence-attestation key.
- PostgreSQL and ClickHouse migration credentials are optional redacted
  secrets under their canonical `[db.*.migration]` sections. Deploy/xtask
  profiles may resolve them through base TOML, local TOML, or the canonical
  nested environment variables; migration commands require them. Production
  runtime validation rejects configurations containing DDL passwords. Runtime
  projections do not retain the passwords, and PostgreSQL URLs are assembled
  through structured URL mutation with reserved-character coverage.
- The protected operator UI now exposes one snapshot-driven Quant Command
  Center: authoritative status and CTA, account KPIs, equity/drawdown, a polar
  recommendation orbit, execution lifecycle, exposure, data quality, research
  readiness, subsystem health, and a severity-ordered action inbox. Sections
  are independently ready/stale/unavailable/forbidden, permissions are clipped
  server-side, account data stays memory-only, and dynamic-route restoration
  precedes catch-all 404 resolution.
- Dashboard charts use the existing ECharts stack with ARIA/decal output and
  equivalent keyboard-readable lists. Motion pauses when hidden or interacted
  with and disables rotation, stagger, count-up, and material interpolation
  under `prefers-reduced-motion`. The dashboard chart code is asynchronously
  loaded and enforced against a 300 KiB gzip budget.
- The WebSocket ingest pipeline uses bounded canonical batches, supervised
  workers, explicit backpressure invalidation, and shutdown drain. ClickHouse
  Rust rows use `u32` for schema columns declared `UInt32`; schema migration 4
  converts the prior `UInt16` columns.
- RPC errors redact URL userinfo/path/query/fragment in both `Display` and
  `Debug`. Gamma keyset bodies are read with a 64 MiB hard bound; invalid
  payload diagnostics persist only content type, byte length, and BLAKE3 hash.
  JSON syntax/EOF failures retry inside the Gamma budget while structural data
  drift remains a permanent reconcile rejection.
- PostgreSQL deployment no longer seeds a known `admin/admin` credential. The
  bootstrap administrator password is supplied through a permission-checked
  secret file, hashed with Argon2id before the seed transaction, and never read
  by the application runtime. The RBAC seed ledger records
  `rbac.admin_user.bootstrap` version 2.
- Dependency cleanup removed 33 unused Rust manifest entries. `cargo machete`,
  nightly `cargo udeps`, UI Knip, circular-dependency checks, and production
  bundle forbidden-pattern scans are part of the closeout gates.
- PostgreSQL Docker test containers are bounded by an RAII-owned per-process
  semaphore. This prevents an unconstrained parallel integration-test binary
  from starving Docker Desktop's maintenance connections while guaranteeing
  permit release on early return or panic.

### Clean-start evidence

The local target was confirmed from deploy config before destructive work.
Only PostgreSQL database `quant_pivot`, ClickHouse database `quant_pivot`,
Redis keys matching `qp:*`, and `var/artifacts` contents were reset. Audit files
were retained and provider URLs in them were redacted.

The final clean start produced one committed baseline with zero rejections:

- 10,179 `catalog_event_object` rows and 79,136 `catalog_market_object` rows;
- the same event/market counts in append-only change rows;
- `initializing -> collecting_baseline -> awaiting_activation` with state
  revision 2;
- zero parity runs, parity latch rows, and recommendation reports.

A schema-v17 runtime config was created, approved, and activated through
`POST /api/system/bootstrap/activate` with the ReportOnlyForced
acknowledgement. The durable state became `active`, revision 3,
`report_only`; one version, approval, activation, and three FSM transitions are
present. A subsequent process restart restored `active|3|report_only` without
creating a parity run or latch row. The short-run log is
`var/audit/2026-07-17/cold-start-closeout/runtime-post-fix.log`.

The original reported signatures are absent from the post-fix log:

- `failed to enqueue stream gap`;
- `UInt16 as u32` / ClickHouse schema mismatch;
- the deleted `mkt:__active__` registry path;
- deterministic parity failure or latch-open containment.

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
