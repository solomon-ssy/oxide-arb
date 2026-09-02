# quant-pivot Cold-Start Production Closeout

<!-- quant-pivot-deployment-contract:v1 -->
> **Deployment contract**
> - `fresh_boot_assumption`: 项目尚未正式生产上线，将从全新 `boot` / schema version `1` 部署；仓库和数据库不保存 lifecycle seal 状态。
> - `schema_data_version_impact`: 本文中的历史版本号与递增路径不再具有实施效力；当前实现不迁移测试数据、旧结构或旧版本。
> - `pre_deployment_behavior`: 允许 clean-break 与唯一 fresh terminal bootstrap rewrite；任何真实数据销毁仍需操作者单独授权。
> - `post_deployment_behavior`: 本次实现只交付唯一 fresh terminal bootstrap；不设计 upgrade/downgrade 或 data/schema/version migration。
> - `rollback_and_data_verification`: 只在 disposable 空基础设施执行 fresh-install 验证；任何真实数据重置必须另行授权。

This document is the execution contract for closing the remaining cold-start,
schema, governance, deterministic-evidence, authentication, and operator-UI
gaps discovered during the 2026-07-16 implementation audit.

## Required Outcomes

- PostgreSQL uses an audited SeaORM fresh-bootstrap crate, an exact normalized schema
  manifest, one configured identity, and a runtime binary with no DDL entrypoint.
- SeaORM 2 dense entities and PostgreSQL native enums own typed persistence;
  duplicate hand-maintained DDL models are removed.
- Bootstrap is restart-safe and fail-closed. Runtime configuration approval,
  activation, state transition, and audit use one durable transaction.
- Catalog commits are atomic, idempotent across ambiguous commit outcomes, and
  point-in-time reads require a committed baseline.
- Automatic parity freezes its complete subject and candidate set before a job
  becomes executable. An empty cold store remains not eligible, not failed.
- Research evidence is canonical, deployment-bound, historically keyed, and
  remains blocked until each profile's real fit contract is satisfied: 33 days
  for Pooled, 93 days for Crypto, and 100 days for Weather. The 200-day raw
  retention floor preserves full research capability; it is not the first-report gate.
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
3. Complete bootstrap, independently governed Config resources, and capability watches.
4. Complete catalog reconciliation and frozen parity protocols.
5. Complete research evidence, authentication, WebSocket, and UI behavior.
6. Remove duplicate/dead code and update active architecture documentation.
7. Reset only quant-pivot-owned data and complete deterministic cold-start,
   explicit activation, restart, and automated quality-gate verification.

## Non-Negotiable Acceptance

- All documented Rust and UI quality gates pass.
- Clean databases deterministically reach `awaiting_activation`; activation is
  explicit and survives restart.
- Per-profile evidence is reported independently: Pooled requires 33 days,
  Crypto 93 days, and Weather 100 days. Missing vertical evidence cannot revoke
  an already published Pooled report unless a real positive vertical exposure
  requires that route to model account risk.
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

## PostgreSQL Fresh-Boot Contract

`quant-pivot-migration` is the only PostgreSQL DDL owner and is linked only by
deploy/test tooling. Its crate name follows SeaORM terminology; the current
project contract contains exactly one entry, `m00000000_000001_bootstrap`, and
does not define an upgrade chain. The application runtime can verify the exact
schema identity but cannot apply, roll back, synchronize, or evolve schema.

- One `MigratorTrait` implementation registers exactly the single bootstrap.
- The dense entities under `snapshots/v1`, typed relational/index/WORM specs,
  native enums, tables, constraints, functions, and grants together define the
  sole fresh terminal schema. Before first production use, a structural change
  replaces this bootstrap and its checked manifests directly.
- There is no historical-row converter, compatibility view, dual reader/writer,
  `down` recovery path, future enum-extension chain, data rewrite, or versioned
  support-module branch.
- SeaQuery owns ordinary DDL. PostgreSQL syntax it cannot model is isolated in
  the bootstrap support module, whose typed specs quote identifiers, validate
  static definitions, and reject statement separators. Repositories and runtime
  startup never own ad-hoc DDL.
- SeaORM's `seaql_migrations` row and the same-transaction
  `schema_migration_audit` row are checksum evidence for that one bootstrap;
  they do not authorize another version. Runtime verification requires exact
  native-ledger/checksum-ledger/manifest membership and rejects unknown history,
  including `_sqlx_migrations`.
- `plan` is read-only. Deploy `apply` is an explicit CLI operation under the
  canonical schema-mutation lease and is used only for a new empty database or
  exact idempotent verification of the same bootstrap.
- After intentionally changing the pre-deployment bootstrap, regenerate and
  review `schema/postgres/migrations.json`; it must still contain only the one
  bootstrap identity. CI rejects unreviewed checksum or schema drift.
- SeaORM `schema-sync` is disabled. The stable `SchemaBuilder::apply` path is
  restricted to the fresh bootstrap; experimental discovery/sync cannot own
  constraints, indexes, triggers, functions, grants, or lifecycle evolution.
- Catalog seeds are independent fresh-boot data artifacts with their own exact
  checksums and atomic seed-ledger writes; they are not an upgrade mechanism.

SeaORM remains the implementation framework for the one schema owner:
[setting up migration](https://www.sea-ql.org/SeaORM/docs/migration/setting-up-migration/)
and [entity-first schema](https://www.sea-ql.org/SeaORM/docs/generate-entity/entity-first/).

## Implementation Record (2026-07-19)

### Delivered contracts

- The workspace is pinned to Rust 1.97.1, SeaORM 2.0.0, and SQLx 0.9.0.
  The dependency graph contains one SeaORM/SQLx generation.
- `quant-pivot-migration` owns exactly one PostgreSQL boot migration. Its
  disposable PostgreSQL 16 shape must equal the checked-in manifest byte for
  byte; the gate derives current table/column/index/constraint counts instead
  of freezing drift-prone prose totals. ClickHouse likewise owns one boot
  manifest at system schema version 1. Application runtime code has no DDL path.
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
  identity, and content-addressed WORM evidence. Bootstrap and reset mutations
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
and ClickHouse credentials have been rotated and installed in a permission-0600,
untracked deploy TOML. No old secret value may be read, copied, or reused.

The explicitly authorized pre-production destructive scope is limited to PostgreSQL database
`quant_pivot`, ClickHouse database `quant_pivot`, and Redis keys matching the
validated non-empty `qp:` namespace. No runtime lifecycle state or production
  freeze exists. This implementation defines no post-deployment schema evolution,
  upgrade, downgrade, or historical-data conversion path.
### Automated verification

The canonical CI inventory is owned directly by [`.github/workflows/ci.yml`](../../../.github/workflows/ci.yml):
`secret-scan`, `rust-static`, `rust-unit-contract`, `system`, `ui`, and `protected-e2e`.
Fixed-runner performance is separately owned by
[`.github/workflows/performance.yml`](../../../.github/workflows/performance.yml). There is no root
gate script and no `cargo test-docker`/`cargo test-network` alias. Only commands actually run and
recorded by the current ledger may be treated as passing evidence:

- `cargo fmt --all --check`;
- `cargo clippy --workspace --all-targets -- -D warnings`;
- `cargo xtask architecture audit-functions` and `cargo xtask architecture check`;
- `cargo test --workspace` or the CI-owned `cargo nextest run --workspace --profile ci` partition;
- `cargo nextest run -p quant-pivot-system-tests --profile system` against shared disposable
  PostgreSQL/Redis/ClickHouse infrastructure;
- `cargo xtask production-stack feedback-closure --runs 1 --retain-artifacts` for the explicitly
  non-operational Phase 12 closure boundary;
- `cargo xtask performance run --profile full --output target/performance-evidence` on the fixed runner;
- `cargo machete --with-metadata` and `cargo +nightly udeps --workspace --all-targets`;
- UI generated-contract checks, lint, dependency/circular checks, typecheck, deterministic unit tests,
  production build and the Playwright projects named by current CI. Historical test counts are not a gate.

The current closeout is tracked only in the Phase 12 Execution Ledger and immutable local
acceptance manifests referenced by it. A check is passing evidence only when its
exact command, result, timestamp and evidence hash are recorded there. The CI
workflow remains the remote owner; local evidence never implies a remote CI run or hosted artifact.

### Explicitly deferred or externally blocked

- The two-hour/twelve-reconciliation soak has not run. This record does not
  claim that gate passed.
- Production ClickHouse Cloud DDL behavior, deploy-file installation, WORM
  restore proof, and Cloud retention/capacity evidence require the
  real deployment environment and remain promotion gates.
- The pre-reset store had 1,944,000 rows spanning about 91 days. That observation
  is not current profile evidence. After the authorized clean reset the local
  evidence store is empty and therefore remains fail-closed rather than being
  backfilled with fixtures or extrapolated data; promotion requires fresh 33/93/100-day
  profile proofs and a separate 200-day retention-capability proof.
- The local host intermittently could not reach Binance and Alchemy. These
  failures are bounded and capability-visible; the RPC provider URL is now
  redacted. Live provider reliability is not inferred from Wiremock tests.
- The `proc-macro-error2` future-incompatibility chain is pinned to the reviewed
  upstream fix; the full workspace build emits no such warning. The macOS debug
  linker can still warn that `__eh_frame` exceeds the compact
  unwind table limit; release artifacts are not affected by this debug-only
  evidence run.

The implementation evidence is not closed until the Phase 12 W7-04 disposable
production rehearsal and W7-05 fixed point are archived. Operational activation
remains a separate, explicitly authorized activity and still requires soak,
real ClickHouse Cloud, deploy-file, WORM restore, retention/capacity,
33/93/100-day profile proofs, and the 200-day research-retention artifact.
